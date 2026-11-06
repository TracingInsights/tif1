//! lz4 frame decoding into a caller-owned buffer.
//!
//! The frame tier stores each driver row as one lz4 frame written by
//! python-lz4 (`lz4.frame.compress`, level 0: linked 64 KB blocks, the content
//! size in the header, no checksums). pyarrow's codec and python-lz4 both
//! return freshly allocated memory, so the caller cannot choose where the
//! 3-6 MB of output of a row lands; `decode_frame_into` writes the blocks
//! straight into `dst` (the previous 64 KB of output is the dictionary of a
//! linked block, read in place as liblz4's prefix), with no intermediate
//! window or copy.
//!
//! The blocks are decoded by liblz4's `LZ4_decompress_safe(_usingDict)`, the
//! decoder pyarrow's codec runs (C source vendored by `lz4-sys`, built into
//! the extension by `cc`); this module parses the frame itself, so the
//! linker keeps only `lz4.c`'s block decoder (the wheel grows by ~7 KB).
//!
//! Frames with block or content checksums, a dictionary id or the legacy
//! format are rejected (`Unsupported`): the Python caller then decodes the
//! row through pyarrow. Corrupt input never reads or writes out of bounds
//! (the safe decoder checks both ends), and nothing is written past the
//! frame's content (`dst` beyond it is left untouched).

use std::os::raw::{c_char, c_int};

// Keeps the crate (and so the static liblz4 its build script compiles) linked.
use lz4_sys as _;

extern "C" {
    // Both are `LZ4LIB_API` in lz4.h (stable API); lz4-sys binds only the first.
    fn LZ4_decompress_safe(
        src: *const c_char,
        dst: *mut c_char,
        compressed_size: c_int,
        dst_capacity: c_int,
    ) -> c_int;
    fn LZ4_decompress_safe_usingDict(
        src: *const c_char,
        dst: *mut c_char,
        compressed_size: c_int,
        dst_capacity: c_int,
        dict_start: *const c_char,
        dict_size: c_int,
    ) -> c_int;
}

const MAGIC: [u8; 4] = [0x04, 0x22, 0x4d, 0x18];
const WINDOW: usize = 64 * 1024;

pub enum FrameError {
    /// A frame feature this decoder does not implement.
    Unsupported(&'static str),
    /// `dst` cannot hold the frame's content.
    TooSmall { needed: Option<u64>, capacity: usize },
    /// Malformed or truncated input.
    Corrupt(String),
}

impl FrameError {
    pub fn message(&self) -> String {
        match self {
            FrameError::Unsupported(what) => format!("unsupported lz4 frame: {what}"),
            FrameError::TooSmall { needed: Some(n), capacity } => {
                format!("destination too small: frame content is {n} bytes, buffer {capacity}")
            }
            FrameError::TooSmall { needed: None, capacity } => {
                format!("destination too small: frame content exceeds {capacity} bytes")
            }
            FrameError::Corrupt(why) => format!("corrupt lz4 frame: {why}"),
        }
    }
}

fn corrupt(why: &str) -> FrameError {
    FrameError::Corrupt(why.to_string())
}

/// Decode one compressed block into `dst` (`dict` is the output that precedes
/// it, empty for an independent block); `None` for a malformed block or one
/// whose output does not fit `dst`.
fn decode_block(block: &[u8], dst: &mut [u8], dict: &[u8]) -> Option<usize> {
    // Block and window sizes are at most 4 MB (checked by the caller), so
    // every length fits a C int.
    let (src_len, dst_cap) = (block.len() as c_int, dst.len() as c_int);
    // SAFETY: the three slices are valid for their lengths; the safe decoder
    // reads at most `src_len` bytes of `block` and `dict`, and writes at most
    // `dst_cap` bytes of `dst`. A `dict` that ends where `dst` starts is read
    // as liblz4's in-place prefix (the fast path), any other as an external
    // dictionary.
    let written = unsafe {
        if dict.is_empty() {
            LZ4_decompress_safe(block.as_ptr().cast(), dst.as_mut_ptr().cast(), src_len, dst_cap)
        } else {
            LZ4_decompress_safe_usingDict(
                block.as_ptr().cast(),
                dst.as_mut_ptr().cast(),
                src_len,
                dst_cap,
                dict.as_ptr().cast(),
                dict.len() as c_int,
            )
        }
    };
    usize::try_from(written).ok()
}

/// Decode the lz4 frame `src` into the front of `dst`; returns the content length.
pub fn decode_frame_into(src: &[u8], dst: &mut [u8]) -> Result<usize, FrameError> {
    if src.len() < 7 || src[..4] != MAGIC {
        return Err(FrameError::Unsupported("not an lz4 frame"));
    }
    let flg = src[4];
    let bd = src[5];
    if flg >> 6 != 1 {
        return Err(FrameError::Unsupported("frame version"));
    }
    if flg & 0x10 != 0 || flg & 0x04 != 0 {
        return Err(FrameError::Unsupported("checksums"));
    }
    if flg & 0x01 != 0 {
        return Err(FrameError::Unsupported("dictionary id"));
    }
    let linked = flg & 0x20 == 0;
    let block_max = match (bd >> 4) & 0x07 {
        4 => 64 * 1024,
        5 => 256 * 1024,
        6 => 1024 * 1024,
        7 => 4 * 1024 * 1024,
        _ => return Err(corrupt("block size code")),
    };
    let mut pos = 6;
    let content_size = if flg & 0x08 != 0 {
        let raw: [u8; 8] = src
            .get(pos..pos + 8)
            .and_then(|s| s.try_into().ok())
            .ok_or_else(|| corrupt("truncated header"))?;
        pos += 8;
        Some(u64::from_le_bytes(raw))
    } else {
        None
    };
    pos += 1; // header checksum byte
    let capacity = dst.len();
    // Nothing is written past the content (a narrowed row's slack follows it).
    let limit = match content_size {
        Some(n) if n > capacity as u64 => {
            return Err(FrameError::TooSmall { needed: Some(n), capacity });
        }
        Some(n) => n as usize,
        None => capacity,
    };
    let dst = &mut dst[..limit];
    let mut out = 0usize;
    loop {
        let word: [u8; 4] = src
            .get(pos..pos + 4)
            .and_then(|s| s.try_into().ok())
            .ok_or_else(|| corrupt("truncated block header"))?;
        pos += 4;
        let word = u32::from_le_bytes(word);
        if word == 0 {
            break;
        }
        let size = (word & 0x7fff_ffff) as usize;
        if size > block_max {
            return Err(corrupt("block larger than the frame's block size"));
        }
        let block = src.get(pos..pos + size).ok_or_else(|| corrupt("truncated block"))?;
        pos += size;
        if word & 0x8000_0000 != 0 {
            let target = dst
                .get_mut(out..out + size)
                .ok_or(FrameError::TooSmall { needed: content_size, capacity })?;
            target.copy_from_slice(block);
            out += size;
            continue;
        }
        let (head, tail) = dst.split_at_mut(out);
        let room = block_max.min(tail.len());
        let dict = if linked { &head[out.saturating_sub(WINDOW)..] } else { &head[..0] };
        // liblz4's wild copies may scribble on the room past a block's output
        // (the next block overwrites it). With the content size known the
        // last block's room is exactly its output; without it, the last block
        // (the end mark follows) goes through a scratch buffer, so the bytes
        // past the content stay untouched.
        let last_of_unsized = content_size.is_none() && src.get(pos..pos + 4) == Some(&[0; 4]);
        let written = if last_of_unsized {
            let mut scratch = vec![0u8; room];
            decode_block(block, &mut scratch, dict).inspect(|&n| {
                tail[..n].copy_from_slice(&scratch[..n]);
            })
        } else {
            decode_block(block, &mut tail[..room], dict)
        };
        out += written.ok_or_else(|| {
            if content_size.is_none() && room < block_max {
                FrameError::TooSmall { needed: None, capacity }
            } else {
                FrameError::Corrupt("malformed block".to_string())
            }
        })?;
    }
    if let Some(n) = content_size {
        if out as u64 != n {
            return Err(corrupt("content size mismatch"));
        }
    }
    Ok(out)
}
