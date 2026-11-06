//! Whole-file decoder of the `tel.v2` columnar telemetry files (tif1's
//! `core_utils/columnar_telemetry.py`, `decode_driver_file`).
//!
//! One call decodes the body of a driver file (the content of its zstd
//! frame): the orjson header, the byte-shuffled buffers, the value encodings,
//! the anchors and lap dates, every prediction (the `telcols` kernels) and
//! the residuals, without the GIL. It follows the Python decoder step for
//! step, so the decoded laps are the same bits. Anything it does not
//! recognise (an encoding, a dtype or a header shape the encoder does not
//! write, an inconsistent length) is an error: the caller then runs the
//! Python decoder, which decides (decodes the file or rejects it), so a
//! malformed file is rejected exactly as before.

use std::collections::HashMap;
use std::sync::Arc;

use crate::telcols;

pub type Res<T> = Result<T, String>;

/// Time spent per decoding phase (test builds only: `bench_bodies`).
#[cfg(test)]
mod timing {
    use std::cell::RefCell;
    use std::collections::HashMap;
    use std::time::Duration;

    thread_local! {
        pub static PHASES: RefCell<HashMap<String, Duration>> = RefCell::new(HashMap::new());
    }

    pub fn add(name: &str, elapsed: Duration) {
        PHASES.with(|p| *p.borrow_mut().entry(name.to_owned()).or_default() += elapsed);
    }
}

macro_rules! timed {
    ($name:expr, $e:expr) => {{
        #[cfg(test)]
        let t0 = std::time::Instant::now();
        let value = $e;
        #[cfg(test)]
        timing::add($name, t0.elapsed());
        value
    }};
}

// -- JSON (the header is orjson output) -----------------------------------------

/// A parsed JSON value. Objects keep their items in order; a repeated key
/// resolves to its last value (orjson's and Python's dict behaviour).
#[derive(Clone, Debug)]
pub enum Json {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    Str(String),
    Arr(Vec<Json>),
    Obj(Vec<(String, Json)>),
}

impl Json {
    /// `obj[key]` (the last item named `key`), None when absent or not an object.
    pub fn get(&self, key: &str) -> Option<&Json> {
        match self {
            Json::Obj(items) => items.iter().rev().find(|(k, _)| k == key).map(|(_, v)| v),
            _ => None,
        }
    }

    /// `obj.get(key)` that is not JSON null.
    fn present(&self, key: &str) -> Option<&Json> {
        self.get(key).filter(|v| !matches!(v, Json::Null))
    }

    fn need(&self, key: &str) -> Res<&Json> {
        self.get(key).ok_or_else(|| format!("missing {key:?}"))
    }

    fn as_str(&self) -> Res<&str> {
        match self {
            Json::Str(s) => Ok(s),
            _ => Err("expected a string".into()),
        }
    }

    fn as_int(&self) -> Res<i64> {
        match self {
            Json::Int(v) => Ok(*v),
            _ => Err("expected an integer".into()),
        }
    }

    fn as_arr(&self) -> Res<&[Json]> {
        match self {
            Json::Arr(v) => Ok(v),
            _ => Err("expected an array".into()),
        }
    }

    fn as_obj(&self) -> Res<&[(String, Json)]> {
        match self {
            Json::Obj(v) => Ok(v),
            _ => Err("expected an object".into()),
        }
    }

    /// `entry.get(key) or {}`: the object's items, empty for a missing or
    /// falsy value; any other value is an error.
    fn items_or_empty(&self, key: &str) -> Res<&[(String, Json)]> {
        match self.get(key) {
            None | Some(Json::Null) | Some(Json::Bool(false)) => Ok(&[]),
            Some(Json::Obj(items)) => Ok(items),
            Some(_) => Err(format!("{key:?} is not an object")),
        }
    }
}

/// Last value of `key` in `items` (a JSON object's items).
fn lookup<'a>(items: &'a [(String, Json)], key: &str) -> Option<&'a Json> {
    items.iter().rev().find(|(k, _)| k == key).map(|(_, v)| v)
}

const MAX_DEPTH: usize = 64;

struct JsonParser<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl<'a> JsonParser<'a> {
    fn parse(bytes: &'a [u8]) -> Res<Json> {
        let mut parser = JsonParser { bytes, pos: 0 };
        let value = parser.value(0)?;
        parser.ws();
        if parser.pos != bytes.len() {
            return Err("trailing data after the header".into());
        }
        Ok(value)
    }

    fn ws(&mut self) {
        while let Some(b' ' | b'\n' | b'\r' | b'\t') = self.bytes.get(self.pos) {
            self.pos += 1;
        }
    }

    fn literal(&mut self, text: &[u8], value: Json) -> Res<Json> {
        if self.bytes[self.pos..].starts_with(text) {
            self.pos += text.len();
            Ok(value)
        } else {
            Err("bad literal".into())
        }
    }

    fn value(&mut self, depth: usize) -> Res<Json> {
        if depth > MAX_DEPTH {
            return Err("header nested too deeply".into());
        }
        self.ws();
        match self.bytes.get(self.pos) {
            Some(b'{') => {
                self.pos += 1;
                let mut items = Vec::new();
                self.ws();
                if self.bytes.get(self.pos) == Some(&b'}') {
                    self.pos += 1;
                    return Ok(Json::Obj(items));
                }
                loop {
                    self.ws();
                    if self.bytes.get(self.pos) != Some(&b'"') {
                        return Err("expected a key".into());
                    }
                    self.pos += 1;
                    let key = self.string()?;
                    self.ws();
                    if self.bytes.get(self.pos) != Some(&b':') {
                        return Err("expected ':'".into());
                    }
                    self.pos += 1;
                    let value = self.value(depth + 1)?;
                    items.push((key, value));
                    self.ws();
                    match self.bytes.get(self.pos) {
                        Some(b',') => self.pos += 1,
                        Some(b'}') => {
                            self.pos += 1;
                            return Ok(Json::Obj(items));
                        }
                        _ => return Err("expected ',' or '}'".into()),
                    }
                }
            }
            Some(b'[') => {
                self.pos += 1;
                let mut items = Vec::new();
                self.ws();
                if self.bytes.get(self.pos) == Some(&b']') {
                    self.pos += 1;
                    return Ok(Json::Arr(items));
                }
                loop {
                    items.push(self.value(depth + 1)?);
                    self.ws();
                    match self.bytes.get(self.pos) {
                        Some(b',') => self.pos += 1,
                        Some(b']') => {
                            self.pos += 1;
                            return Ok(Json::Arr(items));
                        }
                        _ => return Err("expected ',' or ']'".into()),
                    }
                }
            }
            Some(b'"') => {
                self.pos += 1;
                self.string().map(Json::Str)
            }
            Some(b't') => self.literal(b"true", Json::Bool(true)),
            Some(b'f') => self.literal(b"false", Json::Bool(false)),
            Some(b'n') => self.literal(b"null", Json::Null),
            Some(b'-' | b'0'..=b'9') => self.number(),
            _ => Err("unexpected byte in the header".into()),
        }
    }

    fn digits(&mut self) -> usize {
        let start = self.pos;
        while let Some(b'0'..=b'9') = self.bytes.get(self.pos) {
            self.pos += 1;
        }
        self.pos - start
    }

    /// RFC 8259 number. Integers must fit an int64 and floats must be finite
    /// (anything else is left to the Python decoder).
    fn number(&mut self) -> Res<Json> {
        let start = self.pos;
        if self.bytes.get(self.pos) == Some(&b'-') {
            self.pos += 1;
        }
        match self.bytes.get(self.pos) {
            Some(b'0') => self.pos += 1,
            Some(b'1'..=b'9') => {
                self.digits();
            }
            _ => return Err("bad number".into()),
        }
        let mut integer = true;
        if self.bytes.get(self.pos) == Some(&b'.') {
            self.pos += 1;
            integer = false;
            if self.digits() == 0 {
                return Err("bad number".into());
            }
        }
        if let Some(b'e' | b'E') = self.bytes.get(self.pos) {
            self.pos += 1;
            integer = false;
            if let Some(b'+' | b'-') = self.bytes.get(self.pos) {
                self.pos += 1;
            }
            if self.digits() == 0 {
                return Err("bad number".into());
            }
        }
        let token = &self.bytes[start..self.pos];
        // The token is ASCII by construction.
        let text = std::str::from_utf8(token).map_err(|_| "bad number")?;
        if integer {
            text.parse::<i64>().map(Json::Int).map_err(|_| "integer out of range".into())
        } else {
            let value: f64 = fast_float2::parse(token).map_err(|_| "bad number")?;
            if value.is_finite() {
                Ok(Json::Float(value))
            } else {
                Err("float out of range".into())
            }
        }
    }

    fn hex4(&mut self) -> Res<u32> {
        let hex = self.bytes.get(self.pos..self.pos + 4).ok_or("bad unicode escape")?;
        let text = std::str::from_utf8(hex).map_err(|_| "bad unicode escape")?;
        if !text.bytes().all(|b| b.is_ascii_hexdigit()) {
            return Err("bad unicode escape".into());
        }
        self.pos += 4;
        u32::from_str_radix(text, 16).map_err(|_| "bad unicode escape".into())
    }

    /// A string (opening quote consumed): escapes resolved, UTF-8 checked, raw
    /// control characters and lone surrogates rejected.
    fn string(&mut self) -> Res<String> {
        let mut out: Vec<u8> = Vec::new();
        loop {
            let start = self.pos;
            while let Some(&b) = self.bytes.get(self.pos) {
                if b == b'"' || b == b'\\' || b < 0x20 {
                    break;
                }
                self.pos += 1;
            }
            out.extend_from_slice(&self.bytes[start..self.pos]);
            match self.bytes.get(self.pos) {
                Some(b'"') => {
                    self.pos += 1;
                    return String::from_utf8(out).map_err(|_| "invalid utf-8 in the header".into());
                }
                Some(b'\\') => {
                    self.pos += 1;
                    let esc = *self.bytes.get(self.pos).ok_or("bad escape")?;
                    self.pos += 1;
                    match esc {
                        b'"' => out.push(b'"'),
                        b'\\' => out.push(b'\\'),
                        b'/' => out.push(b'/'),
                        b'b' => out.push(8),
                        b'f' => out.push(12),
                        b'n' => out.push(b'\n'),
                        b'r' => out.push(b'\r'),
                        b't' => out.push(b'\t'),
                        b'u' => {
                            let mut code = self.hex4()?;
                            if (0xD800..0xDC00).contains(&code) {
                                if self.bytes.get(self.pos..self.pos + 2) != Some(b"\\u") {
                                    return Err("lone surrogate".into());
                                }
                                self.pos += 2;
                                let low = self.hex4()?;
                                if !(0xDC00..0xE000).contains(&low) {
                                    return Err("lone surrogate".into());
                                }
                                code = 0x10000 + ((code - 0xD800) << 10) + (low - 0xDC00);
                            }
                            let ch = char::from_u32(code).ok_or("lone surrogate")?;
                            let mut buf = [0u8; 4];
                            out.extend_from_slice(ch.encode_utf8(&mut buf).as_bytes());
                        }
                        _ => return Err("bad escape".into()),
                    }
                }
                _ => return Err("unterminated string or control character".into()),
            }
        }
    }
}

// -- buffers ------------------------------------------------------------------------

/// The numpy dtypes the encoder writes (`dtype.str`, little-endian).
#[derive(Clone, Copy, PartialEq, Eq)]
enum Dtype {
    I1,
    U1,
    I2,
    U2,
    I4,
    U4,
    I8,
    U8,
    F8,
}

impl Dtype {
    fn parse(text: &str) -> Res<Dtype> {
        Ok(match text {
            "|i1" => Dtype::I1,
            "|u1" => Dtype::U1,
            "<i2" => Dtype::I2,
            "<u2" => Dtype::U2,
            "<i4" => Dtype::I4,
            "<u4" => Dtype::U4,
            "<i8" => Dtype::I8,
            "<u8" => Dtype::U8,
            "<f8" => Dtype::F8,
            _ => return Err(format!("dtype {text:?}")),
        })
    }

    fn width(self) -> usize {
        match self {
            Dtype::I1 | Dtype::U1 => 1,
            Dtype::I2 | Dtype::U2 => 2,
            Dtype::I4 | Dtype::U4 => 4,
            Dtype::I8 | Dtype::U8 | Dtype::F8 => 8,
        }
    }

    fn signed(self) -> bool {
        matches!(self, Dtype::I1 | Dtype::I2 | Dtype::I4 | Dtype::I8)
    }

    fn unsigned(self) -> bool {
        matches!(self, Dtype::U1 | Dtype::U2 | Dtype::U4 | Dtype::U8)
    }
}

/// A `[dtype, offset, count]` buffer of the body: its dtype and the values'
/// little-endian bytes as zero-extended u64 (the byte planes gathered back).
struct Buffer {
    dtype: Dtype,
    bits: Vec<u64>,
}

fn gather<const W: usize>(planes: &[u8], count: usize) -> Vec<u64> {
    let mut out = vec![0u64; count];
    for b in 0..W {
        let plane = &planes[b * count..(b + 1) * count];
        let shift = 8 * b as u32;
        for (slot, &byte) in out.iter_mut().zip(plane) {
            *slot |= (byte as u64) << shift;
        }
    }
    out
}

fn buffer(descriptor: &Json, body: &[u8]) -> Res<Buffer> {
    let parts = descriptor.as_arr()?;
    if parts.len() != 3 {
        return Err("buffer descriptor is not [dtype, offset, count]".into());
    }
    let dtype = Dtype::parse(parts[0].as_str()?)?;
    let (offset, count) = (parts[1].as_int()?, parts[2].as_int()?);
    if offset < 0 || count < 0 {
        return Err("buffer outside the body".into());
    }
    let (offset, count) = (offset as usize, count as usize);
    let nbytes = count.checked_mul(dtype.width()).ok_or("buffer outside the body")?;
    let end = offset.checked_add(nbytes).ok_or("buffer outside the body")?;
    if end > body.len() {
        return Err("buffer outside the body".into());
    }
    let planes = &body[offset..end];
    let bits = match dtype.width() {
        1 => planes.iter().map(|&b| b as u64).collect(),
        2 => gather::<2>(planes, count),
        4 => gather::<4>(planes, count),
        _ => gather::<8>(planes, count),
    };
    Ok(Buffer { dtype, bits })
}

impl Buffer {
    /// numpy `astype(np.int64)` of an integer buffer.
    fn to_i64(&self) -> Res<Vec<i64>> {
        let shift = 64 - 8 * self.dtype.width() as u32;
        if self.dtype.signed() {
            Ok(self.bits.iter().map(|&v| ((v << shift) as i64) >> shift).collect())
        } else if self.dtype.unsigned() {
            Ok(self.bits.iter().map(|&v| v as i64).collect())
        } else {
            Err("integer buffer expected".into())
        }
    }

    /// An integer buffer cast to float64 (numpy assignment into a float64 array).
    fn ints_to_f64(&self) -> Res<Vec<f64>> {
        let shift = 64 - 8 * self.dtype.width() as u32;
        if self.dtype.signed() {
            Ok(self.bits.iter().map(|&v| (((v << shift) as i64) >> shift) as f64).collect())
        } else if self.dtype.unsigned() {
            Ok(self.bits.iter().map(|&v| v as f64).collect())
        } else {
            Err("integer buffer expected".into())
        }
    }

    fn floats(self) -> Res<Vec<f64>> {
        if self.dtype != Dtype::F8 {
            return Err("float64 buffer expected".into());
        }
        Ok(self.bits.into_iter().map(f64::from_bits).collect())
    }

    /// Residuals: an unsigned buffer (zero-extended to uint64).
    fn residuals(self, count: usize) -> Res<Vec<u64>> {
        if !self.dtype.unsigned() || self.bits.len() != count {
            return Err("residuals do not match their rows".into());
        }
        Ok(self.bits)
    }

    /// `np.unpackbits(buffer, count=count)` (big bit order, zero-padded).
    fn unpack(&self, count: usize) -> Res<Vec<bool>> {
        if self.dtype != Dtype::U1 {
            return Err("bit mask is not uint8".into());
        }
        Ok((0..count)
            .map(|i| self.bits.get(i / 8).is_some_and(|&byte| (byte >> (7 - i % 8)) & 1 == 1))
            .collect())
    }
}

// -- layout -------------------------------------------------------------------------

/// The laps' `time` samples concatenated in lap order (`_Rows`).
struct Rows {
    sizes: Vec<i64>,
    total: usize,
}

impl Rows {
    /// (first row, one past the last row) of each lap.
    fn laps(&self) -> impl Iterator<Item = (usize, usize)> + '_ {
        self.sizes.iter().scan(0usize, |start, &size| {
            let lap = (*start, *start + size as usize);
            *start = lap.1;
            Some(lap)
        })
    }
}

/// The rows where `mask` is set and those where it is not (ascending), without branches.
fn split_rows(mask: &[bool]) -> (Vec<i64>, Vec<i64>) {
    let (mut on, mut off) = (vec![0i64; mask.len()], vec![0i64; mask.len()]);
    let (mut a, mut b) = (0usize, 0usize);
    for (i, &m) in mask.iter().enumerate() {
        on[a] = i as i64;
        off[b] = i as i64;
        a += m as usize;
        b += !m as usize;
    }
    on.truncate(a);
    off.truncate(b);
    (on, off)
}

/// `out[rows[k]] = values[k]`.
fn scatter(out: &mut [f64], rows: &[i64], values: &[f64]) {
    for (&row, &value) in rows.iter().zip(values) {
        out[row as usize] = value;
    }
}

/// Car rows and each row's neighbouring car rows in its lap (`_Anchors`),
/// with the row lists every predicted column of the file shares.
struct Anchors {
    mask: Vec<bool>,
    /// Per row: the last car row at or before it in its lap (`has_prev`), the
    /// first at or after it (`has_next`); meaningless where the flag is false.
    prev: Vec<i64>,
    next: Vec<i64>,
    has_prev: Vec<bool>,
    has_next: Vec<bool>,
    /// The car rows and the other rows.
    rows: Vec<i64>,
    others: Vec<i64>,
    /// `interp`: the other rows between two car rows of their lap, those car
    /// rows, and the other rows that are not between two (`between()`).
    between: Vec<i64>,
    j0: Vec<i64>,
    j1: Vec<i64>,
    outside: Vec<i64>,
    /// `integ`: the car rows with a car row before them in their lap, that
    /// row, and the car rows without one (each lap's first).
    integ: Vec<i64>,
    integ_prev: Vec<i64>,
    integ_first: Vec<i64>,
}

impl Anchors {
    fn new(mask: Vec<bool>, layout: &Rows) -> Anchors {
        let total = layout.total;
        let mut prev = vec![0i64; total];
        let mut next = vec![0i64; total];
        let mut has_prev = vec![false; total];
        let mut has_next = vec![false; total];
        let (mut integ, mut integ_prev, mut integ_first) = (vec![], vec![], vec![]);
        for (start, stop) in layout.laps() {
            let mut last = -1i64;
            for i in start..stop {
                if mask[i] {
                    // `before`: the previous car row of the lap, strictly before `i`
                    if last >= 0 {
                        integ.push(i as i64);
                        integ_prev.push(last);
                    } else {
                        integ_first.push(i as i64);
                    }
                    last = i as i64;
                }
                prev[i] = last;
                has_prev[i] = last >= 0;
            }
            let mut upcoming = -1i64;
            for i in (start..stop).rev() {
                if mask[i] {
                    upcoming = i as i64;
                }
                next[i] = upcoming;
                has_next[i] = upcoming >= 0;
            }
        }
        let (rows, others) = split_rows(&mask);
        let (mut between, mut j0, mut j1, mut outside) = (vec![], vec![], vec![], vec![]);
        for &i in &others {
            let i = i as usize;
            if has_prev[i] && has_next[i] {
                between.push(i as i64);
                j0.push(prev[i]);
                j1.push(next[i]);
            } else {
                outside.push(i as i64);
            }
        }
        Anchors {
            mask,
            prev,
            next,
            has_prev,
            has_next,
            rows,
            others,
            between,
            j0,
            j1,
            outside,
            integ,
            integ_prev,
            integ_first,
        }
    }
}

// -- columns ------------------------------------------------------------------------

/// The decoding state: layout, anchors, lap start dates, decoded float64
/// channels (row-aligned) and the rows' dates of one `time` channel.
struct Context {
    rows: Option<Rows>,
    anchors: Option<Anchors>,
    starts: Option<Vec<i64>>,
    decoded: HashMap<String, Arc<Vec<f64>>>,
    dates: Option<(String, Vec<f64>)>,
    /// Per lap: the point rows of the last spline over `dates` and its basis.
    splines: Vec<Option<(Vec<i64>, telcols::Basis)>>,
}

const MS_LIMIT: f64 = (1u64 << 40) as f64;
const POW10: [f64; 23] = [
    1e0, 1e1, 1e2, 1e3, 1e4, 1e5, 1e6, 1e7, 1e8, 1e9, 1e10, 1e11, 1e12, 1e13, 1e14, 1e15, 1e16,
    1e17, 1e18, 1e19, 1e20, 1e21, 1e22,
];

impl Context {
    /// The decoded reference channel `descriptor[role]` (`context.channel`).
    fn channel<'c>(
        decoded: &'c HashMap<String, Arc<Vec<f64>>>,
        descriptor: &Json,
        role: &str,
        total: usize,
    ) -> Res<&'c [f64]> {
        let name = descriptor.need(role)?.as_str()?;
        match decoded.get(name) {
            Some(values) if values.len() == total => Ok(values),
            _ => Err(format!("predicted column without its reference {name:?}")),
        }
    }

    /// Makes `self.dates` the row dates of the channel `descriptor[role]`
    /// (`_row_dates`: float64 nanoseconds, NaN where `time` is unusable).
    fn ensure_dates(&mut self, descriptor: &Json, role: &str, total: usize) -> Res<()> {
        let (starts, rows) = match (&self.starts, &self.rows) {
            (Some(starts), Some(rows)) => (starts, rows),
            _ => return Err("'interp' column without the lap start dates".into()),
        };
        let time = Context::channel(&self.decoded, descriptor, role, total)?;
        let name = descriptor.need(role)?.as_str()?;
        if matches!(&self.dates, Some((cached, _)) if cached == name) {
            return Ok(());
        }
        let mut dates = Vec::with_capacity(total);
        let mut row = 0usize;
        for (&start, &size) in starts.iter().zip(&rows.sizes) {
            for &t in &time[row..row + size as usize] {
                let ms = (t * 1000.0).round_ties_even();
                if ms.abs() < MS_LIMIT {
                    let ns = start.wrapping_add((ms as i64).wrapping_mul(1_000_000));
                    dates.push(ns as f64);
                } else {
                    dates.push(f64::NAN);
                }
            }
            row += size as usize;
        }
        self.dates = Some((name.to_owned(), dates));
        self.splines.clear();
        Ok(())
    }

    fn rows_of(&self, enc: &str, total: usize) -> Res<&Rows> {
        match &self.rows {
            Some(rows) if rows.total == total => Ok(rows),
            _ => Err(format!("{enc:?} column without the row layout")),
        }
    }
}

/// A value-encoded float64 column (`_decode_values` for `f64`, `dec`, `split`,
/// and `int` as a nested part assigned into a float64 column).
fn values_f64(descriptor: &Json, body: &[u8], total: usize) -> Res<Vec<f64>> {
    let out = match descriptor.need("enc")?.as_str()? {
        "f64" => buffer(descriptor.need("buf")?, body)?.floats()?,
        "dec" => {
            let mut ints = buffer(descriptor.need("buf")?, body)?.to_i64()?;
            match descriptor.get("delta") {
                None | Some(Json::Null) | Some(Json::Bool(false)) => {}
                Some(Json::Bool(true)) => {
                    let mut sum = 0i64;
                    for v in ints.iter_mut() {
                        sum = sum.wrapping_add(*v);
                        *v = sum;
                    }
                }
                Some(_) => return Err("'delta' is not a boolean".into()),
            }
            let k = descriptor.need("k")?.as_int()?;
            let scale = *usize::try_from(k).ok().and_then(|k| POW10.get(k)).ok_or("'dec' scale")?;
            ints.into_iter().map(|n| n as f64 / scale).collect()
        }
        "split" => {
            let whole = buffer(descriptor.need("mask")?, body)?.unpack(total)?;
            let ints = buffer(descriptor.need("ints")?, body)?.ints_to_f64()?;
            let rest = buffer(descriptor.need("rest")?, body)?.floats()?;
            let (on, off) = split_rows(&whole);
            if ints.len() != on.len() || rest.len() != off.len() {
                return Err("'split' parts do not match the mask".into());
            }
            let mut out = vec![0.0f64; total];
            scatter(&mut out, &on, &ints);
            scatter(&mut out, &off, &rest);
            out
        }
        // a nested part: `astype(np.int64)`, then assigned into the float64 column
        "int" => buffer(descriptor.need("buf")?, body)?.to_i64()?.into_iter().map(|v| v as f64).collect(),
        enc => return Err(format!("unknown column encoding {enc:?}")),
    };
    if out.len() != total {
        return Err("column length does not match its laps".into());
    }
    Ok(out)
}

fn kernel<T>(result: Result<T, &'static str>) -> Res<T> {
    result.map_err(str::to_owned)
}

/// Per lap, the spline through the rows `points` flags, evaluated at the
/// lap's other rows, in row order (`telcols::spline` with the targets every
/// caller passes: `flatnonzero(~points)`; NaN predictions are 0.0). A lap
/// whose point rows equal those of the previous spline column over the same
/// dates (`x`, `y`, `z` of most laps) reuses its basis: the same operations
/// on the same inputs, so the same bits.
fn spline_shared(
    dates: &[f64],
    values: &[f64],
    points: &[bool],
    sizes: &[i64],
    cache: &mut Vec<Option<(Vec<i64>, telcols::Basis)>>,
) -> Vec<f64> {
    cache.resize_with(sizes.len(), || None);
    let mut out = Vec::with_capacity(points.len());
    let mut start = 0usize;
    for (lap, &size) in sizes.iter().enumerate() {
        let stop = start + size as usize;
        let (mut on, mut off) = split_rows(&points[start..stop]);
        for row in on.iter_mut().chain(off.iter_mut()) {
            *row += start as i64;
        }
        start = stop;
        if off.is_empty() {
            continue;
        }
        if !matches!(&cache[lap], Some((rows, _)) if *rows == on) {
            let xd: Vec<f64> = on.iter().map(|&r| dates[r as usize]).collect();
            let xe: Vec<f64> = off.iter().map(|&r| dates[r as usize]).collect();
            let basis = timed!("b.new", telcols::Basis::new(&xd, &xe));
            cache[lap] = Some((on, basis));
        }
        let Some((rows, basis)) = &cache[lap] else { unreachable!() };
        let yd: Vec<f64> = rows.iter().map(|&r| values[r as usize]).collect();
        let first = out.len();
        out.resize(first + off.len(), 0.0);
        timed!("b.eval", basis.evaluate(&yd, &mut out[first..]));
    }
    for v in out.iter_mut() {
        if v.is_nan() {
            *v = 0.0;
        }
    }
    out
}

/// One float64 column of every lap, concatenated (`_decode_column`).
fn column_f64(descriptor: &Json, body: &[u8], total: usize, ctx: &mut Context) -> Res<Vec<f64>> {
    let enc = descriptor.need("enc")?.as_str()?;
    match enc {
        "f64" | "dec" | "split" => return values_f64(descriptor, body, total),
        "int" => return Err("'int' float64 column".into()),
        _ => {}
    }
    let sizes = ctx.rows_of(enc, total)?.sizes.clone();
    if matches!(enc, "interp" | "spline" | "integs") {
        ctx.ensure_dates(descriptor, "time", total)?;
    }
    let dates = ctx.dates.as_ref().map(|(_, dates)| dates.as_slice()).unwrap_or(&[]);
    match enc {
        "lerp" | "interp" => {
            let anchors = ctx.anchors.as_ref().ok_or("'interp' column without anchors")?;
            let mut lerp_rows: (Vec<i64>, Vec<i64>, Vec<i64>, Vec<i64>) = Default::default();
            let (predicted, j0, j1, rest) = if enc == "interp" {
                (&anchors.between, &anchors.j0, &anchors.j1, &anchors.outside)
            } else {
                let reference = Context::channel(&ctx.decoded, descriptor, "ref", total)?;
                let (rows, a, b, rest) = &mut lerp_rows;
                for &i in &anchors.others {
                    let i = i as usize;
                    let j0 = if anchors.has_prev[i] { anchors.prev[i] } else { 0 };
                    let j1 = if anchors.has_next[i] { anchors.next[i] } else { 0 };
                    // NaN != NaN, as in numpy
                    let moves = reference[j0 as usize] != reference[j1 as usize];
                    if anchors.has_prev[i] && anchors.has_next[i] && moves {
                        rows.push(i as i64);
                        a.push(j0);
                        b.push(j1);
                    } else {
                        rest.push(i as i64);
                    }
                }
                (&lerp_rows.0, &lerp_rows.1, &lerp_rows.2, &lerp_rows.3)
            };
            let mut out = vec![0.0f64; total];
            let anchor_values = values_f64(descriptor.need("anchor")?, body, anchors.rows.len())?;
            scatter(&mut out, &anchors.rows, &anchor_values);
            let rest_values = values_f64(descriptor.need("rest")?, body, rest.len())?;
            scatter(&mut out, rest, &rest_values);
            let zigzag = buffer(descriptor.need("res")?, body)?.residuals(predicted.len())?;
            if enc == "interp" {
                timed!("k.interp", kernel(telcols::interp(&mut out, dates, predicted, j0, j1, &zigzag))?);
            } else {
                let reference = Context::channel(&ctx.decoded, descriptor, "ref", total)?;
                for (k, &row) in predicted.iter().enumerate() {
                    let (a, b, r) = (j0[k] as usize, j1[k] as usize, row as usize);
                    let r0 = reference[a];
                    let fraction = (reference[r] - r0) / (reference[b] - r0);
                    let y0 = out[a];
                    let p = y0 + (out[b] - y0) * fraction;
                    out[r] = telcols::apply_residual(if p.is_nan() { 0.0 } else { p }, zigzag[k]);
                }
            }
            Ok(out)
        }
        "spline" => {
            let whole = buffer(descriptor.need("mask")?, body)?.unpack(total)?;
            let ints = buffer(descriptor.need("ints")?, body)?.ints_to_f64()?;
            let (points, targets) = split_rows(&whole);
            if ints.len() != points.len() {
                return Err("'spline' integers do not match the mask".into());
            }
            let mut out = vec![0.0f64; total];
            scatter(&mut out, &points, &ints);
            let zigzag = buffer(descriptor.need("res")?, body)?.residuals(targets.len())?;
            let mut predicted =
                timed!("k.spline", spline_shared(dates, &out, &whole, &sizes, &mut ctx.splines));
            kernel(telcols::apply(&mut predicted, &zigzag))?;
            scatter(&mut out, &targets, &predicted);
            Ok(out)
        }
        "integ" | "integs" => {
            let anchors = ctx.anchors.as_ref().ok_or("'integ' column without anchors")?;
            let speed = Context::channel(&ctx.decoded, descriptor, "speed", total)?;
            let time = Context::channel(&ctx.decoded, descriptor, "time", total)?;
            let increments: Vec<f64> = anchors
                .integ
                .iter()
                .zip(&anchors.integ_prev)
                .map(|(&i, &p)| speed[i as usize] / 3.6 * (time[i as usize] - time[p as usize]))
                .collect();
            let integ_rest;
            let rest = if enc == "integs" {
                &anchors.integ_first
            } else {
                let mut keep = vec![true; total];
                for &i in &anchors.integ {
                    keep[i as usize] = false;
                }
                integ_rest = split_rows(&keep).0;
                &integ_rest
            };
            let mut out = vec![0.0f64; total];
            let rest_values = values_f64(descriptor.need("rest")?, body, rest.len())?;
            scatter(&mut out, rest, &rest_values);
            let zigzag = buffer(descriptor.need("res")?, body)?.residuals(anchors.integ.len())?;
            timed!(
                "k.integ",
                kernel(telcols::integ(&mut out, &anchors.integ, &anchors.integ_prev, &increments, &zigzag))?
            );
            if enc == "integs" {
                let spl = buffer(descriptor.need("spl")?, body)?.residuals(anchors.others.len())?;
                let mut between = timed!(
                    "k.spline",
                    spline_shared(dates, &out, &anchors.mask, &sizes, &mut ctx.splines)
                );
                kernel(telcols::apply(&mut between, &spl))?;
                scatter(&mut out, &anchors.others, &between);
            }
            Ok(out)
        }
        "ratio" => {
            let numerator = Context::channel(&ctx.decoded, descriptor, "of", total)?;
            let divisors = buffer(descriptor.need("div")?, body)?.floats()?;
            if divisors.len() != sizes.len() {
                return Err("'ratio' divisors do not match the laps".into());
            }
            let zigzag = buffer(descriptor.need("res")?, body)?.residuals(total)?;
            let mut out = Vec::with_capacity(total);
            for (&divisor, &size) in divisors.iter().zip(&sizes) {
                let row = out.len();
                for &n in &numerator[row..row + size as usize] {
                    let p = n / divisor;
                    out.push(if p.is_nan() { 0.0 } else { p });
                }
            }
            kernel(telcols::apply(&mut out, &zigzag))?;
            Ok(out)
        }
        "accx" => {
            let speed = Context::channel(&ctx.decoded, descriptor, "speed", total)?;
            let time = Context::channel(&ctx.decoded, descriptor, "time", total)?;
            let zigzag = buffer(descriptor.need("res")?, body)?.residuals(total)?;
            let mut out = timed!("k.accx", kernel(telcols::accx(speed, time, &sizes))?);
            kernel(telcols::apply(&mut out, &zigzag))?;
            Ok(out)
        }
        "lat" => {
            let decoded = &ctx.decoded;
            let speed = Context::channel(decoded, descriptor, "speed", total)?;
            let x = Context::channel(decoded, descriptor, "x", total)?;
            let other = Context::channel(decoded, descriptor, "other", total)?;
            let distance = Context::channel(decoded, descriptor, "distance", total)?;
            let mut out = timed!("k.lateral", kernel(telcols::lateral(speed, x, other, distance, &sizes))?);
            let zigzag = buffer(descriptor.need("res")?, body)?.residuals(total)?;
            kernel(telcols::apply(&mut out, &zigzag))?;
            Ok(out)
        }
        _ => Err(format!("unknown column encoding {enc:?}")),
    }
}

// -- file ---------------------------------------------------------------------------

/// A decoded column of every lap.
enum Column {
    Floats(Arc<Vec<f64>>),
    Ints(Vec<i64>),
}

/// One value of a lap's `tel` mapping.
pub enum Item<'a> {
    Floats(&'a [f64]),
    Ints(&'a [i64]),
    /// `DriverAhead`: the int32 codes and the lap's category list.
    Codes(Vec<i32>, &'a Json),
    Json(&'a Json),
}

/// The decoded file: the header and the columns the laps' items point into.
pub struct File {
    header: Json,
    columns: HashMap<(String, String), Column>,
}

const FORMAT_VERSION: i64 = 2;
const HEADER_SIZE: usize = 4;

/// Decode a file body (the decompressed zstd frame).
pub fn decode(body: &[u8]) -> Res<File> {
    let head = body.get(..HEADER_SIZE).ok_or("body shorter than its header length")?;
    let header_len = u32::from_le_bytes(head.try_into().map_err(|_| "header length")?) as usize;
    let header_end = HEADER_SIZE.checked_add(header_len).ok_or("header length")?;
    let header = timed!(
        "header",
        JsonParser::parse(body.get(HEADER_SIZE..header_end).ok_or("truncated header")?)?
    );
    let start = header_end + (8 - header_end % 8) % 8;
    let buffers = body.get(start..).unwrap_or(&[]);
    let laps = header.need("laps")?.as_arr()?;
    if header.get("version").map(Json::as_int).transpose()? != Some(FORMAT_VERSION) {
        return Err("unsupported version".into());
    }

    // Per (channel, kind): the sum of the laps' lengths.
    let mut totals: HashMap<(String, String), usize> = HashMap::new();
    let size_limit = (body.len() as u64).saturating_mul(8).saturating_add(64);
    let mut time_sizes: Option<Vec<i64>> = Some(Vec::with_capacity(laps.len()));
    for entry in laps {
        let arrays = entry.need("arrays")?.as_obj()?;
        for (index, (key, spec)) in arrays.iter().enumerate() {
            // orjson keeps the last of repeated keys; the encoder never repeats one
            if arrays[..index].iter().any(|(other, _)| other == key) {
                return Err("a lap lists a channel twice".into());
            }
            let spec = spec.as_arr()?;
            if spec.len() != 2 {
                return Err("array spec is not [kind, size]".into());
            }
            let (kind, size) = (spec[0].as_str()?, spec[1].as_int()?);
            // every sample takes at least one bit of the body: a larger size
            // is a corrupt header, not an allocation to attempt
            if size < 0 || size as u64 > size_limit {
                return Err("array size outside the body".into());
            }
            let total = totals.entry((key.clone(), kind.to_owned())).or_insert(0);
            *total += size as usize;
            if *total as u64 > size_limit {
                return Err("array size outside the body".into());
            }
        }
        // `_layout_of`: every lap needs a float64 `time` (its last spec).
        let time = lookup(entry.need("arrays")?.as_obj()?, "time");
        match (time, time_sizes.as_mut()) {
            (Some(Json::Arr(spec)), Some(sizes)) if spec.len() == 2 && spec[0].as_str()? == "f8" => {
                sizes.push(spec[1].as_int()?)
            }
            _ => time_sizes = None,
        }
    }
    let rows = time_sizes.map(|sizes| {
        let total = sizes.iter().sum::<i64>() as usize;
        Rows { sizes, total }
    });
    let mut anchors = None;
    let mut starts = None;
    if let Some(rows) = &rows {
        if let Some(packed) = header.present("anchors") {
            let mask = buffer(packed, buffers)?.unpack(rows.total)?;
            anchors = Some(timed!("anchors", Anchors::new(mask, rows)));
        }
        if let Some(dates) = header.present("dates") {
            let dates = buffer(dates, buffers)?;
            if dates.dtype != Dtype::I8 || dates.bits.len() != rows.sizes.len() {
                return Err("lap start dates do not match the laps".into());
            }
            starts = Some(dates.bits.into_iter().map(|v| v as i64).collect());
        }
    }
    let mut ctx = Context {
        rows,
        anchors,
        starts,
        decoded: HashMap::new(),
        dates: None,
        splines: Vec::new(),
    };
    let mut columns: HashMap<(String, String), Column> = HashMap::new();
    for descriptor in header.need("columns")?.as_arr()? {
        let key = descriptor.need("key")?.as_str()?.to_owned();
        let kind = descriptor.need("kind")?.as_str()?.to_owned();
        let total = totals.get(&(key.clone(), kind.clone())).copied().unwrap_or(0);
        let enc = descriptor.need("enc")?.as_str()?;
        let column = match kind.as_str() {
            "f8" => {
                let values =
                    Arc::new(timed!(enc, column_f64(descriptor, buffers, total, &mut ctx)?));
                if ctx.rows.as_ref().is_some_and(|rows| values.len() == rows.total) {
                    ctx.decoded.insert(key.clone(), Arc::clone(&values));
                }
                Column::Floats(values)
            }
            "i8" | "dict" if enc == "int" => {
                let values = buffer(descriptor.need("buf")?, buffers)?.to_i64()?;
                if values.len() != total {
                    return Err("column length does not match its laps".into());
                }
                Column::Ints(values)
            }
            _ => return Err(format!("{kind:?} column with encoding {enc:?}")),
        };
        if columns.insert((key, kind), column).is_some() {
            return Err("a column is listed twice".into());
        }
    }
    if columns.len() != totals.len() || totals.keys().any(|k| !columns.contains_key(k)) {
        return Err("columns do not match the laps".into());
    }
    Ok(File { header, columns })
}

impl File {
    /// Per lap in file order: (lap number, the `tel` items in key order).
    /// Mirrors the Python decoder's per-lap slicing (a cursor per column).
    pub fn laps(&self) -> Res<Vec<(i64, Vec<(&str, Item<'_>)>)>> {
        let mut cursors: HashMap<(&str, &str), usize> = HashMap::new();
        let mut out = Vec::new();
        for entry in self.header.need("laps")?.as_arr()? {
            let arrays = entry.need("arrays")?.as_obj()?;
            let json_values = entry.items_or_empty("json")?;
            let categories = entry.items_or_empty("cats")?;
            let mut items = Vec::new();
            for key in entry.need("keys")?.as_arr()? {
                let key = key.as_str()?;
                let Some(spec) = lookup(arrays, key) else {
                    let value = lookup(json_values, key).ok_or("lap value missing")?;
                    items.push((key, Item::Json(value)));
                    continue;
                };
                let spec = spec.as_arr()?;
                let (kind, size) = (spec[0].as_str()?, spec[1].as_int()? as usize);
                let cursor = cursors.entry((key, kind)).or_insert(0);
                let pos = *cursor;
                *cursor += size;
                let column = self
                    .columns
                    .get(&(key.to_owned(), kind.to_owned()))
                    .ok_or("lap channel without a column")?;
                let item = match (column, kind) {
                    (Column::Floats(values), _) => {
                        Item::Floats(&values[pos.min(values.len())..(pos + size).min(values.len())])
                    }
                    (Column::Ints(values), "dict") => {
                        let slice = &values[pos.min(values.len())..(pos + size).min(values.len())];
                        let cats = lookup(categories, key).ok_or("categories missing")?;
                        Item::Codes(slice.iter().map(|&v| v as i32).collect(), cats)
                    }
                    (Column::Ints(values), _) => {
                        Item::Ints(&values[pos.min(values.len())..(pos + size).min(values.len())])
                    }
                };
                items.push((key, item));
            }
            out.push((entry.need("lap")?.as_int()?, items));
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn json_values() {
        let value = JsonParser::parse(br#"{"a": [1, -2.5, "x\u00e9\ud83d\ude00", null, true], "a": 3}"#)
            .unwrap();
        assert!(matches!(value.get("a"), Some(Json::Int(3))));
        assert!(JsonParser::parse(b"[01]").is_err());
        assert!(JsonParser::parse(b"\"\\ud800\"").is_err());
        assert!(JsonParser::parse(b"1e400").is_err());
        assert!(JsonParser::parse(b"{} x").is_err());
    }

    #[test]
    fn shuffled_buffers() {
        // two int16 values 0x0102 and 0x0304, byte planes [02 04][01 03]
        let body = [0x02u8, 0x04, 0x01, 0x03];
        let desc = JsonParser::parse(br#"["<i2", 0, 2]"#).unwrap();
        assert_eq!(buffer(&desc, &body).unwrap().to_i64().unwrap(), vec![0x0102, 0x0304]);
        let bits = JsonParser::parse(br#"["|u1", 0, 1]"#).unwrap();
        let mask = buffer(&bits, &[0b1010_0000]).unwrap().unpack(10).unwrap();
        assert_eq!(mask, [true, false, true, false, false, false, false, false, false, false]);
    }

    fn body_of(header: &str) -> Vec<u8> {
        let mut body = (header.len() as u32).to_le_bytes().to_vec();
        body.extend_from_slice(header.as_bytes());
        body.resize(body.len().next_multiple_of(8), 0);
        body
    }

    #[test]
    fn corrupt_layouts_are_declined() {
        let huge = r#"{"version":2,"laps":[{"lap":1,"keys":["t"],"arrays":{"t":["f8",1000000000000]}}],"columns":[]}"#;
        assert!(decode(&body_of(huge)).err().unwrap().contains("outside the body"));
        let twice = r#"{"version":2,"laps":[{"lap":1,"keys":["t"],"arrays":{"t":["f8",0],"t":["f8",0]}}],"columns":[]}"#;
        assert!(decode(&body_of(twice)).err().unwrap().contains("twice"));
        let empty = r#"{"version":2,"laps":[{"lap":3,"keys":["t"],"arrays":{"t":["f8",0]}}],"columns":[{"key":"t","kind":"f8","enc":"f64","buf":["<f8",0,0]}]}"#;
        let file = decode(&body_of(empty)).unwrap();
        let laps = file.laps().unwrap();
        assert_eq!(laps.len(), 1);
        assert_eq!(laps[0].0, 3);
        assert!(matches!(laps[0].1[0], ("t", Item::Floats(values)) if values.is_empty()));
    }

    /// Phase times over the file bodies in `$TIF1_C8_BODIES` (decompressed
    /// `tel.v2` frames): `cargo test --release -- --ignored --nocapture`.
    #[test]
    #[ignore]
    fn bench_bodies() {
        let dir = std::env::var("TIF1_C8_BODIES").expect("TIF1_C8_BODIES");
        let mut paths: Vec<_> = std::fs::read_dir(dir).unwrap().map(|e| e.unwrap().path()).collect();
        paths.sort();
        let bodies: Vec<Vec<u8>> = paths.iter().map(|p| std::fs::read(p).unwrap()).collect();
        let reps = 5u32;
        let t0 = std::time::Instant::now();
        for _ in 0..reps {
            for body in &bodies {
                let file = timed!("decode", decode(body).unwrap());
                timed!("laps", file.laps().unwrap());
            }
        }
        let total = t0.elapsed() / reps;
        timing::PHASES.with(|p| {
            let mut phases: Vec<_> = p.borrow().iter().map(|(k, v)| (k.clone(), *v / reps)).collect();
            phases.sort_by_key(|(_, v)| std::cmp::Reverse(*v));
            println!("{} bodies, {:?} per pass", bodies.len(), total);
            for (name, elapsed) in phases {
                println!("  {name:10} {elapsed:?}");
            }
        });
    }
}
