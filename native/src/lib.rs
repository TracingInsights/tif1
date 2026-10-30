//! Optional native accelerators for tif1.
//!
//! `decode_telemetry(payload: bytes) -> dict` parses one `{driver}/{lap}_tel.json`
//! payload into typed numpy arrays. The JSON scan runs without the GIL, so the
//! fetch threads parse in parallel on multi-core hosts; only the final numpy
//! array construction (one memcpy per channel) needs the interpreter.
//!
//! Channel typing follows the pure-Python builder exactly:
//! - every element an integer token            -> int64 array
//! - every element a number or the "None"/null  -> float64 array (sentinels NaN)
//! - every element a sentinel, channel not in `numeric_channels`
//!                                              -> Python list of "None"/None
//! - any other string                           -> Python list of str (sentinels
//!                                                  kept as the literal "None"
//!                                                  so the Python token scan sees
//!                                                  what orjson would produce)
//! - anything else (nested arrays, booleans, ...) -> ValueError; the caller
//!   falls back to orjson for the whole payload.
//!
//! String channels named in `dict_channels` (``DriverAhead``: ~770 samples
//! per lap, a handful of distinct driver numbers) are dictionary-encoded
//! instead of materialized as one Python `str` per sample: the scan interns
//! each token into a category list and records an int32 code per sample
//! (`-1` for a JSON null; the literal `"None"` stays a category, so the
//! encoding is lossless). The interpreter then only builds the code array
//! (one memcpy) and the few category strings, and `dict_factory(codes,
//! categories)` wraps them (a `(codes, categories)` tuple without a factory).
//!
//! Nested objects (the `tel` wrapper) recurse; scalar values (`dataKey`) are
//! returned as Python objects.

use std::borrow::Cow;
use std::collections::{HashMap, HashSet};

use numpy::IntoPyArray;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyBool, PyBytes, PyDict, PyList, PyTuple};

enum Channel {
    Ints(Vec<i64>),
    Floats(Vec<f64>),
    Strings(Vec<Option<String>>),
    Dict { codes: Vec<i32>, categories: Vec<String> },
    Empty,
}

enum Value {
    Object(Vec<(String, Value)>),
    Channel(Channel),
    Str(String),
    Int(i64),
    Float(f64),
    Bool(bool),
    Null,
}

/// Categories looked up by linear scan up to this many; beyond it a hash index
/// keeps a high-cardinality channel linear in its length.
const DICT_LINEAR_MAX: usize = 32;

/// Collects the string samples of one channel: plain owned strings, or
/// dictionary codes into first-appearance categories.
enum StrSink {
    Plain(Vec<Option<String>>),
    Dict {
        codes: Vec<i32>,
        categories: Vec<String>,
        // Code of the previous sample: consecutive samples mostly repeat.
        last: i32,
        index: Option<HashMap<Vec<u8>, i32>>,
    },
}

impl StrSink {
    fn new(encode: bool, capacity: usize) -> Self {
        if encode {
            StrSink::Dict {
                codes: Vec::with_capacity(capacity),
                categories: Vec::new(),
                last: -1,
                index: None,
            }
        } else {
            StrSink::Plain(Vec::with_capacity(capacity))
        }
    }

    #[inline]
    fn push_null(&mut self) {
        match self {
            StrSink::Plain(values) => values.push(None),
            StrSink::Dict { codes, .. } => codes.push(-1),
        }
    }

    /// Push one string sample given as its unescaped bytes.
    fn push_bytes(&mut self, raw: &[u8]) -> ParseResult<()> {
        match self {
            StrSink::Plain(values) => {
                let text = std::str::from_utf8(raw).map_err(|_| "invalid utf-8 in string")?;
                values.push(Some(text.to_owned()));
            }
            StrSink::Dict {
                codes,
                categories,
                last,
                index,
            } => {
                if *last >= 0 && categories[*last as usize].as_bytes() == raw {
                    codes.push(*last);
                    return Ok(());
                }
                let found = match index {
                    Some(map) => map.get(raw).copied(),
                    None => categories
                        .iter()
                        .position(|c| c.as_bytes() == raw)
                        .map(|i| i as i32),
                };
                let code = match found {
                    Some(code) => code,
                    None => {
                        // Equal bytes are equally valid: UTF-8 is checked once
                        // per category, not per sample.
                        let text =
                            std::str::from_utf8(raw).map_err(|_| "invalid utf-8 in string")?;
                        let code = i32::try_from(categories.len())
                            .map_err(|_| "too many categories".to_string())?;
                        categories.push(text.to_owned());
                        if let Some(map) = index {
                            map.insert(raw.to_vec(), code);
                        } else if categories.len() > DICT_LINEAR_MAX {
                            *index = Some(
                                categories
                                    .iter()
                                    .enumerate()
                                    .map(|(i, c)| (c.as_bytes().to_vec(), i as i32))
                                    .collect(),
                            );
                        }
                        code
                    }
                };
                *last = code;
                codes.push(code);
            }
        }
        Ok(())
    }

    fn into_channel(self) -> Channel {
        match self {
            StrSink::Plain(values) => Channel::Strings(values),
            StrSink::Dict {
                codes, categories, ..
            } => Channel::Dict { codes, categories },
        }
    }
}

struct Parser<'a> {
    bytes: &'a [u8],
    pos: usize,
    // Per-element flag for the array being parsed: true when the sentinel
    // was a JSON null rather than the "None" string.
    null_marks: Vec<bool>,
}

type ParseResult<T> = Result<T, String>;

impl<'a> Parser<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Parser {
            bytes,
            pos: 0,
            null_marks: Vec::new(),
        }
    }

    #[inline]
    fn skip_ws(&mut self) {
        while self.pos < self.bytes.len() && matches!(self.bytes[self.pos], b' ' | b'\n' | b'\r' | b'\t')
        {
            self.pos += 1;
        }
    }

    #[inline]
    fn peek(&self) -> Option<u8> {
        self.bytes.get(self.pos).copied()
    }

    fn expect(&mut self, byte: u8) -> ParseResult<()> {
        self.skip_ws();
        if self.peek() == Some(byte) {
            self.pos += 1;
            Ok(())
        } else {
            Err(format!("expected '{}' at {}", byte as char, self.pos))
        }
    }

    fn parse_string(&mut self) -> ParseResult<String> {
        // Called with the opening quote already consumed.
        match self.parse_string_bytes()? {
            Cow::Borrowed(raw) => std::str::from_utf8(raw)
                .map(str::to_owned)
                .map_err(|_| "invalid utf-8 in string".to_string()),
            Cow::Owned(out) => {
                String::from_utf8(out).map_err(|_| "invalid utf-8 in string".to_string())
            }
        }
    }

    /// The unescaped bytes of a string (opening quote consumed); borrowed from
    /// the payload unless the string has escapes. UTF-8 is not validated here.
    fn parse_string_bytes(&mut self) -> ParseResult<Cow<'a, [u8]>> {
        let start = self.pos;
        loop {
            match self.peek() {
                None => return Err("unterminated string".into()),
                Some(b'"') => {
                    let raw = &self.bytes[start..self.pos];
                    self.pos += 1;
                    return Ok(Cow::Borrowed(raw));
                }
                Some(b'\\') => return self.parse_escaped_string(start).map(Cow::Owned),
                Some(_) => self.pos += 1,
            }
        }
    }

    fn parse_escaped_string(&mut self, start: usize) -> ParseResult<Vec<u8>> {
        let mut out: Vec<u8> = self.bytes[start..self.pos].to_vec();
        loop {
            match self.peek() {
                None => return Err("unterminated string".into()),
                Some(b'"') => {
                    self.pos += 1;
                    return Ok(out);
                }
                Some(b'\\') => {
                    self.pos += 1;
                    let esc = self.peek().ok_or("bad escape")?;
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
                            let hex = self
                                .bytes
                                .get(self.pos..self.pos + 4)
                                .ok_or("bad unicode escape")?;
                            let code = u32::from_str_radix(
                                std::str::from_utf8(hex).map_err(|_| "bad unicode escape")?,
                                16,
                            )
                            .map_err(|_| "bad unicode escape")?;
                            self.pos += 4;
                            let ch = char::from_u32(code).ok_or("surrogate escapes unsupported")?;
                            let mut buf = [0u8; 4];
                            out.extend_from_slice(ch.encode_utf8(&mut buf).as_bytes());
                        }
                        _ => return Err("bad escape".into()),
                    }
                }
                Some(b) => {
                    out.push(b);
                    self.pos += 1;
                }
            }
        }
    }

    /// Scan a number token; returns (is_integer, token slice).
    fn number_token(&mut self) -> ParseResult<(bool, &'a [u8])> {
        let start = self.pos;
        let mut is_int = true;
        if self.peek() == Some(b'-') {
            self.pos += 1;
        }
        while let Some(b) = self.peek() {
            match b {
                b'0'..=b'9' => self.pos += 1,
                b'.' | b'e' | b'E' | b'+' | b'-' => {
                    is_int = false;
                    self.pos += 1;
                }
                _ => break,
            }
        }
        if self.pos == start || (self.pos == start + 1 && self.bytes[start] == b'-') {
            return Err(format!("bad number at {}", start));
        }
        Ok((is_int, &self.bytes[start..self.pos]))
    }

    fn parse_f64(token: &[u8]) -> ParseResult<f64> {
        fast_float2::parse::<f64, _>(token).map_err(|_| "bad float".to_string())
    }

    fn parse_i64(token: &[u8]) -> ParseResult<i64> {
        let text = std::str::from_utf8(token).map_err(|_| "bad int")?;
        text.parse::<i64>().map_err(|_| "int out of range".to_string())
    }

    fn parse_literal(&mut self, lit: &[u8]) -> bool {
        if self.bytes[self.pos..].starts_with(lit) {
            self.pos += lit.len();
            true
        } else {
            false
        }
    }

    /// The sentinel-only prefix (first `count` samples) as string samples:
    /// `"None"` tokens stay the literal, JSON nulls become None.
    fn sentinel_strings(&self, count: usize, encode: bool, capacity: usize) -> ParseResult<StrSink> {
        let mut sink = StrSink::new(encode, capacity);
        for i in 0..count {
            if self.null_marks.get(i).copied().unwrap_or(false) {
                sink.push_null();
            } else {
                sink.push_bytes(b"None")?;
            }
        }
        Ok(sink)
    }

    fn parse_array(&mut self, numeric_hint: bool, encode: bool) -> ParseResult<Channel> {
        // Opening bracket consumed. Elements are typed as we go; the
        // representation widens ints -> floats -> strings, never narrows.
        let mut ints: Vec<i64> = Vec::with_capacity(512);
        let mut floats: Vec<f64> = Vec::new();
        let mut strings = StrSink::new(false, 0);
        // 0 = ints, 1 = floats, 2 = strings
        let mut mode = 0u8;
        let mut count = 0usize;
        // Whether a genuine number has been seen: a run of "None"/null
        // tokens followed by a real string is a string channel whose first
        // samples are the sentinel (DriverAhead at the start of a lap).
        let mut numeric_seen = false;
        self.null_marks.clear();
        self.skip_ws();
        if self.peek() == Some(b']') {
            self.pos += 1;
            return Ok(Channel::Empty);
        }
        loop {
            self.skip_ws();
            match self.peek() {
                Some(b'"') => {
                    self.pos += 1;
                    let raw = self.parse_string_bytes()?;
                    self.null_marks.push(false);
                    if *raw == *b"None" {
                        match mode {
                            0 => {
                                floats = ints.iter().map(|&v| v as f64).collect();
                                ints = Vec::new();
                                floats.push(f64::NAN);
                                mode = 1;
                            }
                            1 => floats.push(f64::NAN),
                            _ => strings.push_bytes(&raw)?,
                        }
                    } else {
                        if mode != 2 {
                            if numeric_seen {
                                // Numbers followed by a real string: the
                                // Python builder decides; not a fast path.
                                return Err("mixed numeric/string channel".into());
                            }
                            // Only sentinels so far: they were "None" tokens
                            // (null is reported as None).
                            strings = self.sentinel_strings(count, encode, count + 512)?;
                            ints = Vec::new();
                            floats = Vec::new();
                            mode = 2;
                        }
                        strings.push_bytes(&raw)?;
                    }
                }
                Some(b'n') => {
                    if !self.parse_literal(b"null") {
                        return Err("bad literal".into());
                    }
                    self.null_marks.push(true);
                    match mode {
                        0 => {
                            floats = ints.iter().map(|&v| v as f64).collect();
                            ints = Vec::new();
                            floats.push(f64::NAN);
                            mode = 1;
                        }
                        1 => floats.push(f64::NAN),
                        _ => strings.push_null(),
                    }
                }
                Some(b'-') | Some(b'0'..=b'9') => {
                    let (is_int, token) = self.number_token()?;
                    numeric_seen = true;
                    self.null_marks.push(false);
                    match mode {
                        0 if is_int => match Self::parse_i64(token) {
                            Ok(v) => ints.push(v),
                            Err(_) => {
                                floats = ints.iter().map(|&v| v as f64).collect();
                                ints = Vec::new();
                                floats.push(Self::parse_f64(token)?);
                                mode = 1;
                            }
                        },
                        0 => {
                            floats = ints.iter().map(|&v| v as f64).collect();
                            ints = Vec::new();
                            floats.push(Self::parse_f64(token)?);
                            mode = 1;
                        }
                        1 => floats.push(Self::parse_f64(token)?),
                        _ => return Err("mixed numeric/string channel".into()),
                    }
                }
                Some(b'[') | Some(b'{') | Some(b't') | Some(b'f') => {
                    return Err("unsupported array element".into())
                }
                _ => return Err(format!("bad array element at {}", self.pos)),
            }
            count += 1;
            self.skip_ws();
            match self.peek() {
                Some(b',') => self.pos += 1,
                Some(b']') => {
                    self.pos += 1;
                    break;
                }
                _ => return Err(format!("expected ',' or ']' at {}", self.pos)),
            }
        }
        if !numeric_seen && mode == 1 && !numeric_hint {
            // Every sample was a sentinel in a channel that is not numeric by
            // contract (DriverAhead on a lap with nobody ahead): hand the
            // literal tokens to Python exactly as a generic parser would.
            return Ok(self.sentinel_strings(count, encode, count)?.into_channel());
        }
        Ok(match mode {
            0 => Channel::Ints(ints),
            1 => Channel::Floats(floats),
            _ => strings.into_channel(),
        })
    }

    fn parse_value(&mut self, keys: &ChannelKeys) -> ParseResult<Value> {
        self.skip_ws();
        match self.peek() {
            Some(b'{') => {
                self.pos += 1;
                self.parse_object(keys)
            }
            Some(b'[') => {
                self.pos += 1;
                Ok(Value::Channel(self.parse_array(false, false)?))
            }
            Some(b'"') => {
                self.pos += 1;
                Ok(Value::Str(self.parse_string()?))
            }
            Some(b't') => {
                if self.parse_literal(b"true") {
                    Ok(Value::Bool(true))
                } else {
                    Err("bad literal".into())
                }
            }
            Some(b'f') => {
                if self.parse_literal(b"false") {
                    Ok(Value::Bool(false))
                } else {
                    Err("bad literal".into())
                }
            }
            Some(b'n') => {
                if self.parse_literal(b"null") {
                    Ok(Value::Null)
                } else {
                    Err("bad literal".into())
                }
            }
            Some(b'-') | Some(b'0'..=b'9') => {
                let (is_int, token) = self.number_token()?;
                if is_int {
                    match Self::parse_i64(token) {
                        Ok(v) => Ok(Value::Int(v)),
                        Err(_) => Ok(Value::Float(Self::parse_f64(token)?)),
                    }
                } else {
                    Ok(Value::Float(Self::parse_f64(token)?))
                }
            }
            _ => Err(format!("unexpected byte at {}", self.pos)),
        }
    }

    fn parse_object(&mut self, keys: &ChannelKeys) -> ParseResult<Value> {
        // Opening brace consumed.
        let mut items: Vec<(String, Value)> = Vec::with_capacity(20);
        self.skip_ws();
        if self.peek() == Some(b'}') {
            self.pos += 1;
            return Ok(Value::Object(items));
        }
        loop {
            self.expect(b'"')?;
            let key = self.parse_string()?;
            self.expect(b':')?;
            self.skip_ws();
            let value = if self.peek() == Some(b'[') {
                self.pos += 1;
                Value::Channel(
                    self.parse_array(keys.numeric.contains(&key), keys.dict.contains(&key))?,
                )
            } else {
                self.parse_value(keys)?
            };
            items.push((key, value));
            self.skip_ws();
            match self.peek() {
                Some(b',') => self.pos += 1,
                Some(b'}') => {
                    self.pos += 1;
                    break;
                }
                _ => return Err(format!("expected ',' or '}}' at {}", self.pos)),
            }
        }
        Ok(Value::Object(items))
    }

    fn parse_document(&mut self, keys: &ChannelKeys) -> ParseResult<Value> {
        let value = self.parse_value(keys)?;
        self.skip_ws();
        if self.pos != self.bytes.len() {
            return Err("trailing data".into());
        }
        Ok(value)
    }
}

/// Channel names with a contract: numeric by contract (an all-sentinel
/// channel is a NaN array) and dictionary-encoded string channels.
struct ChannelKeys {
    numeric: HashSet<String>,
    dict: HashSet<String>,
}

fn to_py(py: Python<'_>, value: Value, dict_factory: Option<&Bound<'_, PyAny>>) -> PyResult<Py<PyAny>> {
    Ok(match value {
        Value::Object(items) => {
            let dict = PyDict::new(py);
            for (key, item) in items {
                dict.set_item(key, to_py(py, item, dict_factory)?)?;
            }
            dict.into_any().unbind()
        }
        Value::Channel(Channel::Ints(v)) => v.into_pyarray(py).into_any().unbind(),
        Value::Channel(Channel::Floats(v)) => v.into_pyarray(py).into_any().unbind(),
        Value::Channel(Channel::Strings(v)) => {
            let list = PyList::empty(py);
            for item in v {
                match item {
                    Some(text) => list.append(text)?,
                    None => list.append(py.None())?,
                }
            }
            list.into_any().unbind()
        }
        Value::Channel(Channel::Dict { codes, categories }) => {
            let codes = codes.into_pyarray(py).into_any();
            let categories = PyList::new(py, categories)?.into_any();
            match dict_factory {
                Some(factory) => factory.call1((codes, categories))?.unbind(),
                None => PyTuple::new(py, [codes, categories])?.into_any().unbind(),
            }
        }
        Value::Channel(Channel::Empty) => PyList::empty(py).into_any().unbind(),
        Value::Str(text) => text.into_pyobject(py)?.into_any().unbind(),
        Value::Int(v) => v.into_pyobject(py)?.into_any().unbind(),
        Value::Float(v) => v.into_pyobject(py)?.into_any().unbind(),
        Value::Bool(v) => PyBool::new(py, v).to_owned().into_any().unbind(),
        Value::Null => py.None(),
    })
}

/// Decode a telemetry payload into a dict whose list-valued channels are
/// numpy arrays (numeric) or Python lists (strings). String channels named in
/// ``dict_channels`` come back dictionary-encoded as
/// ``dict_factory(codes, categories)`` (int32 codes, ``-1`` for a JSON null,
/// into a first-appearance list of ``str``), or as the ``(codes, categories)``
/// tuple without a factory. Raises ``ValueError`` for anything the fast
/// scanner does not handle; callers fall back to a generic JSON parser.
#[pyfunction]
#[pyo3(signature = (payload, numeric_channels=None, dict_channels=None, dict_factory=None))]
fn decode_telemetry(
    py: Python<'_>,
    payload: &Bound<'_, PyBytes>,
    numeric_channels: Option<Vec<String>>,
    dict_channels: Option<Vec<String>>,
    dict_factory: Option<&Bound<'_, PyAny>>,
) -> PyResult<Py<PyAny>> {
    let bytes = payload.as_bytes();
    let keys = ChannelKeys {
        numeric: numeric_channels.unwrap_or_default().into_iter().collect(),
        dict: dict_channels.unwrap_or_default().into_iter().collect(),
    };
    let parsed = py
        .detach(|| Parser::new(bytes).parse_document(&keys))
        .map_err(PyValueError::new_err)?;
    if !matches!(parsed, Value::Object(_)) {
        return Err(PyValueError::new_err("telemetry payload is not an object"));
    }
    to_py(py, parsed, dict_factory)
}

// The decoder keeps no state between calls, so free-threaded interpreters
// (3.13t+) can run it without re-enabling the GIL on import.
#[pymodule(gil_used = false)]
fn tif1_native(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(decode_telemetry, m)?)?;
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    Ok(())
}
