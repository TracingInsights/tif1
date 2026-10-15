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
//! Nested objects (the `tel` wrapper) recurse; scalar values (`dataKey`) are
//! returned as Python objects.

use numpy::IntoPyArray;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyBool, PyBytes, PyDict, PyList};

enum Channel {
    Ints(Vec<i64>),
    Floats(Vec<f64>),
    Strings(Vec<Option<String>>),
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
        let start = self.pos;
        loop {
            match self.peek() {
                None => return Err("unterminated string".into()),
                Some(b'"') => {
                    let raw = &self.bytes[start..self.pos];
                    self.pos += 1;
                    return std::str::from_utf8(raw)
                        .map(str::to_owned)
                        .map_err(|_| "invalid utf-8 in string".to_string());
                }
                Some(b'\\') => return self.parse_escaped_string(start),
                Some(_) => self.pos += 1,
            }
        }
    }

    fn parse_escaped_string(&mut self, start: usize) -> ParseResult<String> {
        let mut out: Vec<u8> = self.bytes[start..self.pos].to_vec();
        loop {
            match self.peek() {
                None => return Err("unterminated string".into()),
                Some(b'"') => {
                    self.pos += 1;
                    return String::from_utf8(out).map_err(|_| "invalid utf-8 in string".to_string());
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

    fn parse_array(&mut self, numeric_hint: bool) -> ParseResult<Channel> {
        // Opening bracket consumed. Elements are typed as we go; the
        // representation widens ints -> floats -> strings, never narrows.
        let mut ints: Vec<i64> = Vec::with_capacity(512);
        let mut floats: Vec<f64> = Vec::new();
        let mut strings: Vec<Option<String>> = Vec::new();
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
                    let text = self.parse_string()?;
                    self.null_marks.push(false);
                    if text == "None" {
                        match mode {
                            0 => {
                                floats = ints.iter().map(|&v| v as f64).collect();
                                ints = Vec::new();
                                floats.push(f64::NAN);
                                mode = 1;
                            }
                            1 => floats.push(f64::NAN),
                            _ => strings.push(Some(text)),
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
                            strings = Vec::with_capacity(count + 16);
                            for i in 0..count {
                                strings.push(if self.null_marks.get(i).copied().unwrap_or(false) {
                                    None
                                } else {
                                    Some("None".to_string())
                                });
                            }
                            ints = Vec::new();
                            floats = Vec::new();
                            mode = 2;
                        }
                        strings.push(Some(text));
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
                        _ => strings.push(None),
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
            let mut tokens: Vec<Option<String>> = Vec::with_capacity(count);
            for i in 0..count {
                tokens.push(if self.null_marks.get(i).copied().unwrap_or(false) {
                    None
                } else {
                    Some("None".to_string())
                });
            }
            return Ok(Channel::Strings(tokens));
        }
        Ok(match mode {
            0 => Channel::Ints(ints),
            1 => Channel::Floats(floats),
            _ => Channel::Strings(strings),
        })
    }

    fn parse_value(&mut self, numeric: &dyn Fn(&str) -> bool) -> ParseResult<Value> {
        self.skip_ws();
        match self.peek() {
            Some(b'{') => {
                self.pos += 1;
                self.parse_object(numeric)
            }
            Some(b'[') => {
                self.pos += 1;
                Ok(Value::Channel(self.parse_array(false)?))
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

    fn parse_object(&mut self, numeric: &dyn Fn(&str) -> bool) -> ParseResult<Value> {
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
                Value::Channel(self.parse_array(numeric(&key))?)
            } else {
                self.parse_value(numeric)?
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

    fn parse_document(&mut self, numeric: &dyn Fn(&str) -> bool) -> ParseResult<Value> {
        let value = self.parse_value(numeric)?;
        self.skip_ws();
        if self.pos != self.bytes.len() {
            return Err("trailing data".into());
        }
        Ok(value)
    }
}

fn to_py(py: Python<'_>, value: Value) -> PyResult<Py<PyAny>> {
    Ok(match value {
        Value::Object(items) => {
            let dict = PyDict::new(py);
            for (key, item) in items {
                dict.set_item(key, to_py(py, item)?)?;
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
        Value::Channel(Channel::Empty) => PyList::empty(py).into_any().unbind(),
        Value::Str(text) => text.into_pyobject(py)?.into_any().unbind(),
        Value::Int(v) => v.into_pyobject(py)?.into_any().unbind(),
        Value::Float(v) => v.into_pyobject(py)?.into_any().unbind(),
        Value::Bool(v) => PyBool::new(py, v).to_owned().into_any().unbind(),
        Value::Null => py.None(),
    })
}

/// Decode a telemetry payload into a dict whose list-valued channels are
/// numpy arrays (numeric) or Python lists (strings). Raises ``ValueError``
/// for anything the fast scanner does not handle; callers fall back to a
/// generic JSON parser.
#[pyfunction]
#[pyo3(signature = (payload, numeric_channels=None))]
fn decode_telemetry(
    py: Python<'_>,
    payload: &Bound<'_, PyBytes>,
    numeric_channels: Option<Vec<String>>,
) -> PyResult<Py<PyAny>> {
    let bytes = payload.as_bytes();
    let numeric_set: std::collections::HashSet<String> =
        numeric_channels.unwrap_or_default().into_iter().collect();
    let parsed = py
        .detach(|| {
            let is_numeric = |key: &str| numeric_set.contains(key);
            Parser::new(bytes).parse_document(&is_numeric)
        })
        .map_err(PyValueError::new_err)?;
    if !matches!(parsed, Value::Object(_)) {
        return Err(PyValueError::new_err("telemetry payload is not an object"));
    }
    to_py(py, parsed)
}

#[pymodule]
fn tif1_native(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(decode_telemetry, m)?)?;
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    Ok(())
}
