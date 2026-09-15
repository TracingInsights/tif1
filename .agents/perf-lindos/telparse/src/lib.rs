use numpy::IntoPyArray;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyDict;

struct Scanner<'a> {
    b: &'a [u8],
    i: usize,
}

impl<'a> Scanner<'a> {
    fn ws(&mut self) {
        while self.i < self.b.len() && matches!(self.b[self.i], b' ' | b'\t' | b'\n' | b'\r') {
            self.i += 1;
        }
    }
    fn peek(&mut self) -> Option<u8> {
        self.ws();
        self.b.get(self.i).copied()
    }
    fn expect(&mut self, c: u8) -> PyResult<()> {
        match self.peek() {
            Some(x) if x == c => {
                self.i += 1;
                Ok(())
            }
            _ => Err(PyValueError::new_err(format!(
                "expected '{}' at {}",
                c as char, self.i
            ))),
        }
    }
    fn parse_str(&mut self) -> PyResult<String> {
        self.expect(b'"')?;
        let start = self.i;
        while self.i < self.b.len() && self.b[self.i] != b'"' {
            if self.b[self.i] == b'\\' {
                self.i += 1;
            }
            self.i += 1;
        }
        if self.i >= self.b.len() {
            return Err(PyValueError::new_err("unterminated string"));
        }
        let raw = &self.b[start..self.i];
        self.i += 1;
        if !raw.is_ascii() {
            return Err(PyValueError::new_err("non-ascii key"));
        }
        Ok(String::from_utf8_lossy(raw).into_owned())
    }
    /// Scan a JSON number starting at self.i; returns (is_float, end).
    fn scan_number(&mut self) -> (bool, usize) {
        let start = self.i;
        let mut is_float = false;
        if self.i < self.b.len() && (self.b[self.i] == b'-' || self.b[self.i] == b'+') {
            self.i += 1;
        }
        while self.i < self.b.len() {
            match self.b[self.i] {
                b'0'..=b'9' => self.i += 1,
                b'.' | b'e' | b'E' => {
                    let was_exp = matches!(self.b[self.i], b'e' | b'E');
                    is_float = true;
                    self.i += 1;
                    if was_exp && matches!(self.b.get(self.i), Some(b'+') | Some(b'-')) {
                        self.i += 1;
                    }
                }
                _ => break,
            }
        }
        (is_float, start)
    }
}

#[pyfunction]
fn parse_columns(py: Python, data: &[u8]) -> PyResult<Py<PyDict>> {
    let mut sc = Scanner { b: data, i: 0 };
    let dict = PyDict::new(py);
    sc.expect(b'{')?;
    loop {
        match sc.peek() {
            Some(b'}') => {
                sc.i += 1;
                break;
            }
            Some(b'"') => {}
            _ => return Err(PyValueError::new_err("expected key")),
        }
        let key = sc.parse_str()?;
        sc.expect(b':')?;
        match sc.peek() {
            Some(b'[') => {
                sc.i += 1;
                let mut ints: Vec<i64> = Vec::new();
                let mut floats: Vec<f64> = Vec::new();
                let mut strings: Vec<String> = Vec::new();
                let mut is_float = false;
                let mut has_null = false;
                let mut has_string = false;
                let mut n = 0usize;
                loop {
                    match sc.peek() {
                        Some(b']') => {
                            sc.i += 1;
                            break;
                        }
                        Some(b'n') => {
                            // null
                            if data.len() >= sc.i + 4 && &data[sc.i..sc.i + 4] == b"null" {
                                sc.i += 4;
                                has_null = true;
                                if is_float {
                                    floats.push(f64::NAN);
                                } else {
                                    ints.push(0);
                                }
                                n += 1;
                            } else {
                                return Err(PyValueError::new_err("bad token"));
                            }
                        }
                        Some(b'"') => {
                            // String token: preserve exactly (parity with the
                            // orjson+pandas path, which infers str columns).
                            let tok = sc.parse_str()?;
                            strings.push(tok);
                            has_string = true;
                        }
                        Some(c) if c == b'-' || c.is_ascii_digit() => {
                            let (flt, start) = sc.scan_number();
                            let s = std::str::from_utf8(&data[start..sc.i])
                                .map_err(|_| PyValueError::new_err("utf8"))?;
                            if flt {
                                if !is_float {
                                    // switch to float mode, replay ints
                                    is_float = true;
                                    let mut f = Vec::with_capacity(n + 8);
                                    for v in &ints {
                                        f.push(*v as f64);
                                    }
                                    floats = f;
                                    ints = Vec::new();
                                }
                                floats.push(s.parse::<f64>().map_err(|_| {
                                    PyValueError::new_err(format!("float parse at {}", start))
                                })?);
                            } else if is_float {
                                floats.push(s.parse::<f64>().map_err(|_| {
                                    PyValueError::new_err(format!("float parse at {}", start))
                                })?);
                            } else {
                                ints.push(s.parse::<i64>().map_err(|_| {
                                    PyValueError::new_err(format!("int parse at {}", start))
                                })?);
                            }
                            n += 1;
                        }
                        _ => return Err(PyValueError::new_err("unexpected token in array")),
                    }
                    match sc.peek() {
                        Some(b',') => {
                            sc.i += 1;
                        }
                        Some(b']') => {}
                        _ => return Err(PyValueError::new_err("expected , or ]")),
                    }
                }
                let _ = has_null;
                if has_string {
                    // Mixed string/numeric channels: hand back the raw list so
                    // pandas infers exactly what the orjson path infers.
                    let list = pyo3::types::PyList::new(py, strings.iter().map(|v| v.as_str()))?;
                    dict.set_item(&key, list)?;
                } else if is_float {
                    let arr = floats.into_pyarray(py);
                    dict.set_item(&key, arr)?;
                } else if !ints.is_empty() {
                    let arr = ints.into_pyarray(py);
                    dict.set_item(&key, arr)?;
                }
            }
            Some(b'"') => {
                // scalar string (e.g. dataKey): skipped, non-list channels are
                // dropped by the DataFrame construction too
                let _ = sc.parse_str()?;
            }
            Some(b'n') => {
                if data.len() >= sc.i + 4 && &data[sc.i..sc.i + 4] == b"null" {
                    sc.i += 4;
                } else {
                    return Err(PyValueError::new_err("bad token"));
                }
            }
            Some(c) if c == b'-' || c.is_ascii_digit() => {
                let _ = sc.scan_number();
            }
            _ => return Err(PyValueError::new_err("expected array value")),
        }
        match sc.peek() {
            Some(b',') => {
                sc.i += 1;
            }
            Some(b'}') => {}
            _ => return Err(PyValueError::new_err("expected , or }")),
        }
    }
    Ok(dict.unbind())
}

#[pymodule]
fn telparse(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(parse_columns, m)?)?;
    Ok(())
}
