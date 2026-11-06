//! Predictions of the `tel.v2` columnar telemetry files (tif1's
//! `core_utils/columnar_telemetry.py`).
//!
//! A predicted channel is stored as the distance in units in the last place
//! between each sample and a prediction computed from channels decoded
//! before it. These functions compute the predictions the numpy decoder
//! computes, bit for bit: the same IEEE double operations in the same order
//! (`+ - * /`, `%` = C `fmod`, which is exact; no libm transcendental, no
//! fused multiply-add: Rust never contracts `a * b + c`). A NaN prediction
//! becomes 0.0, so NaN payloads, which differ between CPUs, never reach the
//! stored distance.

const MAGNITUDE: i64 = 0x7FFF_FFFF_FFFF_FFFF;

/// float64 -> int64 key in the order of the floats (an involution on the bits).
#[inline]
fn ordered(v: f64) -> i64 {
    let bits = v.to_bits() as i64;
    bits ^ ((bits >> 63) & MAGNITUDE)
}

#[inline]
fn from_ordered(key: i64) -> f64 {
    f64::from_bits((key ^ ((key >> 63) & MAGNITUDE)) as u64)
}

/// `predicted` moved by the zigzag-coded ULP distance `zigzag` (wrapping).
#[inline]
pub fn apply_residual(predicted: f64, zigzag: u64) -> f64 {
    if zigzag == 0 {
        return predicted;
    }
    let diff = ((zigzag >> 1) as i64) ^ -((zigzag & 1) as i64);
    from_ordered(ordered(predicted).wrapping_add(diff))
}

#[inline]
fn canonical(v: f64) -> f64 {
    if v.is_nan() {
        0.0
    } else {
        v
    }
}

/// `predicted[k]` moved by `zigzag[k]` ULPs, in place. Branch-free: a zero
/// distance maps the key back to the same bits (`ordered` is an involution).
pub fn apply(predicted: &mut [f64], zigzag: &[u64]) -> Result<(), &'static str> {
    if predicted.len() != zigzag.len() {
        return Err("residuals do not match their predictions");
    }
    for (p, &zz) in predicted.iter_mut().zip(zigzag) {
        let diff = ((zz >> 1) as i64) ^ -((zz & 1) as i64);
        *p = from_ordered(ordered(*p).wrapping_add(diff));
    }
    Ok(())
}

/// `interp`: `values[rows[k]]` = numpy.interp's formula between the anchors
/// `j0[k]`, `j1[k]` over `dates`, moved by `zigzag[k]` ULPs, in place.
pub fn interp(values: &mut [f64], dates: &[f64], rows: &[i64], j0: &[i64], j1: &[i64], zigzag: &[u64]) -> Result<(), &'static str> {
    let n = values.len();
    if dates.len() != n || j0.len() != rows.len() || j1.len() != rows.len() || zigzag.len() != rows.len() {
        return Err("interp inputs differ in length");
    }
    for k in 0..rows.len() {
        let (r, a, b) = (rows[k], j0[k], j1[k]);
        if r < 0 || a < 0 || b < 0 || r as usize >= n || a as usize >= n || b as usize >= n {
            return Err("interp row outside the column");
        }
        let (r, a, b) = (r as usize, a as usize, b as usize);
        let (x, x0, x1) = (dates[r], dates[a], dates[b]);
        let (y0, y1) = (values[a], values[b]);
        let slope = (y1 - y0) / (x1 - x0);
        let mut out = slope * (x - x0) + y0;
        if out.is_nan() {
            out = slope * (x - x1) + y1;
            if out.is_nan() && y0 == y1 {
                out = y0;
            }
        }
        if x0 == x {
            out = y0;
        }
        values[r] = apply_residual(canonical(out), zigzag[k]);
    }
    Ok(())
}

/// `distance` at the car rows: `values[prev] + increment`, then the residual,
/// row after row (each row reads the decoded previous anchor).
pub fn integ(values: &mut [f64], rows: &[i64], prev: &[i64], increments: &[f64], zigzag: &[u64]) -> Result<(), &'static str> {
    let n = values.len();
    if prev.len() != rows.len() || increments.len() != rows.len() || zigzag.len() != rows.len() {
        return Err("integ inputs differ in length");
    }
    for k in 0..rows.len() {
        let (row, before) = (rows[k], prev[k]);
        if row < 0 || before < 0 || row as usize >= n || before as usize >= n {
            return Err("integ row outside the column");
        }
        let predicted = canonical(values[before as usize] + increments[k]);
        values[row as usize] = apply_residual(predicted, zigzag[k]);
    }
    Ok(())
}

/// Unit-spacing `numpy.gradient` of `f` (at least two samples) into `out`.
fn gradient(f: &[f64], out: &mut [f64]) {
    let n = f.len();
    for i in 1..n - 1 {
        out[i] = (f[i + 1] - f[i - 1]) / 2.0;
    }
    out[0] = f[1] - f[0];
    out[n - 1] = f[n - 1] - f[n - 2];
}

fn lap_bounds(sizes: &[i64], total: usize) -> Result<Vec<(usize, usize)>, &'static str> {
    let mut bounds = Vec::with_capacity(sizes.len());
    let mut start = 0usize;
    for &size in sizes {
        if size < 0 {
            return Err("negative lap size");
        }
        let stop = start + size as usize;
        if stop > total {
            return Err("lap sizes exceed the column");
        }
        bounds.push((start, stop));
        start = stop;
    }
    if start != total {
        return Err("lap sizes do not cover the column");
    }
    Ok(bounds)
}

/// The data pipeline's longitudinal acceleration (`acc_x`) per lap.
pub fn accx(speed: &[f64], time: &[f64], sizes: &[i64]) -> Result<Vec<f64>, &'static str> {
    let total = speed.len();
    if time.len() != total {
        return Err("accx inputs differ in length");
    }
    let mut out = vec![0.0f64; total];
    let third = 1.0f64 / 3.0;
    for (start, stop) in lap_bounds(sizes, total)? {
        let n = stop - start;
        if n < 2 {
            continue;
        }
        let v: Vec<f64> = speed[start..stop].iter().map(|s| s / 3.6).collect();
        let mut gv = vec![0.0f64; n];
        let mut gt = vec![0.0f64; n];
        gradient(&v, &mut gv);
        gradient(&time[start..stop], &mut gt);
        let a: Vec<f64> = gv.iter().zip(&gt).map(|(x, y)| x / y).collect();
        let mut w = vec![0.0f64; n];
        for i in 0..n {
            let held = if i > 0 && i < n - 1 && a[i] > 25.0 { a[i - 1] } else { a[i] };
            w[i] = held * third;
        }
        let lap = &mut out[start..stop];
        if n == 2 {
            let s = w[0] + w[1];
            lap[0] = s;
            lap[1] = s;
            continue;
        }
        for i in 1..n - 1 {
            lap[i] = (w[i - 1] + w[i]) + w[i + 1];
        }
        lap[0] = w[0] + w[1];
        lap[n - 1] = w[n - 2] + w[n - 1];
    }
    for v in out.iter_mut() {
        *v = canonical(*v);
    }
    Ok(out)
}

// fdlibm s_atan.c
const ATAN_HI: [f64; 4] = [
    4.63647609000806093515e-01,
    7.85398163397448278999e-01,
    9.82793723247329054082e-01,
    1.57079632679489655800e+00,
];
const ATAN_LO: [f64; 4] = [
    2.26987774529616870924e-17,
    3.06161699786838301793e-17,
    1.39033110312309984516e-17,
    6.12323399573676603587e-17,
];
const AT: [f64; 11] = [
    3.33333333333329318027e-01,
    -1.99999999998764832476e-01,
    1.42857142725034663711e-01,
    -1.11111104054623557880e-01,
    9.09088713343650656196e-02,
    -7.69187620504482999495e-02,
    6.66107313738753120669e-02,
    -5.83357013379057348645e-02,
    4.97687799461593236017e-02,
    -3.65315727442169155270e-02,
    1.62858201153657823623e-02,
];
const PI: f64 = 3.14159265358979311600e+00;
const PI_LO: f64 = 1.22464679914735317720e-16;

/// `atan(t)` for `t >= 0` (fdlibm's reduction and polynomial; NaN stays NaN).
fn atan_abs(t: f64) -> f64 {
    let band = if t < 0.6875 {
        0
    } else if t < 1.1875 {
        1
    } else if t < 2.4375 {
        2
    } else {
        3
    };
    let x = if t < 0.4375 {
        t
    } else {
        match band {
            0 => (2.0 * t - 1.0) / (2.0 + t),
            1 => (t - 1.0) / (t + 1.0),
            2 => (t - 1.5) / (1.0 + 1.5 * t),
            _ => -1.0 / t,
        }
    };
    let z = x * x;
    let w = z * z;
    let s1 = z * (AT[0] + w * (AT[2] + w * (AT[4] + w * (AT[6] + w * (AT[8] + w * AT[10])))));
    let s2 = w * (AT[1] + w * (AT[3] + w * (AT[5] + w * (AT[7] + w * AT[9]))));
    let poly = x * (s1 + s2);
    if t < 0.4375 {
        x - poly
    } else {
        ATAN_HI[band] - ((poly - ATAN_LO[band]) - x)
    }
}

fn atan2(y: f64, x: f64) -> f64 {
    let z = atan_abs((y / x).abs());
    let neg_y = y.is_sign_negative();
    let left = x.is_sign_negative();
    if left && !neg_y {
        PI - (z - PI_LO)
    } else if left && neg_y {
        (z - PI_LO) - PI
    } else if neg_y {
        -z
    } else {
        z
    }
}

/// The data pipeline's lateral (`other` = y) or vertical (`other` = z)
/// acceleration per lap (see `_lateral_predict` in the Python codec).
pub fn lateral(speed: &[f64], x: &[f64], other: &[f64], distance: &[f64], sizes: &[i64]) -> Result<Vec<f64>, &'static str> {
    let total = speed.len();
    if x.len() != total || other.len() != total || distance.len() != total {
        return Err("lateral inputs differ in length");
    }
    let eps = f64::EPSILON;
    let two_pi = 2.0 * PI;
    let ninth = 1.0f64 / 9.0;
    let mut out = vec![0.0f64; total];
    for (start, stop) in lap_bounds(sizes, total)? {
        let n = stop - start;
        if n < 2 {
            continue;
        }
        let mut gx = vec![0.0f64; n];
        let mut go = vec![0.0f64; n];
        gradient(&x[start..stop], &mut gx);
        gradient(&other[start..stop], &mut go);
        let mut theta: Vec<f64> = (0..n).map(|i| atan2(go[i], gx[i] + eps)).collect();
        theta[0] = theta[1];
        // numpy.unwrap, period 2*pi
        let mut unwrapped = theta.clone();
        let mut correction_sum = 0.0f64;
        for i in 0..n - 1 {
            let dd = theta[i + 1] - theta[i];
            let shifted = dd + PI;
            // fmod(a, b) is `a` itself when |a| < |b| (exact): skip the libm call.
            let mut m = if shifted.abs() < two_pi { shifted } else { shifted % two_pi };
            if m < 0.0 {
                m += two_pi;
            }
            if m == 0.0 {
                m = 0.0;
            }
            let mut ddmod = m - PI;
            if ddmod == -PI && dd > 0.0 {
                ddmod = PI;
            }
            let correction = if dd.abs() < PI { 0.0 } else { ddmod - dd };
            correction_sum = if i == 0 { correction } else { correction_sum + correction };
            unwrapped[i + 1] = theta[i + 1] + correction_sum;
        }
        let mut g = vec![0.0f64; n];
        gradient(&unwrapped, &mut g);
        let mut gd = vec![0.0f64; n];
        gradient(&distance[start..stop], &mut gd);
        let mut w = vec![0.0f64; n];
        for i in 0..n {
            let dtheta = if i > 0 && i < n - 1 && g[i].abs() > 0.5 { g[i - 1] } else { g[i] };
            let v = speed[start + i] / 3.6;
            let curvature = dtheta / (gd[i] + 0.0001);
            let mut acc = (v * v) * curvature;
            if acc.abs() > 150.0 {
                acc = 0.0;
            }
            w[i] = acc * ninth;
        }
        let lap = &mut out[start..stop];
        for i in 0..n {
            let mut sum = 0.0f64;
            for offset in -4i64..=4 {
                let j = i as i64 + offset;
                if j >= 0 && (j as usize) < n {
                    sum = sum + w[j as usize];
                }
            }
            lap[i] = sum;
        }
    }
    for v in out.iter_mut() {
        *v = canonical(*v);
    }
    Ok(out)
}

/// The three nonzero quadratic B-splines at `x` in knot interval `l`
/// (scipy's `_deBoor_D` with k = 2).
fn de_boor2(t: &[f64], x: f64, l: usize) -> [f64; 3] {
    let mut h = [1.0f64, 0.0, 0.0];
    for j in 1..=2usize {
        let hh = h;
        h[0] = 0.0;
        for n in 1..=j {
            let xb = t[l + n];
            let xa = t[l + n - j];
            if xb == xa {
                h[n] = 0.0;
                continue;
            }
            let w = hh[n - 1] / (xb - xa);
            h[n - 1] = h[n - 1] + w * (xb - x);
            h[n] = w * (x - xa);
        }
    }
    h
}

/// Knot interval of `x` (`t[l] <= x < t[l + 1]`, clamped to `[2, n - 1]`),
/// `t` sorted. Walks right from `hint` when `t[hint] <= x` (ascending inputs:
/// one or two steps), else binary-searches; the same interval either way.
fn interval(t: &[f64], x: f64, n: usize, hint: usize) -> usize {
    let upper = if hint < t.len() && t[hint] <= x {
        let mut u = hint + 1;
        while u < t.len() && t[u] <= x {
            u += 1;
        }
        u
    } else {
        t.partition_point(|&k| k <= x)
    };
    (upper as i64 - 1).clamp(2, n as i64 - 1) as usize
}

/// The part of a lap's quadratic interpolating spline that depends on the
/// dates only (`xd` through the points, `xe` at the targets): the knots'
/// collocation matrix after elimination and each target's B-spline values.
/// Channels whose points share the dates (`x`, `y`, `z` of most laps) share
/// it; [`Basis::evaluate`] then runs the same operations as one pass would.
pub struct Basis {
    /// False: the lap predicts 0 (fewer than three points, a NaN or a
    /// non-increasing date, a collocation entry outside the three diagonals).
    usable: bool,
    sup: Vec<f64>,
    /// The diagonal after elimination.
    diag: Vec<f64>,
    /// `factor[i]` (i >= 1): the multiple of row `i - 1` taken from row `i`.
    factor: Vec<f64>,
    /// Per target: its knot interval and B-spline values (`l` = usize::MAX: a NaN target).
    targets: Vec<(usize, [f64; 3])>,
}

impl Basis {
    /// See `_spline_lap` in the Python codec: same knots, same operations.
    pub fn new(xd: &[f64], xe: &[f64]) -> Basis {
        let n = xd.len();
        let unusable = Basis {
            usable: false,
            sup: Vec::new(),
            diag: Vec::new(),
            factor: Vec::new(),
            targets: Vec::new(),
        };
        if n < 3 || xd.iter().any(|v| v.is_nan()) || xd.windows(2).any(|w| !(w[1] > w[0])) {
            return unusable;
        }
        let mut t = Vec::with_capacity(n + 3);
        t.extend_from_slice(&[xd[0]; 3]);
        for i in 1..n - 2 {
            t.push((xd[i + 1] + xd[i]) / 2.0);
        }
        t.extend_from_slice(&[xd[n - 1]; 3]);
        let (mut sub, mut diag, mut sup) = (vec![0.0f64; n], vec![0.0f64; n], vec![0.0f64; n]);
        let mut hint = usize::MAX;
        for i in 0..n {
            let l = interval(&t, xd[i], n, hint);
            hint = l;
            let h = de_boor2(&t, xd[i], l);
            let offset = l as i64 - 2 - i as i64;
            for (c, &value) in h.iter().enumerate() {
                match offset + c as i64 {
                    -1 => sub[i] = value,
                    0 => diag[i] = value,
                    1 => sup[i] = value,
                    _ => {
                        if value != 0.0 {
                            return unusable;
                        }
                    }
                }
            }
        }
        let mut factor = vec![0.0f64; n];
        for i in 1..n {
            factor[i] = sub[i] / diag[i - 1];
            diag[i] = diag[i] - factor[i] * sup[i - 1];
        }
        let mut targets = Vec::with_capacity(xe.len());
        let mut hint = usize::MAX;
        for &x in xe {
            if x.is_nan() {
                targets.push((usize::MAX, [0.0; 3]));
                continue;
            }
            let l = interval(&t, x, n, hint);
            hint = l;
            targets.push((l, de_boor2(&t, x, l)));
        }
        Basis { usable: true, sup, diag, factor, targets }
    }

    /// The spline through `(xd, yd)` at the targets, into `out` (one per target).
    pub fn evaluate(&self, yd: &[f64], out: &mut [f64]) {
        if !self.usable {
            out.fill(0.0);
            return;
        }
        let n = yd.len();
        let mut r = yd.to_vec();
        for i in 1..n {
            r[i] = r[i] - self.factor[i] * r[i - 1];
        }
        let mut coef = vec![0.0f64; n];
        coef[n - 1] = r[n - 1] / self.diag[n - 1];
        for i in (0..n - 1).rev() {
            coef[i] = (r[i] - self.sup[i] * coef[i + 1]) / self.diag[i];
        }
        for (slot, &(l, h)) in out.iter_mut().zip(&self.targets) {
            *slot = if l == usize::MAX {
                f64::NAN
            } else {
                ((0.0 + coef[l - 2] * h[0]) + coef[l - 1] * h[1]) + coef[l] * h[2]
            };
        }
    }
}

/// Quadratic interpolating spline through `(xd, yd)` evaluated at `xe`
/// (see `_spline_lap` in the Python codec: same knots, same operations).
fn spline_lap(xd: &[f64], yd: &[f64], xe: &[f64], out: &mut [f64]) {
    Basis::new(xd, xe).evaluate(yd, out);
}

/// Per lap, the spline through the rows flagged in `points` evaluated at the
/// rows `targets` (ascending): one prediction per target.
pub fn spline(dates: &[f64], values: &[f64], points: &[bool], targets: &[i64], sizes: &[i64]) -> Result<Vec<f64>, &'static str> {
    let total = dates.len();
    if values.len() != total || points.len() != total {
        return Err("spline inputs differ in length");
    }
    let mut out = vec![0.0f64; targets.len()];
    let mut next = 0usize;
    for (start, stop) in lap_bounds(sizes, total)? {
        let first = next;
        while next < targets.len() && targets[next] >= 0 && (targets[next] as usize) < stop {
            if (targets[next] as usize) < start {
                return Err("spline targets are not ascending");
            }
            next += 1;
        }
        if first == next {
            continue;
        }
        let rows: Vec<usize> = (start..stop).filter(|&r| points[r]).collect();
        let xd: Vec<f64> = rows.iter().map(|&r| dates[r]).collect();
        let yd: Vec<f64> = rows.iter().map(|&r| values[r]).collect();
        let xe: Vec<f64> = targets[first..next].iter().map(|&r| dates[r as usize]).collect();
        spline_lap(&xd, &yd, &xe, &mut out[first..next]);
    }
    if next != targets.len() {
        return Err("spline targets outside the laps");
    }
    for v in out.iter_mut() {
        *v = canonical(*v);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn residual_round_trip() {
        for (actual, predicted) in [(1.5f64, 1.25f64), (-0.0, 0.0), (f64::NAN, 3.0), (-2.0, 7.0)] {
            let d = ordered(actual).wrapping_sub(ordered(predicted));
            let zz = ((d << 1) ^ (d >> 63)) as u64;
            assert_eq!(apply_residual(predicted, zz).to_bits(), actual.to_bits());
        }
    }

    #[test]
    fn atan2_quadrants() {
        assert!((atan2(1.0, 1.0) - std::f64::consts::FRAC_PI_4).abs() < 1e-15);
        assert!((atan2(1.0, -1.0) - 3.0 * std::f64::consts::FRAC_PI_4).abs() < 1e-15);
        assert!((atan2(-1.0, -1.0) + 3.0 * std::f64::consts::FRAC_PI_4).abs() < 1e-15);
        assert!((atan2(-1.0, 1.0) + std::f64::consts::FRAC_PI_4).abs() < 1e-15);
    }
}
