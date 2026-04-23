use std::collections::VecDeque;
use std::time::{Duration, Instant};

/// Rolling-window volatility estimator. Stores `(t, ln(mid))` samples for the
/// last `window` duration and exposes the standard deviation of inter-sample
/// log returns as an estimate of per-second price volatility.
///
/// Units: the returned `sigma` is the stddev of log returns per sample step.
/// When paired with a `tau_secs` horizon in the A-S formula, the caller
/// should scale `sigma² * tau` consistently. The strategy code below treats
/// `sigma²` as "variance of fractional mid change over the recent past" and
/// `tau` as a pure tuning knob — we don't try to annualize.
#[derive(Debug, Clone)]
pub struct Volatility {
    window: Duration,
    samples: VecDeque<(Instant, f64)>,
}

impl Volatility {
    pub fn new(window_secs: u64) -> Self {
        Self { window: Duration::from_secs(window_secs.max(1)), samples: VecDeque::new() }
    }

    /// Push a fresh mid observation and evict anything older than the window.
    pub fn push(&mut self, mid: f64, now: Instant) {
        if mid <= 0.0 || !mid.is_finite() {
            return;
        }
        self.samples.push_back((now, mid.ln()));
        let cutoff = now.checked_sub(self.window).unwrap_or(now);
        while let Some((t, _)) = self.samples.front() {
            if *t < cutoff {
                self.samples.pop_front();
            } else {
                break;
            }
        }
    }

    /// Stddev of log returns across the buffered samples. Returns `None` until
    /// we have at least two samples.
    pub fn sigma(&self) -> Option<f64> {
        if self.samples.len() < 2 {
            return None;
        }
        let returns: Vec<f64> = self
            .samples
            .iter()
            .zip(self.samples.iter().skip(1))
            .map(|((_, a), (_, b))| b - a)
            .collect();
        let n = returns.len() as f64;
        let mean = returns.iter().sum::<f64>() / n;
        let var = returns.iter().map(|r| (r - mean).powi(2)).sum::<f64>() / n;
        Some(var.sqrt())
    }

    pub fn sample_count(&self) -> usize {
        self.samples.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn insufficient_samples() {
        let mut v = Volatility::new(60);
        assert_eq!(v.sigma(), None);
        v.push(100.0, Instant::now());
        assert_eq!(v.sigma(), None);
    }

    #[test]
    fn flat_price_zero_vol() {
        let mut v = Volatility::new(60);
        let t0 = Instant::now();
        for i in 0..20 {
            v.push(100.0, t0 + Duration::from_millis(i * 100));
        }
        let s = v.sigma().unwrap();
        assert!(s < 1e-9, "flat price should have ~0 vol, got {s}");
    }

    #[test]
    fn trending_price_positive_vol() {
        let mut v = Volatility::new(60);
        let t0 = Instant::now();
        for i in 0..20 {
            let mid = 100.0 + (i as f64) * 0.1;
            v.push(mid, t0 + Duration::from_millis(i * 100));
        }
        let s = v.sigma().unwrap();
        // Log returns on a linear ramp decrease slightly but have small
        // variance; just assert non-zero and finite.
        assert!(s > 0.0 && s.is_finite(), "expected positive finite vol, got {s}");
    }

    #[test]
    fn evicts_old_samples() {
        let mut v = Volatility::new(1); // 1-second window
        let t0 = Instant::now();
        for i in 0..5 {
            v.push(100.0 + i as f64, t0 + Duration::from_millis(i * 100));
        }
        assert_eq!(v.sample_count(), 5);
        // Jump well past the window — only the most recent sample should remain.
        v.push(200.0, t0 + Duration::from_secs(10));
        assert!(v.sample_count() <= 2, "expected eviction, got {}", v.sample_count());
    }
}
