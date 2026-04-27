use std::time::Instant;

/// Time-weighted exponential moving average.
///
/// `tau_secs` is the time constant: a step input decays to `1/e` of its
/// terminal value after `tau_secs`. Larger `tau_secs` → smoother, slower.
#[derive(Debug, Clone)]
pub struct Ema {
    tau_secs: f64,
    value: Option<f64>,
    last_update: Option<Instant>,
}

impl Ema {
    pub fn new(tau_secs: f64) -> Self {
        Self { tau_secs, value: None, last_update: None }
    }

    pub fn update(&mut self, sample: f64, now: Instant) -> f64 {
        let next = match (self.value, self.last_update) {
            (Some(prev), Some(ts)) => {
                let dt = now.duration_since(ts).as_secs_f64();
                let alpha = 1.0 - (-dt / self.tau_secs).exp();
                prev + alpha * (sample - prev)
            }
            _ => sample,
        };
        self.value = Some(next);
        self.last_update = Some(now);
        next
    }

    pub fn value(&self) -> Option<f64> {
        self.value
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[allow(clippy::float_cmp)]
    #[test]
    fn seed_and_converge() {
        let t0 = Instant::now();
        let mut ema = Ema::new(60.0);
        assert_eq!(ema.update(100.0, t0), 100.0);

        // Step up to 200; after 1 tau (~60s) we're ~63% of the way.
        let v = ema.update(200.0, t0 + Duration::from_secs(60));
        assert!(v > 160.0 && v < 165.0, "expected ~163, got {v}");

        // After a long time relative to tau, should be very close to 200.
        let v = ema.update(200.0, t0 + Duration::from_secs(600));
        assert!((v - 200.0).abs() < 0.5, "expected ~200, got {v}");
    }

    #[test]
    fn fast_vs_slow_divergence() {
        // Fast EMA leads slow EMA on a trending input.
        let t0 = Instant::now();
        let mut fast = Ema::new(30.0);
        let mut slow = Ema::new(600.0);

        // Seed both at 100, then drift up by 1/sec for 60 seconds.
        fast.update(100.0, t0);
        slow.update(100.0, t0);
        for i in 1..=60 {
            let t = t0 + Duration::from_secs(i);
            fast.update(100.0 + i as f64, t);
            slow.update(100.0 + i as f64, t);
        }
        let f = fast.value().unwrap();
        let s = slow.value().unwrap();
        assert!(f > s, "fast ({f}) should lead slow ({s}) on uptrend");
    }
}
