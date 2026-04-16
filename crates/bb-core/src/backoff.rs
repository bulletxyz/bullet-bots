use std::time::Duration;

/// Exponential backoff with jitter for reconnection logic.
#[derive(Debug)]
pub struct ExponentialBackoff {
    base: Duration,
    max: Duration,
    current: Duration,
}

impl ExponentialBackoff {
    pub fn new(base: Duration, max: Duration) -> Self {
        Self { base, max, current: base }
    }

    /// Get the next delay and advance the backoff state.
    /// Adds up to 30% random jitter to avoid thundering herd.
    pub fn next_delay(&mut self) -> Duration {
        let jitter = rand::random::<f64>() * 0.3;
        let delay = self.current.mul_f64(1.0 + jitter);
        self.current = (self.current * 2).min(self.max);
        delay
    }

    /// Reset backoff to the initial base delay.
    pub fn reset(&mut self) {
        self.current = self.base;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_increases() {
        let mut backoff =
            ExponentialBackoff::new(Duration::from_millis(100), Duration::from_secs(10));

        let d1 = backoff.next_delay();
        let d2 = backoff.next_delay();
        let d3 = backoff.next_delay();

        // Each delay should generally be larger (accounting for jitter)
        // Base: 100ms, then 200ms, then 400ms (before jitter)
        assert!(d1.as_millis() >= 100 && d1.as_millis() <= 130);
        assert!(d2.as_millis() >= 200 && d2.as_millis() <= 260);
        assert!(d3.as_millis() >= 400 && d3.as_millis() <= 520);
    }

    #[test]
    fn backoff_caps_at_max() {
        let mut backoff = ExponentialBackoff::new(Duration::from_secs(1), Duration::from_secs(5));

        // Advance well past the max
        for _ in 0..20 {
            let delay = backoff.next_delay();
            // Should never exceed max * 1.3 (max + jitter)
            assert!(delay <= Duration::from_millis(6500));
        }
    }

    #[test]
    fn backoff_resets() {
        let mut backoff =
            ExponentialBackoff::new(Duration::from_millis(100), Duration::from_secs(10));

        backoff.next_delay();
        backoff.next_delay();
        backoff.next_delay();
        backoff.reset();

        let d = backoff.next_delay();
        assert!(d.as_millis() >= 100 && d.as_millis() <= 130);
    }
}
