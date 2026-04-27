//! Monotonic client-order-id issuer. Lives here instead of being re-invented
//! in every strategy. Format is decimal-encoded u64 so adapters that require
//! a u64 `ClientOrderId` (Bullet) can parse directly, while adapters that
//! accept arbitrary strings (HL via `cloid`) can use the same value verbatim.

#[derive(Debug, Clone, Default)]
pub struct ClientIdIssuer {
    next: u64,
}

impl ClientIdIssuer {
    pub fn new() -> Self {
        Self { next: 1 }
    }

    /// Start the sequence past a known high-water mark. The next `issue()`
    /// returns `start.max(1)`. Useful for crash recovery: on restart,
    /// resume past the last client_id the venue might still have live so
    /// fresh orders don't collide with stale ones.
    pub fn starting_at(start: u64) -> Self {
        Self { next: start.max(1) }
    }

    /// Seed the sequence from the current Unix epoch so that IDs issued in
    /// different process restarts don't collide. Each second of wall time gives
    /// 10 000 unique IDs before wrapping into the next second's range.
    pub fn session_seeded() -> Self {
        let epoch_secs = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        Self::starting_at(epoch_secs * 10_000)
    }

    pub fn issue(&mut self) -> String {
        let id = self.next.max(1);
        self.next = id + 1;
        id.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn monotonic() {
        let mut iss = ClientIdIssuer::new();
        assert_eq!(iss.issue(), "1");
        assert_eq!(iss.issue(), "2");
        assert_eq!(iss.issue(), "3");
    }

    #[test]
    fn default_starts_at_one() {
        // `Default` gives a zero'd `next`; the first `issue()` bumps to 1
        // so callers don't need to remember to call `new`.
        let mut iss = ClientIdIssuer::default();
        assert_eq!(iss.issue(), "1");
    }

    #[test]
    fn starting_at_resumes_past_high_water_mark() {
        let mut iss = ClientIdIssuer::starting_at(1000);
        assert_eq!(iss.issue(), "1000");
        assert_eq!(iss.issue(), "1001");
    }
}
