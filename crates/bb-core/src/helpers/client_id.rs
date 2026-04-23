//! Monotonic client-order-id issuer. Lives here instead of being re-invented
//! in every strategy. Format is decimal-encoded u64 so adapters that require
//! a u64 `ClientOrderId` (Bullet) can parse directly, while adapters that
//! accept arbitrary strings (HL via `cloid`) can use the same value verbatim.

#[derive(Debug, Clone)]
pub struct ClientIdIssuer {
    next: u64,
}

impl ClientIdIssuer {
    pub fn new() -> Self {
        Self { next: 1 }
    }

    /// Start the sequence at an arbitrary value. Useful when a strategy wants
    /// per-session uniqueness (e.g., seeded from process start time).
    pub fn starting_at(start: u64) -> Self {
        Self { next: start.max(1) }
    }

    pub fn issue(&mut self) -> String {
        let id = self.next;
        self.next += 1;
        id.to_string()
    }

    pub fn peek(&self) -> u64 {
        self.next
    }
}

impl Default for ClientIdIssuer {
    fn default() -> Self {
        Self::new()
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
    fn starting_at_zero_coerces_to_one() {
        let mut iss = ClientIdIssuer::starting_at(0);
        assert_eq!(iss.issue(), "1");
    }
}
