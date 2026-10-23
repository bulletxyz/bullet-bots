//! Bounded set of recently-seen ids with FIFO eviction. Used by exchange
//! adapters to drop fills replayed across a WS reconnect — emitting a fill
//! twice would double-count the position — without growing memory unbounded.

use std::collections::{HashSet, VecDeque};

/// Bounded FIFO dedup set. `insert` returns `true` the first time an id is
/// seen and `false` for a repeat, evicting the oldest id once `cap` is
/// exceeded so memory stays bounded.
#[derive(Debug, Clone)]
pub struct RecentIds {
    set: HashSet<String>,
    order: VecDeque<String>,
    cap: usize,
}

impl RecentIds {
    /// `cap` is clamped to at least 1. A zero capacity would evict every id
    /// the instant it is inserted — silently disabling dedup and reintroducing
    /// the double-count it exists to prevent — so we coerce it, matching the
    /// `.max(1)` guard `TickFeed` and `Volatility` use for degenerate sizes.
    #[must_use]
    pub fn new(cap: usize) -> Self {
        Self { set: HashSet::new(), order: VecDeque::new(), cap: cap.max(1) }
    }

    /// Record `id`; returns `true` if it's new, `false` if already seen.
    pub fn insert(&mut self, id: &str) -> bool {
        if !self.set.insert(id.to_string()) {
            return false;
        }
        self.order.push_back(id.to_string());
        if self.order.len() > self.cap
            && let Some(evicted) = self.order.pop_front()
        {
            self.set.remove(&evicted);
        }
        true
    }
}

#[cfg(test)]
mod tests {
    use super::RecentIds;

    #[test]
    fn recent_ids_dedups_exact_repeats() {
        let mut seen = RecentIds::new(4);
        assert!(seen.insert("a"), "first sighting is new");
        assert!(!seen.insert("a"), "exact repeat is a duplicate");
        assert!(seen.insert("b"), "different id is new");
    }

    #[test]
    fn recent_ids_zero_cap_is_clamped_and_still_dedups() {
        // A zero capacity must not silently disable dedup — it's clamped to 1.
        let mut seen = RecentIds::new(0);
        assert!(seen.insert("a"), "first sighting is new");
        assert!(!seen.insert("a"), "immediate repeat is still caught");
    }

    #[test]
    fn recent_ids_evicts_oldest_past_cap() {
        let mut seen = RecentIds::new(2);
        seen.insert("a");
        seen.insert("b");
        seen.insert("c"); // evicts "a"
        assert!(seen.insert("a"), "evicted id is treated as new again");
        // memory stays bounded at the cap
        assert!(seen.set.len() <= 2);
    }
}
