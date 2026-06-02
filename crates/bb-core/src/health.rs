//! Shared connection-health state between an adapter's streaming task and its
//! REST broker.
//!
//! Adapters run a WebSocket muxer task that owns the live connection, while the
//! [`Broker`](crate::broker::Broker) handles request-response on a separate
//! path. `ConnectionHealth` is the small piece of state they share: the muxer
//! flips it, the broker reads it, and strategies observe it via
//! `Broker::take_reconcile_signal` / `Broker::is_disconnected`. The atomic
//! ordering is encapsulated here so adapters don't repeat it.

use std::sync::atomic::{AtomicBool, Ordering};

/// Cross-thread health flags shared between a muxer task and a `Broker`.
///
/// - `reconcile_pending` is raised on every reconnect so a strategy can resync open orders
///   immediately rather than wait for the next periodic sweep.
/// - `disconnected` is raised once the venue's reconnect loop gives up, so a strategy can request
///   shutdown.
///
/// Wrap in an `Arc` and share one instance between the muxer task and the
/// broker.
#[derive(Debug, Default)]
pub struct ConnectionHealth {
    reconcile_pending: AtomicBool,
    disconnected: AtomicBool,
}

impl ConnectionHealth {
    /// Raise the reconcile signal — call from the muxer on every reconnect.
    pub fn flag_reconcile(&self) {
        self.reconcile_pending.store(true, Ordering::Release);
    }

    /// Raise the permanent-disconnect flag — call from the muxer once the
    /// connection has given up for good.
    pub fn flag_disconnected(&self) {
        self.disconnected.store(true, Ordering::Release);
    }

    /// Take-and-clear the reconcile signal: returns `true` once per reconnect.
    pub fn take_reconcile_signal(&self) -> bool {
        self.reconcile_pending.swap(false, Ordering::AcqRel)
    }

    /// Whether the connection has permanently disconnected.
    pub fn is_disconnected(&self) -> bool {
        self.disconnected.load(Ordering::Acquire)
    }
}
