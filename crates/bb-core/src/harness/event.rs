//! Marker trait for events flowing through the harness.
//!
//! An `Event` is `Clone` because the harness fans out each published value to
//! all subscribers via `tokio::sync::broadcast`. It is `Send + 'static` so
//! values can cross thread boundaries and live as long as the receiving task
//! needs them.

use std::fmt::Debug;

pub trait Event: Clone + Debug + Send + 'static {}

impl<T> Event for T where T: Clone + Debug + Send + 'static {}
