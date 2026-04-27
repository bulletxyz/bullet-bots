//! `TickFeed` — framework-provided heartbeat feed. Produces `Tick` events on
//! a fixed interval so periodic strategy work (rebalance checks, logging,
//! staleness detection) flows through the same event model as everything else.

use std::time::{Duration, Instant};

use async_trait::async_trait;
use tokio::time::{MissedTickBehavior, interval};

use crate::error::BotError;
use crate::events::Tick;
use crate::harness::{EventFeed, EventTx, FeedContext};

pub struct TickFeed {
    period: Duration,
}

impl TickFeed {
    pub fn new(period: Duration) -> Self {
        Self { period }
    }

    /// `ms = 0` is silently coerced to 1 ms to keep `interval()` valid.
    pub fn every_ms(ms: u64) -> Self {
        Self::new(Duration::from_millis(ms.max(1)))
    }
}

#[async_trait]
impl EventFeed<Tick> for TickFeed {
    async fn run(
        self: Box<Self>,
        tx: EventTx<Tick>,
        cx: FeedContext,
    ) -> Result<(), BotError> {
        let mut ticker = interval(self.period);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                biased;
                _ = cx.cancelled() => return Ok(()),
                _ = ticker.tick() => {
                    // If nothing is subscribed yet we don't care — ticks are
                    // cheap and strategies may subscribe on a later cycle.
                    let _ = tx.send(Tick { at: Instant::now() });
                }
            }
        }
    }
}
