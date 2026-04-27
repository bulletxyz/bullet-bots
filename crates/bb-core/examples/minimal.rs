//! Minimal harness example — shows the full wire-up without exchange adapters.
//!
//! Run with:
//!   cargo run --example minimal -p bb-core

use std::sync::Arc;

use async_trait::async_trait;
use bb_core::error::BotError;
use bb_core::events::Tick;
use bb_core::harness::{Actor, ActorContext, ActorSpec, EventHandler, HarnessBuilder, WindDownReason};
use bb_core::helpers::TickFeed;

struct Counter {
    count: u32,
    limit: u32,
}

#[async_trait]
impl Actor for Counter {
    async fn init(&mut self, _cx: &ActorContext) -> Result<(), BotError> {
        tracing::info!(limit = self.limit, "Counter started");
        Ok(())
    }

    async fn wind_down(&mut self, reason: &WindDownReason, _cx: &ActorContext) -> Result<(), BotError> {
        tracing::info!(count = self.count, ?reason, "Counter stopped");
        Ok(())
    }
}

#[async_trait]
impl EventHandler<Tick> for Counter {
    async fn on_event(&mut self, _event: Tick, cx: &ActorContext) -> Result<(), BotError> {
        self.count += 1;
        tracing::info!(count = self.count, "tick");
        if self.count >= self.limit {
            cx.request_shutdown();
        }
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), BotError> {
    tracing_subscriber::fmt().init();

    let harness = HarnessBuilder::new()
        .wire_feed_named("ticks", TickFeed::every_ms(100))
        .wire_actor(
            ActorSpec::new("counter", Counter { count: 0, limit: 5 })
                .sub::<Tick>(),
        )
        .build()?;

    let reason = harness.run().await?;
    tracing::info!(?reason, "harness exited");
    Ok(())
}
