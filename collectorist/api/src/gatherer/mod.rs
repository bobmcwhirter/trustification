use std::collections::HashMap;
use chrono::Duration;
use serde::{Deserialize, Serialize};
use tokio::sync::oneshot::{channel, Receiver, Sender};
use crate::server::collect::CollectRequest;
use crate::SharedState;
use crate::state::AppState;

pub mod collectors;
pub mod collector;
pub mod gatherer;


#[derive(Serialize, Deserialize)]
pub enum RateLimit {
    Unlimited,
    PerSecond(u32),
    PerMinute(u32),
    PerHour(u64),
}

