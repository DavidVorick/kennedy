mod context;
mod http;
mod prompts;
mod session;
mod worker;

use std::{path::PathBuf, sync::Arc};

use anyhow::Context;

pub(crate) use http::{Api, ApiError};
pub(crate) use prompts::{Manuals, RuntimeModel};
pub(crate) use session::{AgentMode, Session};

#[derive(Clone, Debug)]
pub(crate) struct Config {
    pub system_prompts_directory: PathBuf,
    pub kweb_base: String,
    pub intelligence_base: String,
    pub conversation_history_base: String,
    pub telegram_relay_base: String,
    pub audio_ingress_base: String,
    pub telegram_web_user_handle: String,
}

/// Run Kennedy's native backend coordinator for the lifetime of the server.
///
/// Service startup is intentionally concurrent with this future. The worker
/// retries its readiness pass until all of the sibling HTTP services are
/// listening, then owns every durable conversation and ingress queue.
pub(crate) async fn run(config: Config) -> anyhow::Result<()> {
    let api = Api::new(&config).context("building backend orchestration HTTP client")?;
    let worker = Arc::new(worker::Orchestrator::new(config, api));
    worker.run().await
}
