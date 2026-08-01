mod prompts;
mod services;
mod worker;

use std::{path::PathBuf, sync::Arc};

pub(crate) use kcode_kennedy_sessions::{
    AgentMode, RuntimeModel, Service as SessionService, Session,
};
pub(crate) use prompts::Manuals;
pub(crate) use services::{Api, ApiError, LocalServices};
pub(crate) use worker::Orchestrator;

#[derive(Clone, Debug)]
pub(crate) struct Config {
    pub system_prompts_directory: PathBuf,
    pub user_root_node_id: String,
    pub kennedy_root_node_id: String,
    pub telegram_max_media_bytes: usize,
    pub telegram_web_user_handle: String,
    pub runtime_model: RuntimeModel,
}

/// Run Kennedy's native backend coordinator for the lifetime of the server.
///
/// Kennedy-owned services are already open and are called through cloned
/// in-process handles, including the Telegram transport.
pub(crate) fn build(config: Config, api: Api, sessions: SessionService) -> Arc<Orchestrator> {
    Arc::new(Orchestrator::new(config, api, sessions))
}

pub(crate) async fn run(worker: Arc<Orchestrator>) -> anyhow::Result<()> {
    worker.run().await
}
