mod context;
mod http;
mod prompts;
mod session;
mod worker;

use std::{path::PathBuf, sync::Arc};

pub(crate) use http::{Api, ApiError, LocalServices};
pub(crate) use prompts::{Manuals, RuntimeModel, human_utc_datetime, runtime_description};
pub(crate) use session::{AgentMode, Session};
pub(crate) use worker::Orchestrator;

#[derive(Clone, Debug)]
pub(crate) struct Config {
    pub system_prompts_directory: PathBuf,
    #[cfg(test)]
    pub kweb_base: String,
    #[cfg(test)]
    pub intelligence_base: String,
    #[cfg(test)]
    pub session_history_base: String,
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
pub(crate) fn build(config: Config, api: Api) -> Arc<Orchestrator> {
    Arc::new(Orchestrator::new(config, api))
}

pub(crate) async fn run(worker: Arc<Orchestrator>) -> anyhow::Result<()> {
    worker.run().await
}
