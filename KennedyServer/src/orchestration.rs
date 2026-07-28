mod context;
mod http;
mod prompts;
mod session;
mod worker;

use std::{path::PathBuf, sync::Arc};

pub(crate) use http::{Api, ApiError, LocalServices};
pub(crate) use prompts::{Manuals, RuntimeModel, human_utc_datetime, runtime_description};
pub(crate) use session::{AgentMode, Session};

#[derive(Clone, Debug)]
pub(crate) struct Config {
    pub system_prompts_directory: PathBuf,
    #[cfg(test)]
    pub kweb_base: String,
    #[cfg(test)]
    pub intelligence_base: String,
    #[cfg(test)]
    pub session_history_base: String,
    pub telegram_relay_base: String,
    pub telegram_max_media_bytes: usize,
    pub telegram_web_user_handle: String,
    pub runtime_model: RuntimeModel,
}

/// Run Kennedy's native backend coordinator for the lifetime of the server.
///
/// Kennedy-owned services are already open and are called through cloned
/// in-process handles. The separately published Telegram relay still owns its
/// loopback listener, so do not construct the worker until its health endpoint
/// confirms that the listener is serving requests.
pub(crate) async fn run(config: Config, api: Api) -> anyhow::Result<()> {
    api.wait_until_telegram_ready().await;
    let worker = Arc::new(worker::Orchestrator::new(config, api));
    worker.run().await
}
