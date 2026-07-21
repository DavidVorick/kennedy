mod context;
mod http;
mod prompts;
mod session;
mod worker;

use std::{path::PathBuf, sync::Arc};

pub(crate) use http::{Api, ApiError, LocalServices};
pub(crate) use prompts::{Manuals, RuntimeModel};
pub(crate) use session::{AgentMode, Session};

#[derive(Clone, Debug)]
pub(crate) struct Config {
    pub system_prompts_directory: PathBuf,
    #[cfg(test)]
    pub kweb_base: String,
    #[cfg(test)]
    pub intelligence_base: String,
    #[cfg(test)]
    pub conversation_history_base: String,
    pub telegram_relay_base: String,
    #[cfg(test)]
    pub audio_ingress_base: String,
    pub telegram_web_user_handle: String,
}

/// Run Kennedy's native backend coordinator for the lifetime of the server.
///
/// Kennedy-owned services are already open and are called through cloned
/// in-process handles. Readiness retries remain for the separately published
/// Telegram relay, whose current crate API still owns a loopback listener.
pub(crate) async fn run(config: Config, api: Api) -> anyhow::Result<()> {
    let worker = Arc::new(worker::Orchestrator::new(config, api));
    worker.run().await
}
