use std::path::PathBuf;

use clap::Parser;

#[derive(Parser, Debug)]
struct Args {
    #[arg(long, default_value = "127.0.0.1:4321")]
    kweb_bind: String,
    #[arg(long, default_value = "127.0.0.1:4323")]
    conversation_history_bind: String,
    #[arg(long, default_value = "http://127.0.0.1:4321")]
    frontend_origin: String,
    #[arg(long, default_value = "./kennedy.sqlite3")]
    kweb_database: PathBuf,
    #[arg(long, default_value = "./kennedy-conversations.sqlite3")]
    conversation_history_database: PathBuf,
    #[arg(long, default_value = "./IntelligenceBackend/config.yaml")]
    intelligence_config: PathBuf,
    #[arg(long, default_value = "./Frontend/public")]
    frontend_dir: PathBuf,
    #[arg(long, default_value = "./Frontend/SystemPrompts")]
    system_prompts_dir: PathBuf,
    #[arg(long, default_value_t = 12)]
    active_limit: usize,
    #[arg(long, default_value_t = 60)]
    fanout_limit: usize,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| {
                "kennedy_server=info,kennedy_kweb=info,kennedy_intelligence=info,kennedy_conversation_history=info,tower_http=info".into()
            }),
        )
        .init();
    rustls::crypto::ring::default_provider()
        .install_default()
        .map_err(|_| anyhow::anyhow!("installing TLS crypto provider"))?;
    let args = Args::parse();
    let kweb = kennedy_kweb::Config {
        bind: args.kweb_bind,
        database: args.kweb_database,
        frontend_dir: args.frontend_dir,
        system_prompts_dir: args.system_prompts_dir,
        active_limit: args.active_limit,
        fanout_limit: args.fanout_limit,
    };
    let history = kennedy_conversation_history::Config {
        bind: args.conversation_history_bind,
        database: args.conversation_history_database,
        allowed_origins: vec![args.frontend_origin],
        max_request_bytes: 10 * 1024 * 1024,
    };
    let intelligence_config = args.intelligence_config;
    tokio::try_join!(
        kennedy_kweb::serve(kweb),
        kennedy_intelligence::serve(intelligence_config),
        kennedy_conversation_history::serve(history),
    )?;
    Ok(())
}
