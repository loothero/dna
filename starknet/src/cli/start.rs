use apibara_dna_common::{run_server, StartArgs};
use clap::Args;
use error_stack::{Result, ResultExt};
use tokio_util::sync::CancellationToken;
use tracing::info;

use crate::{error::StarknetError, StarknetBlockIngestionOptions, StarknetChainSupport};

use super::rpc::RpcArgs;

#[derive(Args, Debug)]
pub struct StartCommand {
    #[clap(flatten)]
    rpc: RpcArgs,
    #[clap(flatten)]
    start: StartArgs,

    /// If set, use WebSocket to trigger block ingestion.
    #[arg(long = "starknet.ws-url", env = "STARKNET_WS_URL")]
    ws_url: Option<String>,

    /// Ingest traces.
    #[arg(
        long = "starknet.ingest-traces",
        env = "STARKNET_INGEST_TRACES",
        default_value = "false"
    )]
    ingest_traces: bool,

    /// Ingest pre-confirmed blocks.
    #[arg(
        long = "starknet.ingest-pre-confirmed",
        env = "STARKNET_INGEST_PRE_CONFIRMED",
        default_value = "false"
    )]
    ingest_pre_confirmed: bool,

    /// Ingest pre-confirmed transaction and receipt data from Starknet WebSocket subscriptions.
    #[arg(
        long = "starknet.ws-live-ingestion-enabled",
        env = "STARKNET_WS_LIVE_INGESTION_ENABLED",
        default_value = "false"
    )]
    ws_live_ingestion_enabled: bool,

    /// Contract address filter for live pre-confirmed Starknet event subscriptions.
    #[arg(
        long = "starknet.ws-live-event-address",
        env = "STARKNET_WS_LIVE_EVENT_ADDRESS"
    )]
    ws_live_event_address: Option<String>,

    /// First event key filter for live pre-confirmed Starknet event subscriptions.
    #[arg(
        long = "starknet.ws-live-event-key0",
        env = "STARKNET_WS_LIVE_EVENT_KEY0"
    )]
    ws_live_event_key0: Option<String>,
}

impl StartCommand {
    pub async fn run(self, ct: CancellationToken) -> Result<(), StarknetError> {
        info!("Starting Starknet DNA server");
        if self.ws_live_ingestion_enabled && self.ws_url.is_none() {
            return Err(StarknetError).attach_printable(
                "STARKNET_WS_URL is required when STARKNET_WS_LIVE_INGESTION_ENABLED=true",
            );
        }

        let provider = self.rpc.to_starknet_provider()?;
        let starknet_ingestion_options = StarknetBlockIngestionOptions {
            ingest_pending: self.ingest_pre_confirmed || self.ws_live_ingestion_enabled,
            ingest_traces: self.ingest_traces,
            live_ingestion_enabled: self.ws_live_ingestion_enabled,
            live_event_address: self.ws_live_event_address,
            live_event_key0: self.ws_live_event_key0,
        };
        let starknet_chain =
            StarknetChainSupport::new(provider, self.ws_url, starknet_ingestion_options);

        run_server(starknet_chain, self.start, env!("CARGO_PKG_VERSION"), ct)
            .await
            .change_context(StarknetError)
    }
}
