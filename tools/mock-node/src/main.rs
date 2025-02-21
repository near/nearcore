//! A binary that starts a mock testing environment for ClientActor. It
//! simulates the entire network by substituting PeerManagerActor with a mock
//! network, responding to the client's network requests by reading from a
//! pre-generated chain history in storage.

use anyhow::Context;
use mock_node::MockNetworkConfig;
use mock_node::setup::setup_mock_node;
use near_o11y::testonly::init_integration_logger;
use near_primitives::types::BlockHeight;
use near_primitives::version::ProtocolVersion;
use std::path::Path;
use std::time::Duration;

/// Program to start a mock node, which starts a TCP server and accepts incoming
/// connections from NEAR nodes. Once connected, it will respond to block and chunk
/// requests, but not do anything else unless periodic outgoing messages are
/// are specified in $home/mock.json.
///
#[derive(clap::Parser)]
struct Cli {
    /// Existing home dir for the pre-generated chain history. For example, you can use
    /// the home dir of a near node.
    #[clap(long)]
    home: String,
    /// If set, the mock node will wait this many millis before sending messages
    #[clap(short = 'd', long)]
    network_delay: Option<u64>,
    /// The height at which the mock network starts. The client would have to
    /// catch up to this height before participating in new block production.
    ///
    /// Defaults to the largest height in history.
    #[clap(long)]
    network_height: Option<BlockHeight>,
    /// Target height that the client should sync to before stopping. If not specified,
    /// use the height of the last block in chain history
    #[clap(long)]
    target_height: Option<BlockHeight>,
    /// Protocol version to advertise in handshakes
    #[clap(long)]
    handshake_protocol_version: Option<ProtocolVersion>,
    /// If set, advertise that the node is archival in the handshake
    #[clap(long)]
    archival: bool,
}

fn main() -> anyhow::Result<()> {
    init_integration_logger();
    let args: Cli = clap::Parser::parse();
    let home_dir = Path::new(&args.home);

    let mock_config_path = home_dir.join("mock.json");
    let mut network_config = if mock_config_path.exists() {
        MockNetworkConfig::from_file(&mock_config_path).with_context(|| {
            format!("Error loading mock config from {}", mock_config_path.display())
        })?
    } else {
        MockNetworkConfig::default()
    };
    if let Some(delay) = args.network_delay {
        network_config.response_delay = Duration::from_millis(delay);
    }

    let runtime = tokio::runtime::Runtime::new().unwrap();
    let res = runtime.block_on(async move {
        let mock_peer = setup_mock_node(
            home_dir,
            network_config,
            args.network_height,
            args.target_height,
            args.handshake_protocol_version,
            args.archival,
        )
        .context("failed setting up mock node")?;

        mock_peer.await.context("failed running mock peer task")?.context("mock peer failed")
    });

    res
}
