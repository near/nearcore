use anyhow::Context;
use near_async::time;
use near_network::raw::{ConnectError, Connection, Message, RoutedMessage};
use near_network::types::HandshakeFailureReason;
use near_primitives::hash::CryptoHash;
use near_primitives::network::PeerId;
use near_primitives::types::{AccountId, BlockHeight, ShardId};
use near_primitives::version::ProtocolVersion;
use std::collections::HashMap;
use std::net::SocketAddr;

pub mod cli;

struct AppInfo {
    pub requests_sent: HashMap<u64, time::Instant>,
}

impl AppInfo {
    fn new() -> Self {
        Self { requests_sent: HashMap::new() }
    }
}

fn handle_message(
    app_info: &mut AppInfo,
    msg: &Message,
    received_at: time::Instant,
) -> anyhow::Result<()> {
    match &msg {
        Message::Routed(RoutedMessage::VersionedStateResponse(response)) => {
            let shard_id = response.shard_id();
            let sync_hash = response.sync_hash();
            let state_response = response.clone().take_state_response();
            let part_id = state_response.part_id();
            let duration = if let Some(part_id) = part_id {
                let duration = app_info
                    .requests_sent
                    .get(&part_id)
                    .map(|sent| (sent.elapsed() - received_at.elapsed()).as_seconds_f64());
                app_info.requests_sent.remove(&part_id);
                duration
            } else {
                None
            };
            tracing::info!(
                shard_id,
                ?sync_hash,
                ?part_id,
                ?duration,
                "Received VersionedStateResponse"
            );
        }
        _ => {}
    };
    Ok(())
}

#[derive(Debug)]
struct PeerIdentifier {
    account_id: Option<AccountId>,
    peer_id: PeerId,
}

impl std::fmt::Display for PeerIdentifier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        match &self.account_id {
            Some(a) => a.fmt(f),
            None => self.peer_id.fmt(f),
        }
    }
}

async fn state_parts_from_node(
    block_hash: CryptoHash,
    shard_id: ShardId,
    chain_id: &str,
    genesis_hash: CryptoHash,
    head_height: BlockHeight,
    protocol_version: Option<ProtocolVersion>,
    peer_id: PeerId,
    peer_addr: SocketAddr,
    ttl: u8,
    request_frequency_millis: u64,
    recv_timeout_seconds: u32,
    start_part_id: u64,
    num_parts: u64,
) -> anyhow::Result<()> {
    assert!(start_part_id < num_parts && num_parts > 0, "{}/{}", start_part_id, num_parts);
    let mut app_info = AppInfo::new();

    let mut peer = match Connection::connect(
        peer_addr,
        peer_id.clone(),
        protocol_version,
        chain_id,
        genesis_hash,
        head_height,
        vec![0],
        time::Duration::seconds(recv_timeout_seconds.into())).await {
        Ok(p) => p,
        Err(ConnectError::HandshakeFailure(reason)) => {
            match reason {
                HandshakeFailureReason::ProtocolVersionMismatch { version, oldest_supported_version } => anyhow::bail!(
                    "Received Handshake Failure: {:?}. Try running again with --protocol-version between {} and {}",
                    reason, oldest_supported_version, version
                ),
                HandshakeFailureReason::GenesisMismatch(_) => anyhow::bail!(
                    "Received Handshake Failure: {:?}. Try running again with --chain-id and --genesis-hash set to these values.",
                    reason,
                ),
                HandshakeFailureReason::InvalidTarget => anyhow::bail!(
                    "Received Handshake Failure: {:?}. Is the public key given with --peer correct?",
                    reason,
                ),
            }
        }
        Err(e) => {
            anyhow::bail!("Error connecting to {:?}: {}", peer_addr, e);
        }
    };
    tracing::info!(target: "state-parts", ?peer_addr, ?peer_id, "Connected to peer");

    let next_request = tokio::time::sleep(std::time::Duration::ZERO);
    tokio::pin!(next_request);

    let mut result = Ok(());
    let mut part_id = start_part_id;
    loop {
        tokio::select! {
            _ = &mut next_request => {
                let target = &peer_id;
                let msg = RoutedMessage::StateRequestPart(shard_id, block_hash, part_id);
                tracing::info!(target: "state-parts", ?target, shard_id, ?block_hash, part_id, ttl, "Sending a request");
                result = peer.send_routed_message(msg, peer_id.clone(), ttl).await.with_context(|| format!("Failed sending State Part Request to {:?}", target));
                app_info.requests_sent.insert(part_id, time::Instant::now());
                tracing::info!(target: "state-parts", ?result);
                if result.is_err() {
                    break;
                }
                next_request.as_mut().reset(tokio::time::Instant::now() + std::time::Duration::from_millis(request_frequency_millis));
                part_id = (part_id + 1) % num_parts;
            }
            res = peer.recv() => {
                let (msg, first_byte_time) = match res {
                    Ok(x) => x,
                    Err(e) => {
                        result = Err(e).context("Failed receiving messages");
                        break;
                    }
                };
                result = handle_message(
                            &mut app_info,
                            &msg,
                            first_byte_time.try_into().unwrap(),
                        );
                if result.is_err() {
                    break;
                }
            }
            _ = tokio::signal::ctrl_c() => {
                break;
            }
        }
    }
    result
}
