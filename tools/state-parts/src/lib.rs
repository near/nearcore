use ::time::ext::InstantExt as _;
use anyhow::Context;
use near_network::raw::{ConnectError, Connection, DirectMessage, Message};
use near_network::types::HandshakeFailureReason;
use near_primitives::hash::CryptoHash;
use near_primitives::network::PeerId;
use near_primitives::types::{BlockHeight, ShardId};
use near_primitives::version::ProtocolVersion;
use near_time::Instant;
use sha2::Digest;
use sha2::Sha256;
use std::collections::HashMap;
use std::fmt::Write;
use std::net::SocketAddr;

pub mod cli;

struct AppInfo {
    pub requests_sent: HashMap<u64, near_time::Instant>,
}

impl AppInfo {
    fn new() -> Self {
        Self { requests_sent: HashMap::new() }
    }
}

fn handle_message(
    app_info: &mut AppInfo,
    msg: &Message,
    received_at: near_time::Instant,
) -> anyhow::Result<()> {
    match &msg {
        Message::Direct(DirectMessage::VersionedStateResponse(response)) => {
            let shard_id = response.shard_id();
            let sync_hash = response.sync_hash();
            let state_response = response.clone().take_state_response();
            let cached_parts = state_response.cached_parts();
            let part_id = state_response.part_id();
            let now = Instant::now();
            let duration = if let Some(part_id) = part_id {
                let duration = app_info.requests_sent.get(&part_id).map(|sent| {
                    (now.signed_duration_since(*sent) - now.signed_duration_since(received_at))
                        .as_seconds_f64()
                });
                app_info.requests_sent.remove(&part_id);
                duration
            } else {
                None
            };
            let part_hash = if let Some(part) = state_response.part() {
                Sha256::digest(&part.1).iter().fold(String::new(), |mut v, byte| {
                    write!(&mut v, "{:02x}", byte).unwrap();
                    v
                })
            } else {
                "No part".to_string()
            };

            tracing::info!(
                ?shard_id,
                ?sync_hash,
                ?part_id,
                ?duration,
                ?part_hash,
                ?cached_parts,
                "Received VersionedStateResponse",
            );
        }
        _ => {}
    };
    Ok(())
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

    let clock = near_time::Clock::real();

    let mut peer = match Connection::connect(
        &clock,
        peer_addr,
        peer_id.clone(),
        protocol_version,
        chain_id,
        genesis_hash,
        head_height,
        vec![ShardId::new(0)],
        Some(near_time::Duration::seconds(recv_timeout_seconds.into())),
    )
    .await
    {
        Ok(p) => p,
        Err(ConnectError::HandshakeFailure(reason)) => match reason {
            HandshakeFailureReason::ProtocolVersionMismatch {
                version,
                oldest_supported_version,
            } => anyhow::bail!(
                "Received Handshake Failure: {:?}. Try running again with --protocol-version between {} and {}",
                reason,
                oldest_supported_version,
                version
            ),
            HandshakeFailureReason::GenesisMismatch(_) => anyhow::bail!(
                "Received Handshake Failure: {:?}. Try running again with --chain-id and --genesis-hash set to these values.",
                reason,
            ),
            HandshakeFailureReason::InvalidTarget => anyhow::bail!(
                "Received Handshake Failure: {:?}. Is the public key given with --peer correct?",
                reason,
            ),
        },
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
                let msg = DirectMessage::StateRequestPart(shard_id, block_hash, part_id);
                tracing::info!(target: "state-parts", ?target, ?shard_id, ?block_hash, part_id, ttl, "Sending a request");
                result = peer.send_message(msg).await.with_context(|| format!("Failed sending State Part Request to {:?}", target));
                app_info.requests_sent.insert(part_id, near_time::Instant::now());
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
