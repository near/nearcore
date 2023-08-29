use anyhow::Context;
use borsh::BorshDeserialize;
use near_async::time;
use near_network::raw::{ConnectError, Connection, DirectMessage, Message};
use near_network::types::HandshakeFailureReason;
use near_primitives::hash::CryptoHash;
use near_primitives::network::PeerId;
use near_primitives::types::{BlockHeight, EpochHeight, EpochId, ShardId, StateRoot};
use near_primitives::version::ProtocolVersion;
use sha2::Digest;
use sha2::Sha256;
use std::collections::HashMap;
use std::net::SocketAddr;
pub mod cli;
mod rpc_requests;
use near_client::sync::external::{
    create_bucket_readonly, create_bucket_readwrite, external_storage_location,
    external_storage_location_directory, get_num_parts_from_filename, ExternalConnection,
};
use near_store::Trie;

fn create_external_connection(
    root_dir: Option<PathBuf>,
    bucket: Option<String>,
    region: Option<String>,
    gcs_bucket: Option<String>,
    credentials_file: Option<PathBuf>,
    mode: Mode,
) -> ExternalConnection {
    if let Some(root_dir) = root_dir {
        ExternalConnection::Filesystem { root_dir }
    } else if let (Some(bucket), Some(region)) = (bucket, region) {
        let bucket = match mode {
            Mode::Readonly => create_bucket_readonly(&bucket, &region, Duration::from_secs(5)),
            Mode::Readwrite => {
                create_bucket_readwrite(&bucket, &region, Duration::from_secs(5), credentials_file)
            }
        }
        .expect("Failed to create an S3 bucket");
        ExternalConnection::S3 { bucket: Arc::new(bucket) }
    } else if let Some(bucket) = gcs_bucket {
        ExternalConnection::GCS {
            gcs_client: Arc::new(cloud_storage::Client::default()),
            reqwest_client: Arc::new(reqwest::Client::default()),
            bucket,
        }
    } else {
        panic!(
            "Please provide --root-dir, or both of --s3-bucket and --s3-region, or --gcs-bucket"
        );
    }
}

async fn state_header_from_node(
    sync_hash: CryptoHash,
    shard_id: ShardId,
    chain_id: &str,
    genesis_hash: CryptoHash,
    head_height: BlockHeight,
    protocol_version: Option<ProtocolVersion>,
    peer_id: PeerId,
    peer_addr: SocketAddr,
    recv_timeout_seconds: u32,
) -> anyhow::Result<StateRoot> {
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
    tracing::info!(target: "state-parts-dump-check", ?peer_addr, ?peer_id, "Connected to peer");

    let mut result = Ok(());
    let mut part_id = start_part_id;

    let target = &peer_id;
    let msg = RoutedMessage::StateRequestHeader(shard_id, sync_hash);
    tracing::info!(target: "state-parts-dump-check", ?target, shard_id, ?sync_hash, part_id, ttl, "Sending a StateRequestHeader request");
    result = peer
        .send_routed_message(msg, peer_id.clone(), ttl)
        .await
        .with_context(|| format!("Failed sending StateRequestHeaderRequest to {:?}", target));
    app_info.requests_sent.insert(part_id, time::Instant::now());
    tracing::info!(target: "state-parts-dump-check", ?result);
    if result.is_err() {
        panic!("state header message failed to send");
    }
    res = peer.recv();
    let (msg, first_byte_time) = match res {
        Ok(x) => x,
        Err(e) => {
            result = Err(e).context("Failed receiving messages");
            break;
        }
    };
    result = handle_message(&mut app_info, &msg, first_byte_time.try_into().unwrap());
    if result.is_err() {
        break;
    }
    result
}

fn handle_message(
    app_info: &mut AppInfo,
    msg: &Message,
    received_at: time::Instant,
) -> anyhow::Result<StateRoot> {
    match &msg {
        Message::Direct(DirectMessage::VersionedStateResponse(response)) => {
            let shard_id = response.shard_id();
            let sync_hash = response.sync_hash();
            let state_response = response.clone().take_state_response();
            let state_header = state_response.take_header().unwrap();
            let state_root = state_header.chunk_prev_state_root();

            tracing::info!(
                shard_id,
                ?sync_hash,
                ?state_root,
                "Received VersionedStateResponse for StateRequestHeader"
            );
            Ok(state_root)
        }
        _ => anyhow::Error("Response is invalid"),
    }
}

fn validate_state_part(state_root: &StateRoot, part_id: PartId, part: &[u8]) -> bool {
    match BorshDeserialize::try_from_slice(data) {
        Ok(trie_nodes) => {
            match Trie::validate_state_part(state_root, part_id, trie_nodes) {
                Ok(_) => true,
                // Storage error should not happen
                Err(err) => {
                    tracing::error!(target: "state-parts", ?err, "State part storage error");
                    false
                }
            }
        }
        // Deserialization error means we've got the data from malicious peer
        Err(err) => {
            tracing::error!(target: "state-parts", ?err, "State part deserialization error");
            false
        }
    }
}
