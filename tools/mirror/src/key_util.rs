use anyhow::Context;
use near_chain::types::RuntimeAdapter;
use near_chain::{ChainStore, ChainStoreAccess};
use near_chain_configs::GenesisValidationMode;
use near_crypto::{PublicKey, SecretKey};
use near_epoch_manager::EpochManager;
use near_epoch_manager::shard_assignment::{account_id_to_shard_id, shard_id_to_uid};
use near_jsonrpc_primitives::types::query::{
    QueryResponseKind as RpcQueryResponseKind, RpcQueryRequest,
};
use near_primitives::types::{AccountId, BlockHeight, BlockId, BlockReference, Finality};
use near_primitives::views::{AccessKeyPermissionView, QueryRequest, QueryResponseKind};
use nearcore::{NightshadeRuntime, NightshadeRuntimeExt};
use std::num::NonZeroU32;
use std::path::Path;

// Access-key list page size; the node clamps it to its own limit.
pub(crate) const ACCESS_KEY_PAGE_SIZE: Option<NonZeroU32> = NonZeroU32::new(1000);

pub(crate) struct SecretAccessKey {
    pub(crate) original_key: Option<PublicKey>,
    pub(crate) mapped_key: SecretKey,
    pub(crate) permission: Option<AccessKeyPermissionView>,
}

pub(crate) fn default_extra_key(
    secret: Option<&[u8; crate::secret::SECRET_LEN]>,
) -> SecretAccessKey {
    SecretAccessKey {
        original_key: None,
        mapped_key: crate::key_mapping::default_extra_key(secret),
        permission: None,
    }
}

pub(crate) fn map_pub_key(
    public_key: &str,
    secret: Option<&[u8; crate::secret::SECRET_LEN]>,
) -> anyhow::Result<SecretAccessKey> {
    let public_key: PublicKey = public_key.parse().context("Could not parse public key")?;
    // we say original_key is None here because the user provided it on the command line in this case, so no need to print it again.
    Ok(SecretAccessKey {
        original_key: None,
        mapped_key: crate::key_mapping::map_key(&public_key, secret),
        permission: None,
    })
}

pub(crate) fn keys_from_source_db(
    home: &Path,
    account_id: &str,
    block_height: Option<BlockHeight>,
    secret: Option<&[u8; crate::secret::SECRET_LEN]>,
) -> anyhow::Result<Vec<SecretAccessKey>> {
    let account_id: AccountId = account_id.parse().context("bad account ID")?;

    let config = nearcore::config::load_config(home.as_ref(), GenesisValidationMode::UnsafeFast)
        .with_context(|| format!("Error loading config from {}", home.display()))?;
    let node_storage =
        nearcore::open_storage(home.as_ref(), &config).context("failed opening storage")?;
    let store = node_storage.get_hot_store();
    let chain = ChainStore::new(
        store.clone(),
        config.client_config.save_trie_changes,
        config.genesis.config.transaction_validity_period,
    );
    let epoch_manager =
        EpochManager::new_arc_handle(store.clone(), &config.genesis.config, Some(home));
    let runtime =
        NightshadeRuntime::from_config(home.as_ref(), store, &config, epoch_manager.clone())
            .context("could not create the transaction runtime")?;
    let block_height = match block_height {
        Some(h) => h,
        None => {
            let head = chain.head().context("failed getting chain head")?;
            head.height
        }
    };

    let header = chain
        .get_block_header_by_height(block_height)
        .with_context(|| format!("failed getting block header #{}", block_height))?;
    let shard_id = account_id_to_shard_id(epoch_manager.as_ref(), &account_id, header.epoch_id())
        .with_context(|| format!("failed finding shard for {}", &account_id))?;
    let shard_uid = shard_id_to_uid(epoch_manager.as_ref(), shard_id, header.epoch_id())
        .context("failed mapping ShardID to ShardUID")?;
    let chunk_extra =
        chain.get_chunk_extra(header.hash(), &shard_uid).context("failed getting chunk extra")?;
    let mut keys = Vec::new();
    let mut after_key = None;
    loop {
        let response = runtime
            .query(
                shard_uid,
                chunk_extra.state_root(),
                header.height(),
                header.raw_timestamp(),
                header.prev_hash(),
                header.hash(),
                header.epoch_id(),
                &QueryRequest::ViewAccessKeyList {
                    account_id: account_id.clone(),
                    after_key,
                    limit: ACCESS_KEY_PAGE_SIZE,
                },
            )
            .with_context(|| format!("failed fetching access keys for {}", &account_id))?;
        let QueryResponseKind::AccessKeyList(l) = response.kind else {
            unreachable!();
        };
        keys.extend(l.keys.into_iter().filter_map(|k| {
            // TODO(post-quantum): Mirror does not support ML-DSA-65 today;
            // hash-form entries can't be mapped because the full pubkey is not
            // recoverable. See key_mapping.rs for the matching panic.
            let full_pk = k.public_key.full_pubkey()?;
            Some(SecretAccessKey {
                mapped_key: crate::key_mapping::map_key(&full_pk, secret),
                original_key: Some(full_pk),
                permission: Some(k.access_key.permission),
            })
        }));
        match l.last_key {
            Some(cursor) => after_key = Some(cursor),
            None => return Ok(keys),
        }
    }
}

pub(crate) async fn keys_from_rpc(
    rpc_url: &str,
    account_id: &str,
    block_height: Option<BlockHeight>,
    secret: Option<&[u8; crate::secret::SECRET_LEN]>,
) -> anyhow::Result<Vec<SecretAccessKey>> {
    let account_id: AccountId = account_id.parse().context("bad account ID")?;

    let rpc_client = near_jsonrpc_client_internal::new_client(rpc_url);

    let block_reference = match block_height {
        Some(h) => BlockReference::BlockId(BlockId::Height(h)),
        None => BlockReference::Finality(Finality::None),
    };
    let mut keys = Vec::new();
    let mut after_key = None;
    loop {
        let request = RpcQueryRequest {
            block_reference: block_reference.clone(),
            request: QueryRequest::ViewAccessKeyList {
                account_id: account_id.clone(),
                after_key,
                limit: ACCESS_KEY_PAGE_SIZE,
            },
        };

        let response = match rpc_client.query(request).await {
            Ok(r) => r,
            Err(e) => anyhow::bail!("failed making RPC request: {:?}", e),
        };

        let RpcQueryResponseKind::AccessKeyList(l) = response.kind else {
            anyhow::bail!(
                "received unexpected RPC response for access key query: {:?}",
                response.kind
            );
        };
        keys.extend(l.keys.into_iter().filter_map(|k| {
            // TODO(post-quantum): Mirror does not support ML-DSA-65 today;
            // hash-form entries can't be mapped because the full pubkey is not
            // recoverable. See key_mapping.rs for the matching panic.
            let full_pk = k.public_key.full_pubkey()?;
            Some(SecretAccessKey {
                mapped_key: crate::key_mapping::map_key(&full_pk, secret),
                original_key: Some(full_pk),
                permission: Some(k.access_key.permission),
            })
        }));
        match l.last_key {
            Some(cursor) => after_key = Some(cursor),
            None => return Ok(keys),
        }
    }
}
