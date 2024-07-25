use anyhow::Context;
use std::path::Path;

use near_chain::types::RuntimeAdapter;
use near_chain::{ChainStore, ChainStoreAccess};
use near_chain_configs::GenesisValidationMode;
use near_crypto::{PublicKey, SecretKey};
use near_epoch_manager::{EpochManager, EpochManagerAdapter};
use near_primitives::types::{AccountId, BlockHeight};
use near_primitives::views::{AccessKeyPermissionView, QueryRequest, QueryResponseKind};
use nearcore::{NightshadeRuntime, NightshadeRuntimeExt};

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

    let mut config =
        nearcore::config::load_config(home.as_ref(), GenesisValidationMode::UnsafeFast)
            .with_context(|| format!("Error loading config from {}", home.display()))?;
    let node_storage =
        nearcore::open_storage(home.as_ref(), &mut config).context("failed opening storage")?;
    let store = node_storage.get_hot_store();
    let chain = ChainStore::new(
        store.clone(),
        config.genesis.config.genesis_height,
        config.client_config.save_trie_changes,
    );
    let epoch_manager = EpochManager::new_arc_handle(store.clone(), &config.genesis.config);
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
    let shard_id = epoch_manager
        .account_id_to_shard_id(&account_id, header.epoch_id())
        .with_context(|| format!("failed finding shard for {}", &account_id))?;
    let shard_uid = epoch_manager
        .shard_id_to_uid(shard_id, header.epoch_id())
        .context("failed mapping ShardID to ShardUID")?;
    let chunk_extra =
        chain.get_chunk_extra(header.hash(), &shard_uid).context("failed getting chunk extra")?;
    match runtime
        .query(
            shard_uid,
            chunk_extra.state_root(),
            header.height(),
            header.raw_timestamp(),
            header.prev_hash(),
            header.hash(),
            header.epoch_id(),
            &QueryRequest::ViewAccessKeyList { account_id: account_id.clone() },
        )
        .with_context(|| format!("failed fetching access keys for {}", &account_id))?
        .kind
    {
        QueryResponseKind::AccessKeyList(l) => Ok(l
            .keys
            .into_iter()
            .map(|k| SecretAccessKey {
                mapped_key: crate::key_mapping::map_key(&k.public_key, secret),
                original_key: Some(k.public_key),
                permission: Some(k.access_key.permission),
            })
            .collect()),
        _ => unreachable!(),
    }
}
