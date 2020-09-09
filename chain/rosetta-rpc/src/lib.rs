use std::convert::{AsRef, TryInto};
use std::sync::Arc;

use actix::Addr;
use actix_cors::{Cors, CorsFactory};
use actix_web::{App, HttpServer};
use paperclip::actix::{
    api_v2_operation,
    web::{self, Json},
    OpenApiExt,
};
use strum::IntoEnumIterator;

use near_chain_configs::Genesis;
use near_client::{ClientActor, ViewClientActor};
use near_primitives::serialize::BaseEncode;

pub use config::RosettaRpcConfig;

mod adapters;
mod config;
mod errors;
mod models;
mod utils;

pub const BASE_PATH: &str = "";
pub const API_VERSION: &str = "1.4.3";

/// Get List of Available Networks
///
/// This endpoint returns a list of NetworkIdentifiers that the Rosetta server
/// supports.
#[api_v2_operation]
async fn network_list(
    client_addr: web::Data<Addr<ClientActor>>,
    _body: Json<models::MetadataRequest>,
) -> Result<Json<models::NetworkListResponse>, models::Error> {
    let status = client_addr
        .send(near_client::Status { is_health_check: false })
        .await?
        .map_err(errors::ErrorKind::InternalError)?;
    Ok(Json(models::NetworkListResponse {
        network_identifiers: vec![models::NetworkIdentifier {
            blockchain: "nearprotocol".to_owned(),
            network: status.chain_id,
            sub_network_identifier: None,
        }],
    }))
}

#[api_v2_operation]
/// Get Network Status
///
/// This endpoint returns the current status of the network requested. Any
/// NetworkIdentifier returned by /network/list should be accessible here.
async fn network_status(
    genesis: web::Data<Arc<Genesis>>,
    client_addr: web::Data<Addr<ClientActor>>,
    view_client_addr: web::Data<Addr<ViewClientActor>>,
    body: Json<models::NetworkRequest>,
) -> Result<Json<models::NetworkStatusResponse>, models::Error> {
    // TODO: reduce copy-paste
    let status = client_addr
        .send(near_client::Status { is_health_check: false })
        .await?
        .map_err(errors::ErrorKind::InternalError)?;
    if status.chain_id != body.network_identifier.network {
        return Err(models::Error {
            code: 2,
            message: "Wrong network (chain id)".to_string(),
            retriable: true,
        });
    }

    let genesis_height = genesis.config.genesis_height;
    let (network_info, genesis_block, earliest_block) = tokio::try_join!(
        client_addr.send(near_client::GetNetworkInfo {}),
        view_client_addr.send(near_client::GetBlock(
            near_primitives::types::BlockId::Height(genesis_height).into(),
        )),
        view_client_addr.send(near_client::GetBlock(
            near_primitives::types::BlockReference::SyncCheckpoint(
                near_primitives::types::SyncCheckpoint::EarliestAvailable
            ),
        )),
    )?;
    let network_info = network_info.map_err(errors::ErrorKind::InternalError)?;
    let genesis_block = genesis_block.map_err(errors::ErrorKind::InternalInvariantError)?;
    let earliest_block = earliest_block;

    let genesis_block_identifier: models::BlockIdentifier = (&genesis_block.header).into();
    let oldest_block_identifier: models::BlockIdentifier = earliest_block
        .ok()
        .map(|block| (&block.header).into())
        .unwrap_or_else(|| genesis_block_identifier.clone());
    Ok(Json(models::NetworkStatusResponse {
        current_block_identifier: models::BlockIdentifier {
            index: status.sync_info.latest_block_height.try_into().unwrap(),
            hash: status.sync_info.latest_block_hash.to_base(),
        },
        current_block_timestamp: status.sync_info.latest_block_time.timestamp_millis(),
        genesis_block_identifier,
        oldest_block_identifier,
        sync_status: if status.sync_info.syncing {
            Some(models::SyncStatus {
                current_index: status.sync_info.latest_block_height.try_into().unwrap(),
                target_index: None,
                stage: None,
            })
        } else {
            None
        },
        peers: network_info
            .active_peers
            .into_iter()
            .map(|peer| models::Peer { peer_id: peer.id.to_string() })
            .collect(),
    }))
}

#[api_v2_operation]
/// Get Network Options
///
/// This endpoint returns the version information and allowed network-specific
/// types for a NetworkIdentifier. Any NetworkIdentifier returned by
/// /network/list should be accessible here. Because options are retrievable in
/// the context of a NetworkIdentifier, it is possible to define unique options
/// for each network.
async fn network_options(
    client_addr: web::Data<Addr<ClientActor>>,
    body: Json<models::NetworkRequest>,
) -> Result<Json<models::NetworkOptionsResponse>, models::Error> {
    // TODO: reduce copy-paste
    let status = client_addr
        .send(near_client::Status { is_health_check: false })
        .await?
        .map_err(errors::ErrorKind::InternalError)?;
    if status.chain_id != body.network_identifier.network {
        return Err(models::Error {
            code: 2,
            message: "Wrong network (chain id)".to_string(),
            retriable: true,
        });
    }

    Ok(Json(models::NetworkOptionsResponse {
        version: models::Version {
            rosetta_version: API_VERSION.to_string(),
            node_version: status.version.version,
            middleware_version: None,
        },
        allow: models::Allow {
            operation_statuses: models::OperationStatusKind::iter()
                .map(|status| models::OperationStatus {
                    status,
                    successful: status.is_successful(),
                })
                .collect(),
            operation_types: models::OperationType::iter().collect(),
            errors: errors::ErrorKind::iter().map(models::Error::from_error_kind).collect(),
            historical_balance_lookup: true,
        },
    }))
}

#[api_v2_operation]
/// Get a Block
///
/// Get a block by its Block Identifier. If transactions are returned in the
/// same call to the node as fetching the block, the response should include
/// these transactions in the Block object. If not, an array of Transaction
/// Identifiers should be returned so /block/transaction fetches can be done to
/// get all transaction information.
///
/// When requesting a block by the hash component of the BlockIdentifier,
/// this request MUST be idempotent: repeated invocations for the same
/// hash-identified block must return the exact same block contents.
///
/// No such restriction is imposed when requesting a block by height,
/// given that a chain reorg event might cause the specific block at
/// height `n` to be set to a different one.
async fn block_details(
    genesis: web::Data<Arc<Genesis>>,
    client_addr: web::Data<Addr<ClientActor>>,
    view_client_addr: web::Data<Addr<ViewClientActor>>,
    body: Json<models::BlockRequest>,
) -> Result<Json<models::BlockResponse>, models::Error> {
    let Json(models::BlockRequest { network_identifier, block_identifier }) = body;

    // TODO: reduce copy-paste
    let status = client_addr
        .send(near_client::Status { is_health_check: false })
        .await?
        .map_err(errors::ErrorKind::InternalError)?;
    if status.chain_id != network_identifier.network {
        return Err(models::Error {
            code: 2,
            message: "Wrong network (chain id)".to_string(),
            retriable: true,
        });
    }

    // TODO: avoid ad-hoc error handling of common use-cases
    let block_id: near_primitives::types::BlockReference =
        block_identifier.try_into().map_err(|_| models::Error {
            code: 4,
            message: "Invalid input".to_string(),
            retriable: true,
        })?;

    let block = match view_client_addr.send(near_client::GetBlock(block_id.clone())).await? {
        Ok(block) => block,
        Err(_) => return Ok(Json(models::BlockResponse { block: None, other_transactions: None })),
    };

    let block_identifier: models::BlockIdentifier = (&block.header).into();

    let parent_block_identifier = if block.header.prev_hash == Default::default() {
        // According to Rosetta API genesis block should have the parent block
        // identifier referencing itself:
        block_identifier.clone()
    } else {
        let parent_block = view_client_addr
            .send(near_client::GetBlock(
                near_primitives::types::BlockId::Hash(block.header.prev_hash).into(),
            ))
            .await?
            .map_err(errors::ErrorKind::InternalError)?;

        models::BlockIdentifier {
            index: parent_block.header.height.try_into().unwrap(),
            hash: parent_block.header.hash.to_base(),
        }
    };

    let transactions = crate::adapters::collect_transactions(
        Arc::clone(&genesis),
        Addr::clone(&view_client_addr),
        &block,
    )
    .await?;

    Ok(Json(models::BlockResponse {
        block: Some(models::Block {
            block_identifier,
            parent_block_identifier,
            timestamp: (block.header.timestamp / 1_000_000).try_into().unwrap(),
            transactions,
        }),
        other_transactions: None,
    }))
}

#[api_v2_operation]
/// Get a Block Transaction
///
/// Get a transaction in a block by its Transaction Identifier. This endpoint
/// should only be used when querying a node for a block does not return all
/// transactions contained within it. All transactions returned by this endpoint
/// must be appended to any transactions returned by the /block method by
/// consumers of this data. Fetching a transaction by hash is considered an
/// Explorer Method (which is classified under the Future Work section). Calling
/// this endpoint requires reference to a BlockIdentifier because transaction
/// parsing can change depending on which block contains the transaction. For
/// example, in Bitcoin it is necessary to know which block contains a
/// transaction to determine the destination of fee payments. Without specifying
/// a block identifier, the node would have to infer which block to use (which
/// could change during a re-org). Implementations that require fetching
/// previous transactions to populate the response (ex: Previous UTXOs in
/// Bitcoin) may find it useful to run a cache within the Rosetta server in the
/// /data directory (on a path that does not conflict with the node).
///
/// NOTE: The current implementation is suboptimal as it processes the whole
/// block to only return a single transaction.
async fn block_transaction_details(
    genesis: web::Data<Arc<Genesis>>,
    client_addr: web::Data<Addr<ClientActor>>,
    view_client_addr: web::Data<Addr<ViewClientActor>>,
    body: Json<models::BlockTransactionRequest>,
) -> Result<Json<models::BlockTransactionResponse>, models::Error> {
    let Json(models::BlockTransactionRequest {
        network_identifier,
        block_identifier,
        transaction_identifier,
    }) = body;

    // TODO: reduce copy-paste
    let status = client_addr
        .send(near_client::Status { is_health_check: false })
        .await?
        .map_err(errors::ErrorKind::InternalError)?;
    if status.chain_id != network_identifier.network {
        return Err(models::Error {
            code: 2,
            message: "Wrong network (chain id)".to_string(),
            retriable: true,
        });
    }

    // TODO: avoid ad-hoc error handling of common use-cases
    let block_id: near_primitives::types::BlockReference =
        block_identifier.try_into().map_err(|_| models::Error {
            code: 4,
            message: "Invalid input".to_string(),
            retriable: true,
        })?;

    let block = view_client_addr
        .send(near_client::GetBlock(block_id.clone()))
        .await?
        .map_err(errors::ErrorKind::NotFound)?;

    let transaction = crate::adapters::collect_transactions(
        Arc::clone(&genesis),
        Addr::clone(&view_client_addr),
        &block,
    )
    .await?
    .into_iter()
    .find(|transaction| transaction.transaction_identifier == transaction_identifier)
    .ok_or_else(|| errors::ErrorKind::NotFound("Transaction not found".into()))?;

    Ok(Json(models::BlockTransactionResponse { transaction }))
}

#[api_v2_operation]
/// Get an Account Balance
///
/// Get an array of all AccountBalances for an AccountIdentifier and the
/// BlockIdentifier at which the balance lookup was performed. The
/// BlockIdentifier must always be returned because some consumers of account
/// balance data need to know specifically at which block the balance was
/// calculated to compare balances they compute from operations with the balance
/// returned by the node. It is important to note that making a balance request
/// for an account without populating the SubAccountIdentifier should not result
/// in the balance of all possible SubAccountIdentifiers being returned. Rather,
/// it should result in the balance pertaining to no SubAccountIdentifiers being
/// returned (sometimes called the liquid balance). To get all balances
/// associated with an account, it may be necessary to perform multiple balance
/// requests with unique AccountIdentifiers. It is also possible to perform a
/// historical balance lookup (if the server supports it) by passing in an
/// optional BlockIdentifier.
async fn account_balance(
    genesis: web::Data<Arc<Genesis>>,
    client_addr: web::Data<Addr<ClientActor>>,
    view_client_addr: web::Data<Addr<ViewClientActor>>,
    body: Json<models::AccountBalanceRequest>,
) -> Result<Json<models::AccountBalanceResponse>, models::Error> {
    let Json(models::AccountBalanceRequest {
        network_identifier,
        block_identifier,
        account_identifier,
    }) = body;

    // TODO: reduce copy-paste
    let status = client_addr
        .send(near_client::Status { is_health_check: false })
        .await?
        .map_err(errors::ErrorKind::InternalError)?;
    if status.chain_id != network_identifier.network {
        return Err(models::Error {
            code: 2,
            message: "Wrong network (chain id)".to_string(),
            retriable: true,
        });
    }

    // TODO: avoid ad-hoc error handling of common use-cases
    let block_id: near_primitives::types::BlockReference = block_identifier
        .map(TryInto::try_into)
        .unwrap_or(Ok(near_primitives::types::BlockReference::Finality(
            near_primitives::types::Finality::Final,
        )))
        .map_err(|_| models::Error {
            code: 4,
            message: "Invalid input".to_string(),
            retriable: true,
        })?;

    // TODO: update error handling once we return structured errors from the
    // view_client handlers
    let block = view_client_addr
        .send(near_client::GetBlock(block_id.clone()))
        .await?
        .map_err(errors::ErrorKind::InternalError)?;

    let query = near_client::Query::new(
        block_id,
        near_primitives::views::QueryRequest::ViewAccount {
            account_id: account_identifier.address,
        },
    );
    let account_info_response = tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            match view_client_addr.send(query.clone()).await? {
                Ok(Some(query_response)) => return Ok(Some(query_response)),
                Ok(None) => {}
                // TODO: update this once we return structured errors from the view_client handlers
                Err(err) => {
                    if err.contains("does not exist") {
                        return Ok(None);
                    }
                    return Err(models::Error::from(errors::ErrorKind::InternalError(err)));
                }
            }
            tokio::time::delay_for(std::time::Duration::from_millis(100)).await;
        }
    })
    .await??;

    let (block_hash, block_height, account_info) = if let Some(account_info_response) =
        account_info_response
    {
        let account_info = match account_info_response.kind {
            near_primitives::views::QueryResponseKind::ViewAccount(account_info) => account_info,
            _ => {
                return Err(errors::ErrorKind::InternalInvariantError(format!(
                    "queried ViewAccount, but received {:?}.",
                    account_info_response.kind
                ))
                .into());
            }
        };
        (account_info_response.block_hash, account_info_response.block_height, account_info)
    } else {
        (
            block.header.hash,
            block.header.height,
            near_primitives::account::Account {
                amount: 0,
                locked: 0,
                storage_usage: 0,
                code_hash: Default::default(),
            }
            .into(),
        )
    };

    let account_balances = crate::utils::RosettaAccountBalances::from_account(
        account_info,
        &genesis.config.runtime_config,
    );

    let balance = if let Some(sub_account) = account_identifier.sub_account {
        match sub_account.address {
            crate::models::SubAccount::Locked => account_balances.locked,
            crate::models::SubAccount::LiquidBalanceForStorage => {
                account_balances.liquid_for_storage
            }
        }
    } else {
        account_balances.liquid
    };

    Ok(Json(models::AccountBalanceResponse {
        block_identifier: models::BlockIdentifier {
            hash: block_hash.to_base(),
            index: block_height.try_into().unwrap(),
        },
        balances: vec![models::Amount::from_yoctonear(balance)],
    }))
}

#[api_v2_operation]
/// Get All Mempool Transactions (not implemented)
///
/// Get all Transaction Identifiers in the mempool
///
/// NOTE: The mempool is short-lived, so it is currently not implemented.
async fn mempool(
    _client_addr: web::Data<Addr<ClientActor>>,
    _body: Json<models::NetworkRequest>,
) -> Result<Json<models::MempoolResponse>, models::Error> {
    Ok(Json(models::MempoolResponse { transaction_identifiers: vec![] }))
}

#[api_v2_operation]
/// Get a Mempool Transaction (not implemented)
///
/// Get a transaction in the mempool by its Transaction Identifier. This is a
/// separate request than fetching a block transaction (/block/transaction)
/// because some blockchain nodes need to know that a transaction query is for
/// something in the mempool instead of a transaction in a block. Transactions
/// may not be fully parsable until they are in a block (ex: may not be possible
/// to determine the fee to pay before a transaction is executed). On this
/// endpoint, it is ok that returned transactions are only estimates of what may
/// actually be included in a block.
///
/// NOTE: The mempool is short-lived, so this method does not make a lot of
/// sense to be implemented.
async fn mempool_transaction(
    _client_addr: web::Data<Addr<ClientActor>>,
    _body: Json<models::MempoolTransactionRequest>,
) -> Result<Json<models::MempoolTransactionResponse>, models::Error> {
    Err(errors::ErrorKind::InternalError("Not implemented yet".to_string()).into())
}

#[api_v2_operation]
/// Derive an Address from a PublicKey (not implemented by design)
///
/// Derive returns the network-specific address associated with a public key.
///
/// Blockchains that require an on-chain action to create an account should not
/// implement this method.
async fn construction_derive() -> Result<Json<()>, models::Error> {
    Err(errors::ErrorKind::InternalError("Not implemented by design".to_string()).into())
}

#[api_v2_operation]
/// Create a Request to Fetch Metadata (offline API)
///
/// Preprocess is called prior to /construction/payloads to construct a request
/// for any metadata that is needed for transaction construction given (i.e.
/// account nonce). The request returned from this method will be used by the
/// caller (in a different execution environment) to call the
/// /construction/metadata endpoint.
async fn construction_preprocess(
    client_addr: web::Data<Addr<ClientActor>>,
    body: Json<models::ConstructionPreprocessRequest>,
) -> Result<Json<models::ConstructionPreprocessResponse>, models::Error> {
    let Json(models::ConstructionPreprocessRequest { network_identifier, operations }) = body;

    // TODO: reduce copy-paste
    let status = client_addr
        .send(near_client::Status { is_health_check: false })
        .await?
        .map_err(errors::ErrorKind::InternalError)?;
    if status.chain_id != network_identifier.network {
        return Err(models::Error {
            code: 2,
            message: "Wrong network (chain id)".to_string(),
            retriable: true,
        });
    }

    let mut sender_account_id: Option<String> = None;
    let mut receiver_account_id: Option<String> = None;
    let mut operations = operations.iter().rev();
    while let Some(receiver_operation) = operations.next() {
        match receiver_operation.type_ {
            models::OperationType::Transfer => {
                let receiver_amount = receiver_operation.amount.as_ref().ok_or_else(|| {
                    errors::ErrorKind::InvalidInput(
                        "TRANSFER operations must specify `amount`".to_string(),
                    )
                })?;
                if !receiver_amount.value.is_positive() {
                    return Err(errors::ErrorKind::InvalidInput(
                        "Receiver TRANSFER operations must have positive `amount`".to_string(),
                    )
                    .into());
                }
                if let Some(ref receiver_account_id) = receiver_account_id {
                    if &receiver_operation.account.address != receiver_account_id {
                        return Err(errors::ErrorKind::InvalidInput(
                            format!("A single transaction cannot be send to multiple receipients ('{}' and '{}')", receiver_operation.account.address, receiver_account_id)
                        ).into());
                    }
                } else {
                    receiver_account_id = Some(receiver_operation.account.address.clone());
                }
                let sender_operation = operations.next().ok_or_else(|| {
                    errors::ErrorKind::InvalidInput(
                        "Source TRANSFER operation is missing".to_string(),
                    )
                })?;

                let sender_amount = sender_operation.amount.as_ref().ok_or_else(|| {
                    errors::ErrorKind::InvalidInput(
                        "TRANSFER operations must specify `amount`".to_string(),
                    )
                })?;
                if -sender_amount.value != receiver_amount.value {
                    return Err(errors::ErrorKind::InvalidInput(
                        "The sum of amounts of Sender and Receiver TRANSFER operations must be zero"
                            .to_string(),
                    )
                    .into());
                }
                if let Some(sender_account_id) = sender_account_id.as_ref() {
                    if &sender_operation.account.address != sender_account_id {
                        return Err(errors::ErrorKind::InvalidInput(
                            format!("A single transaction cannot be send from multiple senders ('{}' and '{}')", sender_operation.account.address, sender_account_id)
                        ).into());
                    }
                } else {
                    sender_account_id = Some(sender_operation.account.address.clone());
                }
            }
            _ => unimplemented!(),
        }
    }

    let signer_account_id = sender_account_id.ok_or_else(|| {
        errors::ErrorKind::InvalidInput(
            "There are no operations specifying signer [sender] account".to_string(),
        )
    })?;
    Ok(Json(models::ConstructionPreprocessResponse {
        required_public_keys: vec![models::AccountIdentifier {
            address: signer_account_id.clone(),
            sub_account: None,
        }],
        options: models::ConstructionMetadataOptions { signer_account_id },
    }))
}

#[api_v2_operation]
/// Get Metadata for Transaction Construction (online API)
///
/// Get any information required to construct a transaction for a specific
/// network. Metadata returned here could be a recent hash to use, an account
/// sequence number, or even arbitrary chain state. The request used when
/// calling this endpoint is often created by calling /construction/preprocess
/// in an offline environment. It is important to clarify that this endpoint
/// should not pre-construct any transactions for the client (this should happen
/// in /construction/payloads). This endpoint is left purposely unstructured
/// because of the wide scope of metadata that could be required.
async fn construction_metadata(
    client_addr: web::Data<Addr<ClientActor>>,
    view_client_addr: web::Data<Addr<ViewClientActor>>,
    body: Json<models::ConstructionMetadataRequest>,
) -> Result<Json<models::ConstructionMetadataResponse>, models::Error> {
    let Json(models::ConstructionMetadataRequest { network_identifier, options, public_keys }) =
        body;

    let signer_public_access_key = public_keys.into_iter().next().ok_or_else(|| {
        errors::ErrorKind::InvalidInput("exactly one public key is expected".to_string())
    })?;

    // TODO: reduce copy-paste
    let status = client_addr
        .send(near_client::Status { is_health_check: false })
        .await?
        .map_err(errors::ErrorKind::InternalError)?;
    if status.chain_id != network_identifier.network {
        return Err(models::Error {
            code: 2,
            message: "Wrong network (chain id)".to_string(),
            retriable: true,
        });
    }

    let access_key_query = near_client::Query::new(
        near_primitives::types::BlockReference::latest(),
        near_primitives::views::QueryRequest::ViewAccessKey {
            account_id: options.signer_account_id.clone(),
            public_key: (&signer_public_access_key).try_into().map_err(|err| {
                errors::ErrorKind::InvalidInput(format!(
                    "public key could not be parsed due to: {:?}",
                    err
                ))
            })?,
        },
    );

    let access_key_query_response =
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            loop {
                match view_client_addr.send(access_key_query.clone()).await? {
                    Ok(Some(query_response)) => return Ok(query_response),
                    Ok(None) => {}
                    // TODO: update this once we return structured errors in the
                    // view_client handlers
                    Err(err) => {
                        if err.contains("does not exist") {
                            return Err(crate::errors::ErrorKind::NotFound(err));
                        }
                        return Err(crate::errors::ErrorKind::InternalError(err));
                    }
                }
                tokio::time::delay_for(std::time::Duration::from_millis(100)).await;
            }
        })
        .await??;

    let access_key = if let near_primitives::views::QueryResponseKind::AccessKey(access_key) =
        access_key_query_response.kind
    {
        access_key
    } else {
        return Err(crate::errors::ErrorKind::InternalInvariantError(
            "queried ViewAccessKey, but received something else.".to_string(),
        )
        .into());
    };
    Ok(Json(models::ConstructionMetadataResponse {
        metadata: models::ConstructionMetadata {
            recent_block_hash: access_key_query_response.block_hash.to_base(),
            signer_account_id: options.signer_account_id,
            signer_public_access_key_nonce: access_key.nonce,
        },
    }))
}

#[api_v2_operation]
/// Generate an Unsigned Transaction and Signing Payloads (offline API)
///
/// Payloads is called with an array of operations and the response from
/// `/construction/metadata`. It returns an unsigned transaction blob and a
/// collection of payloads that must be signed by particular addresses using a
/// certain SignatureType. The array of operations provided in transaction
/// construction often times can not specify all "effects" of a transaction
/// (consider invoked transactions in Ethereum). However, they can
/// deterministically specify the "intent" of the transaction, which is
/// sufficient for construction. For this reason, parsing the corresponding
/// transaction in the Data API (when it lands on chain) will contain a superset
/// of whatever operations were provided during construction.
async fn construction_payloads(
    client_addr: web::Data<Addr<ClientActor>>,
    body: Json<models::ConstructionPayloadsRequest>,
) -> Result<Json<models::ConstructionPayloadsResponse>, models::Error> {
    let Json(models::ConstructionPayloadsRequest {
        network_identifier,
        operations,
        public_keys,
        metadata,
    }) = body;

    let signer_public_access_key = public_keys.into_iter().next().ok_or_else(|| {
        errors::ErrorKind::InvalidInput("exactly one public key is expected".to_string())
    })?;

    // TODO: reduce copy-paste
    let status = client_addr
        .send(near_client::Status { is_health_check: false })
        .await?
        .map_err(errors::ErrorKind::InternalError)?;
    if status.chain_id != network_identifier.network {
        return Err(models::Error {
            code: 2,
            message: "Wrong network (chain id)".to_string(),
            retriable: true,
        });
    }

    let models::ConstructionMetadata {
        recent_block_hash,
        signer_account_id,
        signer_public_access_key_nonce,
    } = metadata;
    let unsigned_transaction = near_primitives::transaction::Transaction {
        block_hash: recent_block_hash.try_into().map_err(|err| {
            errors::ErrorKind::InvalidInput(format!(
                "block hash could not be parsed due to: {:?}",
                err
            ))
        })?,
        signer_id: signer_account_id.clone(),
        public_key: (&signer_public_access_key).try_into().map_err(|err| {
            errors::ErrorKind::InvalidInput(format!(
                "public key could not be parsed due to: {:?}",
                err
            ))
        })?,
        nonce: signer_public_access_key_nonce,
        // TODO
        receiver_id: "near.test".to_string(),
        actions: vec![near_primitives::transaction::Action::Transfer(
            near_primitives::transaction::TransferAction {
                deposit: 1_000_000_000_000_000_000_000_000,
            },
        )],
    };

    let unsigned_transaction_blob: crate::utils::BorshInHexString<
        near_primitives::transaction::Transaction,
    > = unsigned_transaction.into();
    Ok(Json(models::ConstructionPayloadsResponse {
        unsigned_transaction: unsigned_transaction_blob.clone(),
        payloads: vec![models::SigningPayload {
            address: signer_account_id,
            signature_type: None,
            hex_bytes: unsigned_transaction_blob,
        }],
    }))
}

#[api_v2_operation]
/// Create Network Transaction from Signatures (offline API)
///
/// Combine creates a network-specific transaction from an unsigned transaction
/// and an array of provided signatures. The signed transaction returned from
/// this method will be sent to the /construction/submit endpoint by the caller.
async fn construction_combine(
    client_addr: web::Data<Addr<ClientActor>>,
    body: Json<models::ConstructionCombineRequest>,
) -> Result<Json<models::ConstructionCombineResponse>, models::Error> {
    let Json(models::ConstructionCombineRequest {
        network_identifier,
        unsigned_transaction,
        signatures,
    }) = body;

    let signature = signatures
        .iter()
        .next()
        .ok_or_else(|| {
            errors::ErrorKind::InvalidInput("exactly one signature is expected".to_string())
        })?
        .try_into()
        .map_err(|err: near_crypto::ParseSignatureError| {
            errors::ErrorKind::InvalidInput(err.to_string())
        })?;

    // TODO: reduce copy-paste
    let status = client_addr
        .send(near_client::Status { is_health_check: false })
        .await?
        .map_err(errors::ErrorKind::InternalError)?;
    if status.chain_id != network_identifier.network {
        return Err(models::Error {
            code: 2,
            message: "Wrong network (chain id)".to_string(),
            retriable: true,
        });
    }

    let signed_transction = near_primitives::transaction::SignedTransaction::new(
        signature,
        unsigned_transaction.into_inner(),
    );

    Ok(Json(models::ConstructionCombineResponse { signed_transaction: signed_transction.into() }))
}

#[api_v2_operation]
/// Parse a Transaction (offline API)
///
/// Parse is called on both unsigned and signed transactions to understand the
/// intent of the formulated transaction. This is run as a sanity check before
/// signing (after /construction/payloads) and before broadcast (after
/// /construction/combine).
async fn construction_parse(
    client_addr: web::Data<Addr<ClientActor>>,
    body: Json<models::ConstructionParseRequest>,
) -> Result<Json<models::ConstructionParseResponse>, models::Error> {
    let Json(models::ConstructionParseRequest { network_identifier, transaction, signed }) = body;

    // TODO: reduce copy-paste
    let status = client_addr
        .send(near_client::Status { is_health_check: false })
        .await?
        .map_err(errors::ErrorKind::InternalError)?;
    if status.chain_id != network_identifier.network {
        return Err(models::Error {
            code: 2,
            message: "Wrong network (chain id)".to_string(),
            retriable: true,
        });
    }

    unimplemented!()
    //Ok(Json(models::ConstructionParseResponse { signers, operations }))
}

#[api_v2_operation]
/// Get the Hash of a Signed Transaction
///
/// TransactionHash returns the network-specific transaction hash for a signed
/// transaction.
async fn construction_hash(
    client_addr: web::Data<Addr<ClientActor>>,
    body: Json<models::ConstructionHashRequest>,
) -> Result<Json<models::TransactionIdentifierResponse>, models::Error> {
    let Json(models::ConstructionHashRequest { network_identifier, signed_transaction }) = body;

    // TODO: reduce copy-paste
    let status = client_addr
        .send(near_client::Status { is_health_check: false })
        .await?
        .map_err(errors::ErrorKind::InternalError)?;
    if status.chain_id != network_identifier.network {
        return Err(models::Error {
            code: 2,
            message: "Wrong network (chain id)".to_string(),
            retriable: true,
        });
    }

    Ok(Json(models::TransactionIdentifierResponse {
        transaction_identifier: models::TransactionIdentifier {
            hash: signed_transaction.as_ref().get_hash().to_base(),
        },
    }))
}

#[api_v2_operation]
/// Submit a Signed Transaction
///
/// Submit a pre-signed transaction to the node. This call should not block on
/// the transaction being included in a block. Rather, it should return
/// immediately with an indication of whether or not the transaction was
/// included in the mempool. The transaction submission response should only
/// return a 200 status if the submitted transaction could be included in the
/// mempool. Otherwise, it should return an error.
async fn construction_submit(
    client_addr: web::Data<Addr<ClientActor>>,
    body: Json<models::ConstructionSubmitRequest>,
) -> Result<Json<models::TransactionIdentifierResponse>, models::Error> {
    let Json(models::ConstructionSubmitRequest { network_identifier, signed_transaction }) = body;

    // TODO: reduce copy-paste
    let status = client_addr
        .send(near_client::Status { is_health_check: false })
        .await?
        .map_err(errors::ErrorKind::InternalError)?;
    if status.chain_id != network_identifier.network {
        return Err(models::Error {
            code: 2,
            message: "Wrong network (chain id)".to_string(),
            retriable: true,
        });
    }

    let transaction_hash = signed_transaction.as_ref().get_hash().to_base();
    let transaction_submittion = client_addr
        .send(near_network::NetworkClientMessages::Transaction {
            transaction: signed_transaction.into_inner(),
            is_forwarded: false,
            check_only: false,
        })
        .await?;
    match transaction_submittion {
        near_network::NetworkClientResponses::ValidTx
        | near_network::NetworkClientResponses::RequestRouted => {
            Ok(Json(models::TransactionIdentifierResponse {
                transaction_identifier: models::TransactionIdentifier { hash: transaction_hash },
            }))
        }
        near_network::NetworkClientResponses::InvalidTx(error) => {
            Err(errors::ErrorKind::InvalidInput(error.to_string()).into())
        }
        _ => Err(errors::ErrorKind::InternalInvariantError(format!(
            "Transaction submition return unexpected result: {:?}",
            transaction_submittion
        ))
        .into()),
    }
}

fn get_cors(cors_allowed_origins: &[String]) -> CorsFactory {
    let mut cors = Cors::new();
    if cors_allowed_origins != ["*".to_string()] {
        for origin in cors_allowed_origins {
            cors = cors.allowed_origin(&origin);
        }
    }
    cors.allowed_methods(vec!["GET", "POST"])
        .allowed_headers(vec![
            actix_web::http::header::AUTHORIZATION,
            actix_web::http::header::ACCEPT,
        ])
        .allowed_header(actix_web::http::header::CONTENT_TYPE)
        .max_age(3600)
        .finish()
}

pub fn start_rosetta_rpc(
    config: crate::config::RosettaRpcConfig,
    genesis: Arc<Genesis>,
    client_addr: Addr<ClientActor>,
    view_client_addr: Addr<ViewClientActor>,
) {
    let crate::config::RosettaRpcConfig { addr, cors_allowed_origins } = config;
    HttpServer::new(move || {
        App::new()
            .data(Arc::clone(&genesis))
            .data(client_addr.clone())
            .data(view_client_addr.clone())
            .wrap(get_cors(&cors_allowed_origins))
            .wrap_api()
            .service(web::resource("/network/list").route(web::post().to(network_list)))
            .service(web::resource("/network/status").route(web::post().to(network_status)))
            .service(web::resource("/network/options").route(web::post().to(network_options)))
            .service(web::resource("/block").route(web::post().to(block_details)))
            .service(
                web::resource("/block/transaction")
                    .route(web::post().to(block_transaction_details)),
            )
            .service(web::resource("/account/balance").route(web::post().to(account_balance)))
            .service(
                web::resource("/construction/submit").route(web::post().to(construction_submit)),
            )
            .service(web::resource("/mempool").route(web::post().to(mempool)))
            .service(
                web::resource("/mempool/transaction").route(web::post().to(mempool_transaction)),
            )
            .service(
                web::resource("/construction/derive").route(web::post().to(construction_derive)),
            )
            .service(
                web::resource("/construction/preprocess")
                    .route(web::post().to(construction_preprocess)),
            )
            .service(
                web::resource("/construction/metadata")
                    .route(web::post().to(construction_metadata)),
            )
            .service(
                web::resource("/construction/payloads")
                    .route(web::post().to(construction_payloads)),
            )
            .service(
                web::resource("/construction/combine").route(web::post().to(construction_combine)),
            )
            .service(web::resource("/construction/parse").route(web::post().to(construction_parse)))
            .service(web::resource("/construction/hash").route(web::post().to(construction_hash)))
            .with_json_spec_at("/api/spec")
            .build()
    })
    .bind(addr)
    .unwrap()
    .run();
}
