#![doc = include_str!("../README.md")]

use std::convert::AsRef;
use std::sync::Arc;

use actix::Addr;
use axum::Router;
use axum::extract::{Json, State};
use axum::http::HeaderValue;
use axum::http::header::{ACCEPT, AUTHORIZATION, CONTENT_TYPE};
use axum::http::Method;
use axum::routing::post;
use tower_http::cors::CorsLayer;
use tower_http::limit::RequestBodyLimitLayer;
use utoipa::OpenApi;
// use utoipa_swagger_ui::SwaggerUi; // TODO: Re-enable when SwaggerUI integration is fixed
use strum::IntoEnumIterator;

pub use config::RosettaRpcConfig;
use near_async::futures::{FutureSpawner, FutureSpawnerExt};
use near_async::messaging::CanSendAsync;
use near_async::tokio::TokioRuntimeHandle;
use near_chain_configs::Genesis;
use near_client::client_actor::ClientActorInner;
use near_client::{RpcHandlerActor, ViewClientActor};
use near_o11y::span_wrapped_msg::SpanWrappedMessageExt;
use near_primitives::{account::AccountContract, borsh::BorshDeserialize};
use near_o11y::tracing::{info, error};

mod adapters;
mod config;
mod errors;
mod models;
pub mod test;
mod types;
mod utils;

pub const BASE_PATH: &str = "";
pub const API_VERSION: &str = "1.4.4";
pub const BLOCKCHAIN: &str = "nearprotocol";

#[derive(Clone)]
struct AppState {
    genesis: Arc<GenesisWithIdentifier>,
    client_addr: TokioRuntimeHandle<ClientActorInner>,
    view_client_addr: Addr<ViewClientActor>,
    tx_handler_addr: Addr<RpcHandlerActor>,
    currencies: Option<Vec<models::Currency>>,
}

/// Genesis together with genesis block identifier.
struct GenesisWithIdentifier {
    genesis: Genesis,
    block_id: models::BlockIdentifier,
}

/// Verifies that network identifier provided by the user is what we expect.
///
/// `blockchain` and `network` must match and `sub_network_identifier` must not
/// be provided.  On success returns client actor’s status response.
async fn check_network_identifier(
    client_addr: &TokioRuntimeHandle<ClientActorInner>,
    identifier: models::NetworkIdentifier,
) -> Result<near_client::StatusResponse, errors::ErrorKind> {
    if identifier.blockchain != BLOCKCHAIN {
        return Err(errors::ErrorKind::WrongNetwork(format!(
            "Invalid blockchain {}, expecting {}",
            identifier.blockchain, BLOCKCHAIN
        )));
    }

    if identifier.sub_network_identifier.is_some() {
        return Err(errors::ErrorKind::WrongNetwork("Unexpected sub_network_identifier".into()));
    }

    let status = client_addr
        .send_async(near_client::Status { is_health_check: false, detailed: false }.span_wrap())
        .await?
        .map_err(|err| errors::ErrorKind::InternalError(err.to_string()))?;
    if status.chain_id != identifier.network {
        return Err(errors::ErrorKind::WrongNetwork(format!(
            "Invalid network {}, expecting {}",
            identifier.network, status.chain_id
        )));
    }

    Ok(status)
}

/// Get List of Available Networks
///
/// This endpoint returns a list of NetworkIdentifiers that the Rosetta server
/// supports.
#[utoipa::path(
    post,
    path = "/network/list",
    request_body = models::MetadataRequest,
    responses(
        (status = 200, description = "List of available networks", body = models::NetworkListResponse),
        (status = 500, description = "Internal server error", body = models::Error)
    )
)]
async fn network_list(
    State(app_state): State<AppState>,
    Json(_body): Json<models::MetadataRequest>,
) -> Result<Json<models::NetworkListResponse>, models::Error> {
    let client_addr = &app_state.client_addr;
    let status = client_addr
        .send_async(near_client::Status { is_health_check: false, detailed: false }.span_wrap())
        .await?
        .map_err(|err| errors::ErrorKind::InternalError(err.to_string()))?;
    Ok(Json(models::NetworkListResponse {
        network_identifiers: vec![models::NetworkIdentifier {
            blockchain: BLOCKCHAIN.to_string(),
            network: status.chain_id,
            sub_network_identifier: None,
        }],
    }))
}

/// Get Network Status
///
/// This endpoint returns the current status of the network requested. Any
/// NetworkIdentifier returned by /network/list should be accessible here.
#[utoipa::path(
    post,
    path = "/network/status",
    request_body = models::NetworkRequest,
    responses(
        (status = 200, description = "Network status information", body = models::NetworkStatusResponse),
        (status = 500, description = "Internal server error", body = models::Error)
    )
)]
async fn network_status(
    State(app_state): State<AppState>,
    Json(models::NetworkRequest { network_identifier }): Json<models::NetworkRequest>,
) -> Result<Json<models::NetworkStatusResponse>, models::Error> {
    let client_addr = &app_state.client_addr;
    let view_client_addr = &app_state.view_client_addr;
    let genesis = &app_state.genesis;

    let status = check_network_identifier(client_addr, network_identifier).await?;

    let (network_info, earliest_block) = tokio::try_join!(
        client_addr.send_async(near_client::GetNetworkInfo {}.span_wrap()),
        near_async::messaging::SendAsync::send_async(
            view_client_addr,
            near_client::GetBlock(near_primitives::types::BlockReference::SyncCheckpoint(
                near_primitives::types::SyncCheckpoint::EarliestAvailable
            )),
        ),
    )?;
    let network_info = network_info.map_err(errors::ErrorKind::InternalError)?;
    let genesis_block_identifier = genesis.block_id.clone();
    let oldest_block_identifier: models::BlockIdentifier = earliest_block
        .ok()
        .map(|block| (&block).into())
        .unwrap_or_else(|| genesis_block_identifier.clone());

    let final_block = crate::utils::get_final_block(view_client_addr).await?;
    Ok(Json(models::NetworkStatusResponse {
        current_block_identifier: (&final_block).into(),
        current_block_timestamp: i64::try_from(final_block.header.timestamp_nanosec / 1_000_000)
            .unwrap(),
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
            .connected_peers
            .into_iter()
            .map(|peer| models::Peer { peer_id: peer.id.to_string() })
            .collect(),
    }))
}

/// Get Network Options
///
/// This endpoint returns the version information and allowed network-specific
/// types for a NetworkIdentifier. Any NetworkIdentifier returned by
/// /network/list should be accessible here. Because options are retrievable in
/// the context of a NetworkIdentifier, it is possible to define unique options
/// for each network.
#[utoipa::path(
    post,
    path = "/network/options",
    request_body = models::NetworkRequest,
    responses(
        (status = 200, description = "Network options and version information", body = models::NetworkOptionsResponse),
        (status = 500, description = "Internal server error", body = models::Error)
    )
)]
async fn network_options(
    State(app_state): State<AppState>,
    Json(models::NetworkRequest { network_identifier }): Json<models::NetworkRequest>,
) -> Result<Json<models::NetworkOptionsResponse>, models::Error> {
    let client_addr = &app_state.client_addr;

    let status = check_network_identifier(client_addr, network_identifier).await?;

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
    State(app_state): State<AppState>,
    Json(models::BlockRequest { network_identifier, block_identifier }): Json<models::BlockRequest>,
) -> Result<Json<models::BlockResponse>, models::Error> {
    let genesis = &app_state.genesis;
    let client_addr = &app_state.client_addr;
    let view_client_addr = &app_state.view_client_addr;
    let currencies = &app_state.currencies;

    check_network_identifier(client_addr, network_identifier).await?;

    let block_id: near_primitives::types::BlockReference = block_identifier.try_into()?;
    let block = crate::utils::get_block_if_final(&block_id, view_client_addr)
        .await?
        .ok_or_else(|| errors::ErrorKind::NotFound("Block not found".into()))?;

    let block_identifier: models::BlockIdentifier = (&block).into();

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
            .map_err(|err| errors::ErrorKind::InternalError(err.to_string()))?;
        (&parent_block).into()
    };

    let transactions = crate::adapters::collect_transactions(
        &genesis.genesis,
        view_client_addr,
        &block,
        currencies,
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

/// cspell:ignore UTXOs
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
    State(app_state): State<AppState>,
    Json(models::BlockTransactionRequest {
        network_identifier,
        block_identifier,
        transaction_identifier,
    }): Json<models::BlockTransactionRequest>,
) -> Result<Json<models::BlockTransactionResponse>, models::Error> {
    let genesis = &app_state.genesis;
    let client_addr = &app_state.client_addr;
    let view_client_addr = &app_state.view_client_addr;
    let currencies = &app_state.currencies;

    check_network_identifier(client_addr, network_identifier).await?;

    let block_id: near_primitives::types::BlockReference = block_identifier.try_into()?;

    let block = crate::utils::get_block_if_final(&block_id, view_client_addr)
        .await?
        .ok_or_else(|| errors::ErrorKind::NotFound("Block not found".into()))?;

    let transaction = crate::adapters::collect_transactions(
        &genesis.genesis,
        view_client_addr,
        &block,
        currencies,
    )
    .await?
    .into_iter()
    .find(|transaction| transaction.transaction_identifier == transaction_identifier)
    .ok_or_else(|| errors::ErrorKind::NotFound("Transaction not found".into()))?;

    Ok(Json(models::BlockTransactionResponse { transaction }))
}

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
    State(app_state): State<AppState>,
    Json(models::AccountBalanceRequest {
        network_identifier,
        block_identifier,
        account_identifier,
        currencies,
    }): Json<models::AccountBalanceRequest>,
) -> Result<Json<models::AccountBalanceResponse>, models::Error> {
    let client_addr = &app_state.client_addr;
    let view_client_addr = &app_state.view_client_addr;
    let config_currencies = &app_state.currencies;

    check_network_identifier(client_addr, network_identifier).await?;

    let block_id: near_primitives::types::BlockReference = block_identifier
        .map(TryInto::try_into)
        .unwrap_or(Ok(near_primitives::types::BlockReference::Finality(
            near_primitives::types::Finality::Final,
        )))?;

    // TODO: update error handling once we return structured errors from the
    // view_client handlers
    let block = crate::utils::get_block_if_final(&block_id, view_client_addr)
        .await?
        .ok_or_else(|| errors::ErrorKind::NotFound("Block not found".into()))?;

    let runtime_config =
        crate::utils::query_protocol_config(block.header.hash, view_client_addr)
            .await?
            .runtime_config;

    let account_id_for_access_key = account_identifier.address.clone();
    let account_identifier_for_ft = account_identifier.clone();
    let account_id = account_identifier.address.into();
    let (block_hash, block_height, account_info) =
        match crate::utils::query_account(block_id, account_id, view_client_addr).await {
            Ok(account_info_response) => account_info_response,
            Err(crate::errors::ErrorKind::NotFound(_)) => (
                block.header.hash,
                block.header.height,
                near_primitives::account::Account::new(0, 0, AccountContract::None, 0).into(),
            ),
            Err(err) => return Err(err.into()),
        };

    let account_balances =
        crate::utils::RosettaAccountBalances::from_account(account_info, &runtime_config);

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
    let nonces = if let Some(metadata) = account_identifier.metadata {
        Some(
            crate::utils::get_nonces(
                view_client_addr,
                account_id_for_access_key,
                metadata.public_keys,
            )
            .await?,
        )
    } else {
        None
    };
    if let Some(currencies) = currencies {
        let mut balances: Vec<models::Amount> = Vec::default();
        for currency in currencies {
            let ft_balance = crate::adapters::nep141::get_fungible_token_balance_for_account(
                view_client_addr,
                &block.header,
                &currency
                    .clone()
                    .metadata
                    .or_else(|| {
                        // retrieve contract address from global config if not provided in query
                        config_currencies.as_ref().and_then(|currencies| {
                            currencies.iter().find_map(|c| {
                                if c.symbol == currency.symbol { c.metadata.clone() } else { None }
                            })
                        })
                    })
                    .ok_or_else(|| {
                        errors::ErrorKind::NotFound(format!(
                            "Unknown currency `{}`, try providing the contract address",
                            currency.symbol
                        ))
                    })?
                    .contract_address
                    .clone(),
                &account_identifier_for_ft,
            )
            .await?;
            balances.push(models::Amount::from_fungible_token(ft_balance, currency))
        }
        balances.push(models::Amount::from_yoctonear(balance));
        Ok(Json(models::AccountBalanceResponse {
            block_identifier: models::BlockIdentifier::new(block_height, &block_hash),
            balances,
            metadata: nonces,
        }))
    } else {
        Ok(Json(models::AccountBalanceResponse {
            block_identifier: models::BlockIdentifier::new(block_height, &block_hash),
            balances: vec![models::Amount::from_yoctonear(balance)],
            metadata: nonces,
        }))
    }
}

/// Get All Mempool Transactions (not implemented)
///
/// Get all Transaction Identifiers in the mempool
///
/// NOTE: The mempool is short-lived, so it is currently not implemented.
async fn mempool(
    State(_app_state): State<AppState>,
    Json(_network_request): Json<models::NetworkRequest>,
) -> Result<Json<models::MempoolResponse>, models::Error> {
    Ok(Json(models::MempoolResponse { transaction_identifiers: vec![] }))
}

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
    State(_app_state): State<AppState>,
    Json(_mempool_transaction_request): Json<models::MempoolTransactionRequest>,
) -> Result<Json<models::MempoolTransactionResponse>, models::Error> {
    Err(errors::ErrorKind::InternalError("Not implemented yet".to_string()).into())
}

/// Derive an Address from a PublicKey (offline API, only for implicit accounts)
///
/// Derive returns the network-specific address associated with a public key.
///
/// Blockchains that require an on-chain action to create an account should not
/// implement this method.
///
/// NEAR implements explicit accounts with CREATE_ACCOUNT action and implicit
/// accounts, where account id is just a hex of the public key.
async fn construction_derive(
    State(app_state): State<AppState>,
    Json(models::ConstructionDeriveRequest { network_identifier, public_key }): Json<models::ConstructionDeriveRequest>,
) -> Result<Json<models::ConstructionDeriveResponse>, models::Error> {
    let client_addr = &app_state.client_addr;

    check_network_identifier(client_addr, network_identifier).await?;

    let public_key: near_crypto::PublicKey = (&public_key)
        .try_into()
        .map_err(|_| errors::ErrorKind::InvalidInput("Invalid PublicKey".to_string()))?;
    let address = if let near_crypto::KeyType::ED25519 = public_key.key_type() {
        hex::encode(public_key.key_data())
    } else {
        return Err(errors::ErrorKind::InvalidInput(
            "Only Ed25519 keys are allowed for implicit accounts".to_string(),
        )
        .into());
    };

    Ok(Json(models::ConstructionDeriveResponse {
        account_identifier: models::AccountIdentifier {
            address: address.parse().unwrap(),
            sub_account: None,
            metadata: None,
        },
    }))
}

/// Create a Request to Fetch Metadata (offline API)
///
/// Preprocess is called prior to /construction/payloads to construct a request
/// for any metadata that is needed for transaction construction given (i.e.
/// account nonce). The request returned from this method will be used by the
/// caller (in a different execution environment) to call the
/// /construction/metadata endpoint.
async fn construction_preprocess(
    State(app_state): State<AppState>,
    Json(models::ConstructionPreprocessRequest { network_identifier, operations }): Json<models::ConstructionPreprocessRequest>,
) -> Result<Json<models::ConstructionPreprocessResponse>, models::Error> {
    let client_addr = &app_state.client_addr;

    check_network_identifier(client_addr, network_identifier).await?;

    let near_actions: crate::adapters::NearActions = operations.try_into()?;

    Ok(Json(models::ConstructionPreprocessResponse {
        required_public_keys: vec![models::AccountIdentifier {
            address: near_actions.sender_account_id.clone().into(),
            sub_account: None,
            metadata: None,
        }],
        options: models::ConstructionMetadataOptions {
            signer_account_id: near_actions.sender_account_id.into(),
        },
    }))
}

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
    State(app_state): State<AppState>,
    Json(models::ConstructionMetadataRequest { network_identifier, options, public_keys }): Json<models::ConstructionMetadataRequest>,
) -> Result<Json<models::ConstructionMetadataResponse>, models::Error> {
    let client_addr = &app_state.client_addr;
    let view_client_addr = &app_state.view_client_addr;

    check_network_identifier(client_addr, network_identifier).await?;

    let signer_public_access_key = public_keys.into_iter().next().ok_or_else(|| {
        errors::ErrorKind::InvalidInput("exactly one public key is expected".to_string())
    })?;

    let (block_hash, _block_height, access_key) = crate::utils::query_access_key(
        near_primitives::types::BlockReference::latest(),
        options.signer_account_id.into(),
        (&signer_public_access_key).try_into().map_err(|err| {
            errors::ErrorKind::InvalidInput(format!(
                "public key could not be parsed due to: {:?}",
                err
            ))
        })?,
        view_client_addr,
    )
    .await?;

    Ok(Json(models::ConstructionMetadataResponse {
        metadata: models::ConstructionMetadata {
            recent_block_hash: block_hash.to_string(),
            signer_public_access_key_nonce: access_key.nonce.saturating_add(1),
        },
    }))
}

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
    State(app_state): State<AppState>,
    Json(models::ConstructionPayloadsRequest {
        network_identifier,
        operations,
        public_keys,
        metadata,
    }): Json<models::ConstructionPayloadsRequest>,
) -> Result<Json<models::ConstructionPayloadsResponse>, models::Error> {
    let client_addr = &app_state.client_addr;

    check_network_identifier(client_addr, network_identifier).await?;

    let signer_public_access_key: near_crypto::PublicKey = public_keys
        .first()
        .ok_or_else(|| {
            errors::ErrorKind::InvalidInput("exactly one public key is expected".to_string())
        })?
        .try_into()
        .map_err(|err| {
            errors::ErrorKind::InvalidInput(format!(
                "public key could not be parsed due to: {:?}",
                err
            ))
        })?;

    let crate::adapters::NearActions {
        sender_account_id: signer_account_id,
        receiver_account_id,
        actions,
    } = operations.try_into()?;
    let models::ConstructionMetadata { recent_block_hash, signer_public_access_key_nonce } =
        metadata;
    let unsigned_transaction = near_primitives::transaction::Transaction::V0(
        near_primitives::transaction::TransactionV0 {
            block_hash: recent_block_hash.parse().map_err(|err| {
                errors::ErrorKind::InvalidInput(format!(
                    "block hash could not be parsed due to: {:?}",
                    err
                ))
            })?,
            signer_id: signer_account_id.clone(),
            public_key: signer_public_access_key.clone(),
            nonce: signer_public_access_key_nonce,
            receiver_id: receiver_account_id,
            actions,
        },
    );

    let (transaction_hash, _) = unsigned_transaction.get_hash_and_size();

    Ok(Json(models::ConstructionPayloadsResponse {
        unsigned_transaction: unsigned_transaction.into(),
        payloads: vec![models::SigningPayload {
            account_identifier: signer_account_id.into(),
            signature_type: Some(signer_public_access_key.key_type().into()),
            hex_bytes: transaction_hash.as_ref().to_owned().into(),
        }],
    }))
}

/// Create Network Transaction from Signatures (offline API)
///
/// Combine creates a network-specific transaction from an unsigned transaction
/// and an array of provided signatures. The signed transaction returned from
/// this method will be sent to the /construction/submit endpoint by the caller.
async fn construction_combine(
    State(app_state): State<AppState>,
    Json(models::ConstructionCombineRequest {
        network_identifier,
        unsigned_transaction,
        signatures,
    }): Json<models::ConstructionCombineRequest>,
) -> Result<Json<models::ConstructionCombineResponse>, models::Error> {
    let client_addr = &app_state.client_addr;

    check_network_identifier(client_addr, network_identifier).await?;

    let signature = signatures
        .first()
        .ok_or_else(|| {
            errors::ErrorKind::InvalidInput("exactly one signature is expected".to_string())
        })?
        .try_into()
        .map_err(|err: near_crypto::ParseSignatureError| {
            errors::ErrorKind::InvalidInput(err.to_string())
        })?;

    let signed_transaction = near_primitives::transaction::SignedTransaction::new(
        signature,
        unsigned_transaction.into_inner(),
    );

    Ok(Json(models::ConstructionCombineResponse { signed_transaction: signed_transaction.into() }))
}

/// Parse a Transaction (offline API)
///
/// Parse is called on both unsigned and signed transactions to understand the
/// intent of the formulated transaction. This is run as a sanity check before
/// signing (after /construction/payloads) and before broadcast (after
/// /construction/combine).
async fn construction_parse(
    State(app_state): State<AppState>,
    Json(models::ConstructionParseRequest { network_identifier, transaction, signed }): Json<models::ConstructionParseRequest>,
) -> Result<Json<models::ConstructionParseResponse>, models::Error> {
    let client_addr = &app_state.client_addr;

    check_network_identifier(client_addr, network_identifier).await?;

    let transaction = if signed {
        near_primitives::transaction::SignedTransaction::try_from_slice(&transaction.into_inner())
            .map_err(|err| {
                errors::ErrorKind::InvalidInput(format!(
                    "Could not parse unsigned transaction: {}",
                    err
                ))
            })?
            .transaction
    } else {
        near_primitives::transaction::Transaction::try_from_slice(&transaction.into_inner())
            .map_err(|err| {
                errors::ErrorKind::InvalidInput(format!(
                    "Could not parse unsigned transaction: {}",
                    err
                ))
            })?
    };

    let account_identifier_signers =
        if signed { vec![transaction.signer_id().clone().into()] } else { vec![] };

    let near_actions = crate::adapters::NearActions {
        sender_account_id: transaction.signer_id().clone(),
        receiver_account_id: transaction.receiver_id().clone(),
        actions: transaction.take_actions(),
    };

    Ok(Json(models::ConstructionParseResponse {
        account_identifier_signers,
        operations: near_actions.into(),
    }))
}

/// Get the Hash of a Signed Transaction
///
/// TransactionHash returns the network-specific transaction hash for a signed
/// transaction.
async fn construction_hash(
    State(app_state): State<AppState>,
    Json(models::ConstructionHashRequest { network_identifier, signed_transaction }): Json<models::ConstructionHashRequest>,
) -> Result<Json<models::TransactionIdentifierResponse>, models::Error> {
    let client_addr = &app_state.client_addr;

    check_network_identifier(client_addr, network_identifier).await?;

    Ok(Json(models::TransactionIdentifierResponse {
        transaction_identifier: models::TransactionIdentifier::transaction(
            &signed_transaction.as_ref().get_hash(),
        ),
    }))
}

/// Submit a Signed Transaction
///
/// Submit a pre-signed transaction to the node. This call should not block on
/// the transaction being included in a block. Rather, it should return
/// immediately with an indication of whether or not the transaction was
/// included in the mempool. The transaction submission response should only
/// return a 200 status if the submitted transaction could be included in the
/// mempool. Otherwise, it should return an error.
async fn construction_submit(
    State(app_state): State<AppState>,
    Json(models::ConstructionSubmitRequest { network_identifier, signed_transaction }): Json<models::ConstructionSubmitRequest>,
) -> Result<Json<models::TransactionIdentifierResponse>, models::Error> {
    let client_addr = &app_state.client_addr;
    let tx_handler_addr = &app_state.tx_handler_addr;

    check_network_identifier(client_addr, network_identifier).await?;

    let transaction_hash = signed_transaction.as_ref().get_hash();
    let transaction_submission = tx_handler_addr
        .send(near_client::ProcessTxRequest {
            transaction: signed_transaction.into_inner(),
            is_forwarded: false,
            check_only: false,
        })
        .await?;
    match transaction_submission {
        near_client::ProcessTxResponse::ValidTx | near_client::ProcessTxResponse::RequestRouted => {
            Ok(Json(models::TransactionIdentifierResponse {
                transaction_identifier: models::TransactionIdentifier::transaction(
                    &transaction_hash,
                ),
            }))
        }
        near_client::ProcessTxResponse::InvalidTx(error) => {
            Err(errors::ErrorKind::InvalidInput(error.to_string()).into())
        }
        _ => Err(errors::ErrorKind::InternalInvariantError(format!(
            "Transaction submission return unexpected result: {:?}",
            transaction_submission
        ))
        .into()),
    }
}

#[derive(OpenApi)]
#[openapi(
    paths(
        network_list,
        network_status,
        network_options,
        // Add other handlers as needed
    ),
    components(
        schemas(
            models::MetadataRequest,
            models::NetworkListResponse,
            models::NetworkRequest,
            models::NetworkStatusResponse,
            models::NetworkOptionsResponse,
            models::Error,
            // Add other response models as needed
        )
    ),
    tags(
        (name = "Network", description = "Network endpoints"),
        (name = "Block", description = "Block endpoints"),
        (name = "Account", description = "Account endpoints"),
        (name = "Construction", description = "Construction endpoints"),
        (name = "Mempool", description = "Mempool endpoints")
    )
)]
pub struct RosettaApiDoc;

fn get_cors(cors_allowed_origins: &[String]) -> CorsLayer {
    if cors_allowed_origins == ["*".to_string()] {
        CorsLayer::permissive()
    } else {
        let mut cors = CorsLayer::new();
        for origin in cors_allowed_origins {
            cors = cors.allow_origin(origin.parse::<HeaderValue>().unwrap());
        }
        cors.allow_methods([Method::GET, Method::POST])
            .allow_headers([AUTHORIZATION, ACCEPT, CONTENT_TYPE])
            .max_age(std::time::Duration::from_secs(3600))
    }
}

pub fn start_rosetta_rpc(
    config: crate::config::RosettaRpcConfig,
    genesis: Genesis,
    genesis_block_hash: &near_primitives::hash::CryptoHash,
    client_addr: TokioRuntimeHandle<ClientActorInner>,
    view_client_addr: Addr<ViewClientActor>,
    tx_handler_addr: Addr<RpcHandlerActor>,
    future_spawner: &dyn FutureSpawner,
) {
    let crate::config::RosettaRpcConfig { addr, cors_allowed_origins, limits, currencies } = config;
    let block_id = models::BlockIdentifier::new(genesis.config.genesis_height, genesis_block_hash);
    let genesis = Arc::new(GenesisWithIdentifier { genesis, block_id });
    
    info!(target:"rosetta", "Starting Rosetta RPC server at {}", addr);

    let app_state = AppState {
        genesis,
        client_addr,
        view_client_addr,
        tx_handler_addr,
        currencies,
    };

    // TODO: Add SwaggerUI integration once we resolve axum compatibility
    let app = Router::new()
        .route("/network/list", post(network_list))
        .route("/network/status", post(network_status))
        .route("/network/options", post(network_options))
        .route("/block", post(block_details))
        .route("/block/transaction", post(block_transaction_details))
        .route("/account/balance", post(account_balance))
        .route("/mempool", post(mempool))
        .route("/mempool/transaction", post(mempool_transaction))
        .route("/construction/derive", post(construction_derive))
        .route("/construction/preprocess", post(construction_preprocess))
        .route("/construction/metadata", post(construction_metadata))
        .route("/construction/payloads", post(construction_payloads))
        .route("/construction/combine", post(construction_combine))
        .route("/construction/parse", post(construction_parse))
        .route("/construction/hash", post(construction_hash))
        .route("/construction/submit", post(construction_submit))
        .layer(get_cors(&cors_allowed_origins))
        .layer(RequestBodyLimitLayer::new(limits.input_payload_max_size))
        .with_state(app_state);

    let addr_copy = addr.clone();
    future_spawner.spawn("Rosetta RPC", async move {
        match tokio::net::TcpListener::bind(addr_copy).await {
            Ok(listener) => {
                if let Err(e) = axum::serve(listener, app).await {
                    error!(target: "rosetta", "Rosetta RPC server error: {:?}", e);
                }
            }
            Err(e) => {
                error!(
                    target: "rosetta",
                    "Could not start Rosetta RPC server at {} due to {:?}", addr, e,
                );
            }
        }
    });
}
