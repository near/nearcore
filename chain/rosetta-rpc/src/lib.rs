#![doc = include_str!("../README.md")]

use std::convert::AsRef;
use std::sync::Arc;

use actix::Addr;
use actix_cors::Cors;
use actix_web::{App, HttpServer, ResponseError};
use paperclip::actix::{
    api_v2_operation,
    web::{self, Json},
    OpenApiExt,
};
use strum::IntoEnumIterator;

use near_chain_configs::Genesis;
use near_client::{ClientActor, ViewClientActor};
use near_o11y::WithSpanContextExt;
use near_primitives::borsh::BorshDeserialize;

pub use config::RosettaRpcConfig;

mod adapters;
mod config;
mod errors;
mod models;
mod types;
mod utils;

pub const BASE_PATH: &str = "";
pub const API_VERSION: &str = "1.4.4";
pub const BLOCKCHAIN: &str = "nearprotocol";

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
    client_addr: &web::Data<Addr<ClientActor>>,
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
        .send(near_client::Status { is_health_check: false, detailed: false }.with_span_context())
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
#[api_v2_operation]
async fn network_list(
    client_addr: web::Data<Addr<ClientActor>>,
    _body: Json<models::MetadataRequest>,
) -> Result<Json<models::NetworkListResponse>, models::Error> {
    let status = client_addr
        .send(near_client::Status { is_health_check: false, detailed: false }.with_span_context())
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

#[api_v2_operation]
/// Get Network Status
///
/// This endpoint returns the current status of the network requested. Any
/// NetworkIdentifier returned by /network/list should be accessible here.
async fn network_status(
    genesis: web::Data<GenesisWithIdentifier>,
    client_addr: web::Data<Addr<ClientActor>>,
    view_client_addr: web::Data<Addr<ViewClientActor>>,
    body: Json<models::NetworkRequest>,
) -> Result<Json<models::NetworkStatusResponse>, models::Error> {
    let Json(models::NetworkRequest { network_identifier }) = body;

    let status = check_network_identifier(&client_addr, network_identifier).await?;

    let (network_info, earliest_block) = tokio::try_join!(
        client_addr.send(near_client::GetNetworkInfo {}.with_span_context()),
        view_client_addr.send(
            near_client::GetBlock(near_primitives::types::BlockReference::SyncCheckpoint(
                near_primitives::types::SyncCheckpoint::EarliestAvailable
            ),)
            .with_span_context()
        ),
    )?;
    let network_info = network_info.map_err(errors::ErrorKind::InternalError)?;
    let genesis_block_identifier = genesis.block_id.clone();
    let oldest_block_identifier: models::BlockIdentifier = earliest_block
        .ok()
        .map(|block| (&block).into())
        .unwrap_or_else(|| genesis_block_identifier.clone());

    let final_block = crate::utils::get_final_block(&view_client_addr).await?;
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
    let Json(models::NetworkRequest { network_identifier }) = body;

    let status = check_network_identifier(&client_addr, network_identifier).await?;

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
    genesis: web::Data<GenesisWithIdentifier>,
    client_addr: web::Data<Addr<ClientActor>>,
    view_client_addr: web::Data<Addr<ViewClientActor>>,
    body: Json<models::BlockRequest>,
) -> Result<Json<models::BlockResponse>, models::Error> {
    let Json(models::BlockRequest { network_identifier, block_identifier }) = body;

    check_network_identifier(&client_addr, network_identifier).await?;

    let block_id: near_primitives::types::BlockReference = block_identifier.try_into()?;
    let block = crate::utils::get_block_if_final(&block_id, view_client_addr.get_ref())
        .await?
        .ok_or_else(|| errors::ErrorKind::NotFound("Block not found".into()))?;

    let block_identifier: models::BlockIdentifier = (&block).into();

    let parent_block_identifier = if block.header.prev_hash == Default::default() {
        // According to Rosetta API genesis block should have the parent block
        // identifier referencing itself:
        block_identifier.clone()
    } else {
        let parent_block = view_client_addr
            .send(
                near_client::GetBlock(
                    near_primitives::types::BlockId::Hash(block.header.prev_hash).into(),
                )
                .with_span_context(),
            )
            .await?
            .map_err(|err| errors::ErrorKind::InternalError(err.to_string()))?;
        (&parent_block).into()
    };

    let transactions =
        crate::adapters::collect_transactions(&genesis.genesis, view_client_addr.get_ref(), &block)
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
    genesis: web::Data<GenesisWithIdentifier>,
    client_addr: web::Data<Addr<ClientActor>>,
    view_client_addr: web::Data<Addr<ViewClientActor>>,
    body: Json<models::BlockTransactionRequest>,
) -> Result<Json<models::BlockTransactionResponse>, models::Error> {
    let Json(models::BlockTransactionRequest {
        network_identifier,
        block_identifier,
        transaction_identifier,
    }) = body;

    check_network_identifier(&client_addr, network_identifier).await?;

    let block_id: near_primitives::types::BlockReference = block_identifier.try_into()?;

    let block = crate::utils::get_block_if_final(&block_id, view_client_addr.get_ref())
        .await?
        .ok_or_else(|| errors::ErrorKind::NotFound("Block not found".into()))?;

    let transaction =
        crate::adapters::collect_transactions(&genesis.genesis, view_client_addr.get_ref(), &block)
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
    client_addr: web::Data<Addr<ClientActor>>,
    view_client_addr: web::Data<Addr<ViewClientActor>>,
    body: Json<models::AccountBalanceRequest>,
) -> Result<Json<models::AccountBalanceResponse>, models::Error> {
    let Json(models::AccountBalanceRequest {
        network_identifier,
        block_identifier,
        account_identifier,
    }) = body;

    check_network_identifier(&client_addr, network_identifier).await?;

    let block_id: near_primitives::types::BlockReference = block_identifier
        .map(TryInto::try_into)
        .unwrap_or(Ok(near_primitives::types::BlockReference::Finality(
            near_primitives::types::Finality::Final,
        )))?;

    // TODO: update error handling once we return structured errors from the
    // view_client handlers
    let block = crate::utils::get_block_if_final(&block_id, view_client_addr.get_ref())
        .await?
        .ok_or_else(|| errors::ErrorKind::NotFound("Block not found".into()))?;

    let runtime_config =
        crate::utils::query_protocol_config(block.header.hash, view_client_addr.get_ref())
            .await?
            .runtime_config;

    let account_id_for_access_key = account_identifier.address.clone();
    let account_id = account_identifier.address.into();
    let (block_hash, block_height, account_info) =
        match crate::utils::query_account(block_id, account_id, &view_client_addr).await {
            Ok(account_info_response) => account_info_response,
            Err(crate::errors::ErrorKind::NotFound(_)) => (
                block.header.hash,
                block.header.height,
                near_primitives::account::Account::new(0, 0, Default::default(), 0).into(),
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
                &view_client_addr,
                account_id_for_access_key,
                metadata.public_keys,
            )
            .await?,
        )
    } else {
        None
    };
    Ok(Json(models::AccountBalanceResponse {
        block_identifier: models::BlockIdentifier::new(block_height, &block_hash),
        balances: vec![models::Amount::from_yoctonear(balance)],
        metadata: nonces,
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
    client_addr: web::Data<Addr<ClientActor>>,
    body: Json<models::ConstructionDeriveRequest>,
) -> Result<Json<models::ConstructionDeriveResponse>, models::Error> {
    let Json(models::ConstructionDeriveRequest { network_identifier, public_key }) = body;

    check_network_identifier(&client_addr, network_identifier).await?;

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

    check_network_identifier(&client_addr, network_identifier).await?;

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

    check_network_identifier(&client_addr, network_identifier).await?;

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
        &view_client_addr,
    )
    .await?;

    Ok(Json(models::ConstructionMetadataResponse {
        metadata: models::ConstructionMetadata {
            recent_block_hash: block_hash.to_string(),
            signer_public_access_key_nonce: access_key.nonce.saturating_add(1),
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

    check_network_identifier(&client_addr, network_identifier).await?;

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
    let unsigned_transaction = near_primitives::transaction::Transaction {
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
    };

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

    check_network_identifier(&client_addr, network_identifier).await?;

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

    check_network_identifier(&client_addr, network_identifier).await?;

    let near_primitives::transaction::Transaction {
        actions,
        signer_id: sender_account_id,
        receiver_id: receiver_account_id,
        ..
    } = if signed {
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
        if signed { vec![sender_account_id.clone().into()] } else { vec![] };

    let near_actions =
        crate::adapters::NearActions { sender_account_id, receiver_account_id, actions };

    Ok(Json(models::ConstructionParseResponse {
        account_identifier_signers,
        operations: near_actions.into(),
    }))
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

    check_network_identifier(&client_addr, network_identifier).await?;

    Ok(Json(models::TransactionIdentifierResponse {
        transaction_identifier: models::TransactionIdentifier::transaction(
            &signed_transaction.as_ref().get_hash(),
        ),
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

    check_network_identifier(&client_addr, network_identifier).await?;

    let transaction_hash = signed_transaction.as_ref().get_hash();
    let transaction_submittion = client_addr
        .send(
            near_client::ProcessTxRequest {
                transaction: signed_transaction.into_inner(),
                is_forwarded: false,
                check_only: false,
            }
            .with_span_context(),
        )
        .await?;
    match transaction_submittion {
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
            "Transaction submition return unexpected result: {:?}",
            transaction_submittion
        ))
        .into()),
    }
}

fn get_cors(cors_allowed_origins: &[String]) -> Cors {
    let mut cors = Cors::permissive();
    if cors_allowed_origins != ["*".to_string()] {
        for origin in cors_allowed_origins {
            cors = cors.allowed_origin(origin);
        }
    }
    cors.allowed_methods(vec!["GET", "POST"])
        .allowed_headers(vec![
            actix_web::http::header::AUTHORIZATION,
            actix_web::http::header::ACCEPT,
        ])
        .allowed_header(actix_web::http::header::CONTENT_TYPE)
        .max_age(3600)
}

pub fn start_rosetta_rpc(
    config: crate::config::RosettaRpcConfig,
    genesis: Genesis,
    genesis_block_hash: &near_primitives::hash::CryptoHash,
    client_addr: Addr<ClientActor>,
    view_client_addr: Addr<ViewClientActor>,
) -> actix_web::dev::ServerHandle {
    let crate::config::RosettaRpcConfig { addr, cors_allowed_origins, limits } = config;
    let block_id = models::BlockIdentifier::new(genesis.config.genesis_height, genesis_block_hash);
    let genesis = Arc::new(GenesisWithIdentifier { genesis, block_id });
    let server = HttpServer::new(move || {
        let json_config = web::JsonConfig::default()
            .limit(limits.input_payload_max_size)
            .error_handler(|err, _req| {
                let error_message = err.to_string();
                actix_web::error::InternalError::from_response(
                    err,
                    models::Error::from_error_kind(errors::ErrorKind::InvalidInput(error_message))
                        .error_response(),
                )
                .into()
            });

        App::new()
            .app_data(json_config)
            .wrap(actix_web::middleware::Logger::default())
            .app_data(web::Data::from(genesis.clone()))
            .app_data(web::Data::new(client_addr.clone()))
            .app_data(web::Data::new(view_client_addr.clone()))
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
            .service(
                web::resource("/construction/submit").route(web::post().to(construction_submit)),
            )
            .with_json_spec_at("/api/spec")
            .build()
    })
    .bind(addr)
    .unwrap()
    .shutdown_timeout(5)
    .disable_signals()
    .run();

    let handle = server.handle();

    tokio::spawn(server);

    handle
}
