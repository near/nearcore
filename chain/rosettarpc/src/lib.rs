use std::convert::TryInto;
use std::sync::Arc;

use actix::Addr;
use actix_cors::{Cors, CorsFactory};
use actix_web::{App, HttpServer};
use futures::StreamExt;
use paperclip::actix::{
    api_v2_operation,
    // use this instead of actix_web::web
    web::{self, Json},
    Apiv2Schema,
    // extension trait for actix_web::App and proc-macro attributes
    OpenApiExt,
};
use serde::{Deserialize, Serialize};
use strum::IntoEnumIterator;

use near_chain_configs::Genesis;
use near_client::{ClientActor, ViewClientActor};
use near_primitives::serialize::BaseEncode;

pub const BASE_PATH: &str = "";
pub const API_VERSION: &str = "1.4.0";

pub mod config;
pub use config::RosettaRpcConfig;
mod consts;
pub mod models;

// Mark containers (body, query, parameter, etc.) like so...
#[derive(Serialize, Deserialize, Apiv2Schema)]
struct Pet {
    name: String,
    id: Option<i64>,
}

/// Get List of Available Networks
#[api_v2_operation]
async fn network_list(
    client_addr: web::Data<Addr<ClientActor>>,
    _body: Json<models::MetadataRequest>,
) -> Result<Json<models::NetworkListResponse>, models::Error> {
    let status = client_addr
        .send(near_client::Status { is_health_check: false })
        .await?
        .map_err(models::ErrorKind::Other)?;
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
async fn network_status(
    genesis: web::Data<Arc<Genesis>>,
    client_addr: web::Data<Addr<ClientActor>>,
    view_client_addr: web::Data<Addr<ViewClientActor>>,
    body: Json<models::NetworkRequest>,
) -> Result<Json<models::NetworkStatusResponse>, models::Error> {
    let status = client_addr
        .send(near_client::Status { is_health_check: false })
        .await?
        .map_err(models::ErrorKind::Other)?;
    if status.chain_id != body.network_identifier.network {
        return Err(models::Error {
            code: 2,
            message: "Wrong network (chain id)".to_string(),
            retriable: true,
            details: None,
        });
    }
    let (network_info, genesis_block) = tokio::try_join!(
        client_addr.send(near_client::GetNetworkInfo {}),
        view_client_addr.send(near_client::GetBlock(
            near_primitives::types::BlockId::Height(genesis.config.genesis_height).into(),
        ))
    )?;
    let network_info = network_info.map_err(models::ErrorKind::Other)?;
    let genesis_block = genesis_block.map_err(models::ErrorKind::Other)?;

    let genesis_block_identifier = models::BlockIdentifier {
        index: genesis.config.genesis_height.try_into().unwrap(),
        hash: genesis_block.header.hash.to_base(),
    };
    let oldest_block_identifier =
        if true { genesis_block_identifier.clone() } else { genesis_block_identifier.clone() }; // XXX: query GC for the oldest block
    Ok(Json(models::NetworkStatusResponse {
        current_block_identifier: models::BlockIdentifier {
            index: status.sync_info.latest_block_height.try_into().unwrap(),
            hash: status.sync_info.latest_block_hash.to_base(),
        },
        current_block_timestamp: status.sync_info.latest_block_time.timestamp(),
        genesis_block_identifier,
        oldest_block_identifier,
        peers: network_info
            .active_peers
            .into_iter()
            .map(|peer| models::Peer { peer_id: peer.id.to_string(), metadata: None })
            .collect(),
    }))
}

#[api_v2_operation]
/// Get Network Options
async fn network_options(
    client_addr: web::Data<Addr<ClientActor>>,
    body: Json<models::NetworkRequest>,
) -> Result<Json<models::NetworkOptionsResponse>, models::Error> {
    let status = client_addr
        .send(near_client::Status { is_health_check: false })
        .await?
        .map_err(models::ErrorKind::Other)?;
    if status.chain_id != body.network_identifier.network {
        return Err(models::Error {
            code: 2,
            message: "Wrong network (chain id)".to_string(),
            retriable: true,
            details: None,
        });
    }

    Ok(Json(models::NetworkOptionsResponse {
        version: models::Version {
            rosetta_version: API_VERSION.to_string(),
            node_version: status.version.version,
            middleware_version: None,
            metadata: None,
        },
        allow: models::Allow {
            operation_statuses: models::OperationStatusKind::iter()
                .map(|status| models::OperationStatus {
                    status,
                    successful: status.is_successful(),
                })
                .collect(),
            operation_types: models::OperationType::iter().collect(),
            errors: models::ErrorKind::iter().map(models::Error::from_error_kind).collect(),
            historical_balance_lookup: true,
        },
    }))
}

#[api_v2_operation]
/// Get a Block
async fn block_details(
    client_addr: web::Data<Addr<ClientActor>>,
    view_client_addr: web::Data<Addr<ViewClientActor>>,
    body: Json<models::BlockRequest>,
) -> Result<Json<models::BlockResponse>, models::Error> {
    let Json(models::BlockRequest { network_identifier, block_identifier }) = body;

    let status = client_addr
        .send(near_client::Status { is_health_check: false })
        .await?
        .map_err(models::ErrorKind::Other)?;
    if status.chain_id != network_identifier.network {
        return Err(models::Error {
            code: 2,
            message: "Wrong network (chain id)".to_string(),
            retriable: true,
            details: None,
        });
    }

    let block_id = block_identifier.try_into().map_err(|_| models::Error {
        code: 4,
        message: "Invalid input".to_string(),
        retriable: true,
        details: None,
    })?;

    let block = view_client_addr
        .send(near_client::GetBlock(block_id))
        .await?
        .map_err(models::ErrorKind::Other)?;

    let block_identifier = models::BlockIdentifier {
        index: block.header.height.try_into().unwrap(),
        hash: block.header.hash.to_base(),
    };

    let parent_block_identifier = if block.header.prev_hash == Default::default() {
        block_identifier.clone()
    } else {
        let parent_block = view_client_addr
            .send(near_client::GetBlock(
                near_primitives::types::BlockId::Hash(block.header.prev_hash).into(),
            ))
            .await?
            .map_err(models::ErrorKind::Other)?;

        models::BlockIdentifier {
            index: parent_block.header.height.try_into().unwrap(),
            hash: parent_block.header.hash.to_base(),
        }
    };

    let mut chunks: futures::stream::FuturesUnordered<_> = block
        .chunks
        .iter()
        .map(|chunk| {
            view_client_addr.send(near_client::GetChunk::ChunkHash(chunk.chunk_hash.into()))
        })
        .collect();
    let mut transactions = Vec::<models::Transaction>::new();
    while let Some(chunk) = chunks.next().await {
        let chunk = chunk?.map_err(models::ErrorKind::Other)?;
        transactions.extend(chunk.transactions.iter().map(Into::into));
        transactions.extend(chunk.receipts.iter().map(Into::into));
    }

    Ok(Json(models::BlockResponse {
        block: models::Block {
            block_identifier,
            parent_block_identifier,
            timestamp: block.header.timestamp.try_into().unwrap(),
            transactions,
            metadata: None,
        },
        other_transactions: None,
    }))
}

#[api_v2_operation]
/// Get a Block Transaction
async fn block_transaction_details(
    client_addr: web::Data<Addr<ClientActor>>,
    view_client_addr: web::Data<Addr<ViewClientActor>>,
    body: Json<models::BlockTransactionRequest>,
) -> Result<Json<models::BlockTransactionResponse>, models::Error> {
    let Json(models::BlockTransactionRequest {
        network_identifier,
        block_identifier,
        transaction_identifier,
    }) = body;

    let status = client_addr
        .send(near_client::Status { is_health_check: false })
        .await?
        .map_err(models::ErrorKind::Other)?;
    if status.chain_id != network_identifier.network {
        return Err(models::Error {
            code: 2,
            message: "Wrong network (chain id)".to_string(),
            retriable: true,
            details: None,
        });
    }

    let block_id: near_primitives::types::BlockIdOrFinality =
        block_identifier.try_into().map_err(|_| models::Error {
            code: 4,
            message: "Invalid input".to_string(),
            retriable: true,
            details: None,
        })?;

    let transaction_or_receipt_hash =
        (transaction_identifier.hash.as_ref() as &str).try_into().map_err(|_| models::Error {
            code: 4,
            message: "Invalid input".to_string(),
            retriable: true,
            details: None,
        })?;

    let block = view_client_addr
        .send(near_client::GetBlock(block_id))
        .await?
        .map_err(models::ErrorKind::Other)?;

    let mut chunks: futures::stream::FuturesUnordered<_> = block
        .chunks
        .iter()
        .map(|chunk| {
            view_client_addr.send(near_client::GetChunk::ChunkHash(chunk.chunk_hash.into()))
        })
        .collect();

    while let Some(chunk) = chunks.next().await {
        let chunk = chunk?.map_err(models::ErrorKind::Other)?;
        let transaction = if let Some(transaction) = chunk
            .transactions
            .iter()
            .find(|transaction| transaction.hash == transaction_or_receipt_hash)
        {
            transaction.into()
        } else if let Some(receipt) =
            chunk.receipts.iter().find(|receipt| receipt.receipt_id == transaction_or_receipt_hash)
        {
            receipt.into()
        } else {
            continue;
        };
        return Ok(Json(models::BlockTransactionResponse { transaction }));
    }

    Err(models::ErrorKind::NotFound(
        "Neither transaction nor receipt was found for the given hash".to_string(),
    )
    .into())
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

    let status = client_addr
        .send(near_client::Status { is_health_check: false })
        .await?
        .map_err(models::ErrorKind::Other)?;
    if status.chain_id != network_identifier.network {
        return Err(models::Error {
            code: 2,
            message: "Wrong network (chain id)".to_string(),
            retriable: true,
            details: None,
        });
    }

    let block_id: near_primitives::types::BlockIdOrFinality = block_identifier
        .map(TryInto::try_into)
        .unwrap_or(Ok(near_primitives::types::BlockIdOrFinality::Finality(
            near_primitives::types::Finality::Final,
        )))
        .map_err(|_| models::Error {
            code: 4,
            message: "Invalid input".to_string(),
            retriable: true,
            details: None,
        })?;

    let query = near_client::Query::new(
        block_id,
        near_primitives::views::QueryRequest::ViewAccount {
            account_id: account_identifier.address,
        },
    );
    let near_primitives::views::QueryResponse { block_hash, block_height, kind } =
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            loop {
                if let Some(query_response) =
                    view_client_addr.send(query.clone()).await?.map_err(models::ErrorKind::Other)?
                {
                    break Result::<_, models::Error>::Ok(query_response);
                }
                tokio::time::delay_for(std::time::Duration::from_millis(100)).await;
            }
        })
        .await??;

    let account_info =
        if let near_primitives::views::QueryResponseKind::ViewAccount(account_info) = kind {
            account_info
        } else {
            return Err(models::ErrorKind::Other(
            "Internal invariant is not held; we queried ViewAccount, but received something else."
                .to_string(),
        )
        .into());
        };

    Ok(Json(models::AccountBalanceResponse {
        block_identifier: models::BlockIdentifier {
            hash: block_hash.to_base(),
            index: block_height.try_into().unwrap(),
        },
        balances: vec![models::Amount {
            value: account_info.amount.to_string(),
            currency: consts::YOCTO_NEAR_CURRENCY.clone(),
            metadata: None,
        }],
        metadata: None,
    }))
}

#[api_v2_operation]
/// Get All Mempool Transactions
///
/// Get all Transaction Identifiers in the mempool
async fn mempool(
    client_addr: web::Data<Addr<ClientActor>>,
    body: Json<models::NetworkRequest>,
) -> Result<Json<models::MempoolResponse>, models::Error> {
    // TODO
    Err(models::ErrorKind::Other("Not implemented yet".to_string()).into())
}

#[api_v2_operation]
/// Get a Mempool Transaction
///
/// Get a transaction in the mempool by its Transaction Identifier. This is a
/// separate request than fetching a block transaction (/block/transaction)
/// because some blockchain nodes need to know that a transaction query is for
/// something in the mempool instead of a transaction in a block. Transactions
/// may not be fully parsable until they are in a block (ex: may not be possible
/// to determine the fee to pay before a transaction is executed). On this
/// endpoint, it is ok that returned transactions are only estimates of what may
/// actually be included in a block.
async fn mempool_transaction(
    client_addr: web::Data<Addr<ClientActor>>,
    body: Json<models::MempoolTransactionRequest>,
) -> Result<Json<models::MempoolTransactionResponse>, models::Error> {
    // TODO
    Err(models::ErrorKind::Other("Not implemented yet".to_string()).into())
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
) -> Result<Json<models::ConstructionSubmitResponse>, models::Error> {
    let Json(models::ConstructionSubmitRequest { network_identifier, signed_transaction }) = body;
    let status = client_addr
        .send(near_client::Status { is_health_check: false })
        .await?
        .map_err(models::ErrorKind::Other)?;
    if status.chain_id != network_identifier.network {
        return Err(models::Error {
            code: 2,
            message: "Wrong network (chain id)".to_string(),
            retriable: true,
            details: None,
        });
    }

    let transaction_hash = signed_transaction.0.get_hash().to_base();
    client_addr.do_send(near_network::NetworkClientMessages::Transaction {
        transaction: signed_transaction.0,
        is_forwarded: false,
        check_only: false,
    });
    Ok(Json(models::ConstructionSubmitResponse {
        transaction_identifier: models::TransactionIdentifier { hash: transaction_hash },
        metadata: None,
    }))
}

#[api_v2_operation]
/// Create a Request to Fetch Metadata
///
/// Preprocess is called prior to /construction/payloads to construct a request
/// for any metadata that is needed for transaction construction given (i.e.
/// account nonce). The request returned from this method will be used by the
/// caller (in a different execution environment) to call the
/// /construction/metadata endpoint.
async fn construction_preprocess(
    client_addr: web::Data<Addr<ClientActor>>,
    body: Json<models::ConstructionSubmitRequest>,
) -> Result<Json<models::ConstructionSubmitResponse>, models::Error> {
    // TODO
    Err(models::ErrorKind::Other("Not implemented yet".to_string()).into())
}

#[api_v2_operation]
/// Get Metadata for Transaction Construction
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
    body: Json<models::ConstructionSubmitRequest>,
) -> Result<Json<models::ConstructionSubmitResponse>, models::Error> {
    // TODO
    Err(models::ErrorKind::Other("Not implemented yet".to_string()).into())
}

#[api_v2_operation]
/// Generate an Unsigned Transaction and Signing Payloads
async fn construction_payloads(
    client_addr: web::Data<Addr<ClientActor>>,
    body: Json<models::ConstructionSubmitRequest>,
) -> Result<Json<models::ConstructionSubmitResponse>, models::Error> {
    // TODO
    Err(models::ErrorKind::Other("Not implemented yet".to_string()).into())
}

#[api_v2_operation]
/// Create Network Transaction from Signatures
///
/// Combine creates a network-specific transaction from an unsigned transaction
/// and an array of provided signatures. The signed transaction returned from
/// this method will be sent to the /construction/submit endpoint by the caller.
async fn construction_combine(
    client_addr: web::Data<Addr<ClientActor>>,
    body: Json<models::ConstructionSubmitRequest>,
) -> Result<Json<models::ConstructionSubmitResponse>, models::Error> {
    // TODO
    Err(models::ErrorKind::Other("Not implemented yet".to_string()).into())
}

#[api_v2_operation]
/// Parse a Transaction
///
/// Parse is called on both unsigned and signed transactions to understand the
/// intent of the formulated transaction. This is run as a sanity check before
/// signing (after /construction/payloads) and before broadcast (after
/// /construction/combine).
async fn construction_parse(
    client_addr: web::Data<Addr<ClientActor>>,
    body: Json<models::ConstructionSubmitRequest>,
) -> Result<Json<models::ConstructionSubmitResponse>, models::Error> {
    // TODO
    Err(models::ErrorKind::Other("Not implemented yet".to_string()).into())
}

#[api_v2_operation]
/// Get the Hash of a Signed Transaction
///
/// TransactionHash returns the network-specific transaction hash for a signed
/// transaction.
async fn construction_hash(
    client_addr: web::Data<Addr<ClientActor>>,
    body: Json<models::ConstructionHashRequest>,
) -> Result<Json<models::ConstructionHashResponse>, models::Error> {
    let Json(models::ConstructionHashRequest { network_identifier, signed_transaction }) = body;

    Ok(Json(models::ConstructionHashResponse {
        transaction_hash: signed_transaction.0.get_hash().to_base(),
    }))
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

pub fn start_rosettarpc(
    config: RosettaRpcConfig,
    genesis: Arc<Genesis>,
    client_addr: Addr<ClientActor>,
    view_client_addr: Addr<ViewClientActor>,
) {
    let RosettaRpcConfig { addr, cors_allowed_origins } = config;
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
                web::resource("/construction/metadata")
                    .route(web::post().to(construction_metadata)),
            )
            .service(
                web::resource("​/construction​/preprocess")
                    .route(web::post().to(construction_preprocess)),
            )
            .service(
                web::resource("/construction/​payloads")
                    .route(web::post().to(construction_payloads)),
            )
            .service(
                web::resource("/construction/combine").route(web::post().to(construction_combine)),
            )
            .service(web::resource("/construction/parse").route(web::post().to(construction_parse)))
            .service(web::resource("/construction/hash").route(web::post().to(construction_hash)))
            // Not implemented by design:
            // Blockchains that require an on-chain action to create an account should not implement
            // this method.
            //.service(web::resource("​/construction​/derive").route(web::post().to(_)))
            .with_json_spec_at("/api/spec")
            .build()
    })
    .bind(addr)
    .unwrap()
    .run();
}
