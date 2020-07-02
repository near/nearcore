use std::sync::Arc;

use actix::Addr;
use actix_cors::{Cors, CorsFactory};
use futures::StreamExt;
use strum::IntoEnumIterator;

pub const BASE_PATH: &str = "";
pub const API_VERSION: &str = "1.3.1";

lazy_static::lazy_static! {
    static ref YOCTO_NEAR_CURRENCY: models::Currency =
        models::Currency { symbol: "yoctoNEAR".to_string(), decimals: 0, metadata: None };
}

/*
/// API where `Context` isn't passed on every API call
pub trait ApiNoContext<C: Send + Sync> {
    /// Get an Account Balance
    async fn account_balance(
        &self,
        account_balance_request: models::AccountBalanceRequest,
    ) -> Result<AccountBalanceResponse, ApiError>;

    /// Get a Block
    async fn block(&self, block_request: models::BlockRequest) -> Result<BlockResponse, ApiError>;

    /// Get a Block Transaction
    async fn block_transaction(
        &self,
        block_transaction_request: models::BlockTransactionRequest,
    ) -> Result<BlockTransactionResponse, ApiError>;

    /// Get Transaction Construction Metadata
    async fn construction_metadata(
        &self,
        construction_metadata_request: models::ConstructionMetadataRequest,
    ) -> Result<ConstructionMetadataResponse, ApiError>;

    /// Submit a Signed Transaction
    async fn construction_submit(
        &self,
        construction_submit_request: models::ConstructionSubmitRequest,
    ) -> Result<ConstructionSubmitResponse, ApiError>;

    /// Get All Mempool Transactions
    async fn mempool(
        &self,
        mempool_request: models::MempoolRequest,
    ) -> Result<MempoolResponse, ApiError>;

    /// Get a Mempool Transaction
    async fn mempool_transaction(
        &self,
        mempool_transaction_request: models::MempoolTransactionRequest,
    ) -> Result<MempoolTransactionResponse, ApiError>;

    /// Get List of Available Networks
    async fn network_list(
        &self,
        metadata_request: models::MetadataRequest,
    ) -> Result<NetworkListResponse, ApiError>;

    /// Get Network Options
    async fn network_options(
        &self,
        network_request: models::NetworkRequest,
    ) -> Result<NetworkOptionsResponse, ApiError>;

    /// Get Network Status
    async fn network_status(
        &self,
        network_request: models::NetworkRequest,
    ) -> Result<NetworkStatusResponse, ApiError>;
}
*/

pub mod models;

use std::convert::TryInto;

use actix_web::{App, HttpServer};
use near_chain_configs::Genesis;
use near_client::{ClientActor, ViewClientActor};
use paperclip::actix::{
    api_v2_operation,
    web::HttpResponse,
    // use this instead of actix_web::web
    web::{self, Json},
    Apiv2Schema,
    // extension trait for actix_web::App and proc-macro attributes
    OpenApiExt,
};
use serde::{Deserialize, Serialize};

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

    Ok(Json(models::NetworkStatusResponse {
        current_block_identifier: models::BlockIdentifier {
            index: status.sync_info.latest_block_height.try_into().unwrap(),
            hash: status.sync_info.latest_block_hash.to_string(),
        },
        current_block_timestamp: status.sync_info.latest_block_time.timestamp(),
        genesis_block_identifier: models::BlockIdentifier {
            index: genesis.config.genesis_height.try_into().unwrap(),
            hash: genesis_block.header.hash.to_string(),
        },
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
            operation_statuses: vec![
                models::OperationStatus {
                    status: models::OperationStatusKind::Unknown,
                    successful: false,
                },
                models::OperationStatus {
                    status: models::OperationStatusKind::Failure,
                    successful: false,
                },
                models::OperationStatus {
                    status: models::OperationStatusKind::Success,
                    successful: true,
                },
            ],
            operation_types: models::OperationType::iter().collect(),
            errors: models::ErrorKind::iter().map(models::Error::from_error_kind).collect(),
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
        });
    }

    let block_id = block_identifier.try_into().map_err(|_| models::Error {
        code: 4,
        message: "Invalid input".to_string(),
        retriable: true,
    })?;

    let block = view_client_addr
        .send(near_client::GetBlock(block_id))
        .await?
        .map_err(models::ErrorKind::Other)?;

    let block_identifier = models::BlockIdentifier {
        index: block.header.height.try_into().unwrap(),
        hash: block.header.hash.to_string(),
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
            hash: parent_block.header.hash.to_string(),
        }
    };

    let mut chunks: futures::stream::FuturesUnordered<_> = block
        .chunks
        .iter()
        .map(|chunk| {
            view_client_addr.send(near_client::GetChunk::ChunkHash(chunk.chunk_hash.into()))
        })
        .collect();
    let mut transactions = Vec::<models::TransactionIdentifier>::new();
    while let Some(chunk) = chunks.next().await {
        let chunk = chunk?.map_err(models::ErrorKind::Other)?;
        transactions.extend(chunk.transactions.into_iter().map(|transaction| {
            models::TransactionIdentifier { hash: transaction.hash.to_string() }
        }));
    }

    Ok(Json(models::BlockResponse {
        block: models::Block {
            block_identifier,
            parent_block_identifier,
            timestamp: block.header.timestamp.try_into().unwrap(),
            transactions: vec![],
            metadata: None,
        },
        other_transactions: Some(transactions),
    }))
}

#[api_v2_operation]
/// Get a Block Transaction
async fn transaction_details(
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
        });
    }

    // To fetch a transaction in NEAR you only need the transaction hash and
    // the receiver account id to identify which shard to query, but RosettaRPC
    // defines the interface to be block id + transaction hash, so we do the
    // querying suboptimally: the transaction hash from every shard, and check
    // if the transaction belong to the specified block.

    let block_id: near_primitives::types::BlockIdOrFinality =
        block_identifier.try_into().map_err(|_| models::Error {
            code: 4,
            message: "Invalid input".to_string(),
            retriable: true,
        })?;

    let near_primitives::views::FinalExecutionOutcomeView { transaction, receipts_outcome, .. } =
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            loop {
                if let Some(transaction) = view_client_addr
                    .send(near_client::TxStatus {
                        tx_hash: (transaction_identifier.hash.as_ref() as &str)
                            .try_into()
                            .map_err(|_| models::Error {
                                code: 4,
                                message: "Invalid input".to_string(),
                                retriable: true,
                            })?,
                        signer_account_id: near_primitives::utils::system_account(),
                    })
                    .await??
                {
                    break Result::<_, models::Error>::Ok(transaction);
                }
                tokio::time::delay_for(std::time::Duration::from_millis(300)).await;
            }
        })
        .await??;

    let mut operations = vec![];
    for receipt_outcome in receipts_outcome {
        let chunk = view_client_addr
            .send(near_client::GetChunk::BlockHash(receipt_outcome.block_hash, 0))
            .await?
            .map_err(models::ErrorKind::Other)?;
        for receipt_view in &chunk.receipts {
            if receipt_view.receipt_id == receipt_outcome.id {
                let actions =
                    if let near_primitives::views::ReceiptEnumView::Action { ref actions, .. } =
                        receipt_view.receipt
                    {
                        actions
                    } else {
                        continue;
                    };
                operations.extend(actions.iter().enumerate().map(|(index, action)| {
                    let amount = match action {
                        near_primitives::views::ActionView::Transfer { deposit } => {
                            Some(models::Amount {
                                value: deposit.to_string(),
                                currency: YOCTO_NEAR_CURRENCY.clone(),
                                metadata: None,
                            })
                        }
                        _ => None,
                    };
                    models::Operation {
                        operation_identifier: models::OperationIdentifier {
                            index: index.try_into().unwrap(),
                            network_index: None,
                        },
                        type_: models::OperationType::from(action),
                        amount,
                        account: Some(models::AccountIdentifier {
                            address: transaction.receiver_id.clone(),
                            sub_account: None,
                            metadata: None,
                        }),
                        related_operations: None,
                        status: models::OperationStatusKind::Unknown,
                        metadata: None,
                    }
                }));
            }
        }
    }

    Ok(Json(models::BlockTransactionResponse {
        transaction: models::Transaction { transaction_identifier, operations, metadata: None },
    }))
}

#[api_v2_operation]
/// Get an Account Balance
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
            hash: block_hash.to_string(),
            index: block_height.try_into().unwrap(),
        },
        balances: vec![models::Amount {
            value: account_info.amount.to_string(),
            currency: YOCTO_NEAR_CURRENCY.clone(),
            metadata: None,
        }],
        metadata: None,
    }))
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RosettaRpcConfig {
    pub addr: String,
    pub cors_allowed_origins: Vec<String>,
}

impl Default for RosettaRpcConfig {
    fn default() -> Self {
        Self { addr: "0.0.0.0:3040".to_owned(), cors_allowed_origins: vec!["*".to_owned()] }
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
            .service(web::resource("/block/transaction").route(web::post().to(transaction_details)))
            .service(web::resource("/account/balance").route(web::post().to(account_balance)))
            //
            .service(web::resource("/mempool").route(web::post().to(block_details)))
            .service(web::resource("/mempool/transaction").route(web::post().to(block_details)))
            .service(web::resource("/construction/metadata").route(web::post().to(block_details)))
            .service(web::resource("/construction/submit").route(web::post().to(block_details)))
            //
            .with_json_spec_at("/api/spec")
            .build()
    })
    .bind(addr)
    .unwrap()
    .run();
}
