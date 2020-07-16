use std::env;

use bigdecimal::BigDecimal;
use num_traits::cast::FromPrimitive;
use tokio::sync::mpsc;

use diesel::{
    prelude::*,
    r2d2::{ConnectionManager, Pool},
};
#[macro_use]
extern crate diesel;
extern crate dotenv;
use dotenv::dotenv;
use tokio_diesel::*;

use near_indexer;
mod schema;
use schema::access_keys;

const ADD: &str = "ADD";
const DELETE: &str = "DELETE";
const PENDING: &str = "PENDING";
const SUCCESS: &str = "SUCCESS";
const FAILED: &str = "FAILED";
const FUNCTION_CALL: &str = "FUNCTION_CALL";
const FULL_ACCESS: &str = "FULL_ACCESS";

#[derive(Insertable, Queryable)]
struct AccessKey {
    public_key: String,
    account_id: String,
    action: &'static str,
    status: &'static str,
    receipt_hash: String,
    block_height: BigDecimal,
    permission: &'static str,
}

impl AccessKey {
    pub fn from_receipt_view(
        receipt: &near_indexer::near_primitives::views::ReceiptView,
        block_height: u64,
    ) -> Vec<Self> {
        let mut access_keys: Vec<Self> = vec![];
        if let near_indexer::near_primitives::views::ReceiptEnumView::Action { actions, .. } =
            &receipt.receipt
        {
            for action in actions {
                let access_key = match action {
                    near_indexer::near_primitives::views::ActionView::AddKey {
                        public_key,
                        access_key,
                    } => Self {
                        public_key: public_key.to_string(),
                        account_id: receipt.receiver_id.to_string(),
                        action: ADD,
                        status: PENDING,
                        receipt_hash: receipt.receipt_id.to_string(),
                        block_height: BigDecimal::from_u64(block_height).expect("Block height must be provided."),
                        permission: match access_key.permission {
                            near_indexer::near_primitives::views::AccessKeyPermissionView::FullAccess => {
                                FULL_ACCESS
                            },
                            near_indexer::near_primitives::views::AccessKeyPermissionView::FunctionCall { .. } => {
                                FUNCTION_CALL
                            },
                        },
                    },
                    near_indexer::near_primitives::views::ActionView::DeleteKey { public_key } => Self {
                        public_key: public_key.to_string(),
                        account_id: receipt.receiver_id.to_string(),
                        action: DELETE,
                        status: PENDING,
                        receipt_hash: receipt.receipt_id.to_string(),
                        block_height: BigDecimal::from_u64(block_height).expect("Block height must be provided."),
                        permission: "",
                    },
                    _ => { continue },
                };
                access_keys.push(access_key);
            }
        }
        access_keys
    }
}

fn establish_connection() -> Pool<ConnectionManager<PgConnection>> {
    dotenv().ok();

    let database_url = env::var("DATABASE_URL").expect("DATABASE_URL must be set");
    let manager = ConnectionManager::<PgConnection>::new(&database_url);
    Pool::builder().build(manager).expect(&format!("Error connecting to {}", database_url))
}

async fn handle_genesis_public_keys(near_config: near_indexer::NearConfig) {
    let pool = establish_connection();
    let genesis_height = near_config.genesis.config.genesis_height.clone();
    let access_keys = near_config.genesis.records.as_ref()
        .iter()
        .filter(|record| match record {
        near_indexer::near_primitives::state_record::StateRecord::AccessKey { .. } => true,
        _ => false,
    })
        .filter_map(|record| if let near_indexer::near_primitives::state_record::StateRecord::AccessKey { account_id, public_key, access_key} = record {
        Some(AccessKey {
            public_key: public_key.to_string(),
            account_id: account_id.to_string(),
            action: ADD,
            status: SUCCESS,
            receipt_hash: "genesis".to_string(),
            block_height: BigDecimal::from_u64(genesis_height).expect("genesis_height must be provided"),
            permission: match access_key.permission {
                near_indexer::near_primitives::account::AccessKeyPermission::FullAccess => FULL_ACCESS,
                near_indexer::near_primitives::account::AccessKeyPermission::FunctionCall(_) => FUNCTION_CALL,
            }
        })
    } else { None }).collect::<Vec<AccessKey>>();

    diesel::insert_into(schema::access_keys::table)
        .values(access_keys)
        .on_conflict_do_nothing()
        .execute_async(&pool)
        .await
        .unwrap();
}

async fn listen_blocks(mut stream: mpsc::Receiver<near_indexer::BlockResponse>) {
    let pool = establish_connection();

    while let Some(block) = stream.recv().await {
        eprintln!("Block height {:?}", block.block.header.height);

        // Handle outcomes
        let receipt_outcomes = &block
            .outcomes
            .iter()
            .filter_map(|outcome| match outcome {
                near_indexer::Outcome::Receipt(execution_outcome) => Some(execution_outcome),
                _ => None,
            })
            .collect::<Vec<&near_indexer::near_primitives::views::ExecutionOutcomeWithIdView>>();

        diesel::update(
            schema::access_keys::table.filter(
                schema::access_keys::dsl::receipt_hash.eq_any(
                    receipt_outcomes
                        .iter()
                        .filter(|outcome| match &outcome.outcome.status {
                            near_indexer::near_primitives::views::ExecutionStatusView::Failure(
                                _,
                            ) => true,
                            _ => false,
                        })
                        .map(|outcome| outcome.id.clone().to_string())
                        .collect::<Vec<String>>(),
                ),
            ),
        )
        .set(schema::access_keys::dsl::status.eq(FAILED))
        .execute_async(&pool)
        .await
        .unwrap();

        diesel::update(
            schema::access_keys::table.filter(
                schema::access_keys::dsl::receipt_hash.eq_any(
                    receipt_outcomes
                        .iter()
                        .filter(|outcome| match &outcome.outcome.status {
                            near_indexer::near_primitives::views::ExecutionStatusView::SuccessReceiptId(
                                _,
                            ) | near_indexer::near_primitives::views::ExecutionStatusView::SuccessValue(_) => true,
                            _ => false,
                        })
                        .map(|outcome| outcome.id.clone().to_string())
                        .collect::<Vec<String>>(),
                ),
            ),
        )
        .set(schema::access_keys::dsl::status.eq(SUCCESS))
        .execute_async(&pool)
        .await
        .unwrap();

        // Handle receipts
        for chunk in &block.chunks {
            diesel::insert_into(schema::access_keys::table)
                .values(
                    chunk
                        .receipts
                        .iter()
                        .filter(|receipt| match receipt.receipt {
                            near_indexer::near_primitives::views::ReceiptEnumView::Action {
                                ..
                            } => true,
                            _ => false,
                        })
                        .map(|receipt| {
                            AccessKey::from_receipt_view(receipt, block.block.header.height.clone())
                        })
                        .flatten()
                        .collect::<Vec<AccessKey>>(),
                )
                .execute_async(&pool)
                .await
                .unwrap();
        }
    }
}

fn main() {
    let home_dir: Option<String> = env::args().nth(1);

    let indexer = near_indexer::Indexer::new(home_dir.as_ref().map(AsRef::as_ref));
    let near_config = indexer.near_config().clone();
    let stream = indexer.streamer();
    actix::spawn(handle_genesis_public_keys(near_config));
    actix::spawn(listen_blocks(stream));
    indexer.start();
}
