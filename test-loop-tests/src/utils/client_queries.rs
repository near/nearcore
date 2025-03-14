use near_client::Client;
use near_epoch_manager::shard_assignment::{account_id_to_shard_id, shard_id_to_uid};
use near_primitives::hash::CryptoHash;
use near_primitives::types::{AccountId, Balance, ShardId};
use near_primitives::views::{
    FinalExecutionOutcomeView, QueryRequest, QueryResponse, QueryResponseKind,
};

pub trait ClientQueries {
    fn client_index_tracking_account(&self, account: &AccountId) -> usize;
    fn runtime_query(&self, account: &AccountId, query: QueryRequest) -> QueryResponse;
    fn query_balance(&self, account: &AccountId) -> Balance;
    #[allow(unused)]
    fn view_call(&self, account: &AccountId, method: &str, args: &[u8]) -> Vec<u8>;
    #[allow(unused)]
    fn tx_outcome(&self, tx_hash: CryptoHash) -> FinalExecutionOutcomeView;
    fn tracked_shards_for_each_client(&self) -> Vec<Vec<ShardId>>;
}

impl<Data> ClientQueries for Vec<Data>
where
    Data: AsRef<Client>,
{
    fn client_index_tracking_account(&self, account_id: &AccountId) -> usize {
        let client: &Client = self[0].as_ref();
        let head = client.chain.head().unwrap();
        let shard_id =
            account_id_to_shard_id(client.epoch_manager.as_ref(), &account_id, &head.epoch_id)
                .unwrap();

        for i in 0..self.len() {
            let client: &Client = self[i].as_ref();
            let validator_signer = client.validator_signer.get().unwrap();
            let account_id = validator_signer.validator_id();
            let tracks_shard = client
                .epoch_manager
                .cares_about_shard_from_prev_block(&head.prev_block_hash, account_id, shard_id)
                .unwrap();
            if tracks_shard {
                return i;
            }
        }
        panic!("No client tracks shard {}", shard_id);
    }

    fn runtime_query(&self, account_id: &AccountId, query: QueryRequest) -> QueryResponse {
        let client_index = self.client_index_tracking_account(account_id);
        let client: &Client = self[client_index].as_ref();
        let head = client.chain.head().unwrap();
        let last_block = client.chain.get_block(&head.last_block_hash).unwrap();
        let shard_id =
            account_id_to_shard_id(client.epoch_manager.as_ref(), &account_id, &head.epoch_id)
                .unwrap();
        let shard_uid =
            shard_id_to_uid(client.epoch_manager.as_ref(), shard_id, &head.epoch_id).unwrap();
        let shard_layout = client.epoch_manager.get_shard_layout(&head.epoch_id).unwrap();
        let shard_index = shard_layout.get_shard_index(shard_id).unwrap();
        let last_chunk_header = &last_block.chunks()[shard_index];

        client
            .runtime_adapter
            .query(
                shard_uid,
                &last_chunk_header.prev_state_root(),
                last_block.header().height(),
                last_block.header().raw_timestamp(),
                last_block.header().prev_hash(),
                last_block.header().hash(),
                last_block.header().epoch_id(),
                &query,
            )
            .unwrap()
    }

    fn query_balance(&self, account_id: &AccountId) -> Balance {
        let response = self.runtime_query(
            account_id,
            QueryRequest::ViewAccount { account_id: account_id.clone() },
        );
        if let QueryResponseKind::ViewAccount(account_view) = response.kind {
            account_view.amount
        } else {
            panic!("Wrong return value")
        }
    }

    fn view_call(&self, account_id: &AccountId, method: &str, args: &[u8]) -> Vec<u8> {
        let response = self.runtime_query(
            account_id,
            QueryRequest::CallFunction {
                account_id: account_id.clone(),
                method_name: method.to_string(),
                args: args.to_vec().into(),
            },
        );
        if let QueryResponseKind::CallResult(call_result) = response.kind {
            call_result.result
        } else {
            panic!("Wrong return value")
        }
    }

    fn tx_outcome(&self, tx_hash: CryptoHash) -> FinalExecutionOutcomeView {
        // TODO: this does not work yet with single-shard tracking.
        let client: &Client = self[0].as_ref();
        client.chain.get_final_transaction_result(&tx_hash).unwrap()
    }

    fn tracked_shards_for_each_client(&self) -> Vec<Vec<ShardId>> {
        let client: &Client = self[0].as_ref();
        let head = client.chain.head().unwrap();
        let all_shard_ids = client.epoch_manager.shard_ids(&head.epoch_id).unwrap();

        let mut ret = Vec::new();
        for i in 0..self.len() {
            let client: &Client = self[i].as_ref();
            let validator_signer = client.validator_signer.get().unwrap();
            let account_id = validator_signer.validator_id();
            let mut tracked_shards = Vec::new();
            for shard_id in &all_shard_ids {
                let tracks_shard = client
                    .epoch_manager
                    .cares_about_shard_from_prev_block(&head.prev_block_hash, account_id, *shard_id)
                    .unwrap();
                if tracks_shard {
                    tracked_shards.push(*shard_id);
                }
            }
            ret.push(tracked_shards);
        }
        ret
    }
}
