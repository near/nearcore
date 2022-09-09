use borsh::{BorshDeserialize, BorshSerialize};
use near_primitives::transaction::ExecutionOutcomeWithIdAndProof;
use rocksdb::MergeOperands;

use crate::DBCol;

use super::RocksDB;

pub(crate) fn merge_transaction_result<'a>(
    existing: Option<&'a [u8]>,
    operands: impl IntoIterator<Item = &'a [u8]>,
) -> Vec<u8> {
    let mut result: Vec<ExecutionOutcomeWithIdAndProof> = vec![];
    for operand in existing.into_iter().chain(operands.into_iter()) {
        result.extend(
            <Vec<ExecutionOutcomeWithIdAndProof>>::try_from_slice(operand)
                .expect("Failed to deserialize during merge"),
        );
    }
    result.try_to_vec().expect("Borsh shouldn't fail")
}

impl RocksDB {
    pub fn get_merge_operator_for_supporting_column(
        col: DBCol,
    ) -> fn(&[u8], Option<&[u8]>, &MergeOperands) -> Option<Vec<u8>> {
        match col {
            DBCol::TransactionResult => |_, existing, operands| {
                Some(DBCol::TransactionResult.apply_merge(existing, operands))
            },
            _ => unreachable!(),
        }
    }
}
