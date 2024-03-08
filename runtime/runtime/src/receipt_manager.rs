use near_crypto::PublicKey;
use near_primitives::action::{
    Action, AddKeyAction, CreateAccountAction, DeleteAccountAction, DeleteKeyAction,
    DeployContractAction, FunctionCallAction, StakeAction, TransferAction,
};
use near_primitives::errors::RuntimeError;
use near_primitives::receipt::DataReceiver;
use near_primitives_core::account::{AccessKey, AccessKeyPermission, FunctionCallPermission};
use near_primitives_core::hash::CryptoHash;
use near_primitives_core::types::{AccountId, Balance, Gas, GasWeight, Nonce};
use near_vm_runner::logic::HostError;
use near_vm_runner::logic::VMLogicError;
use std::collections::HashMap;

use crate::config::safe_add_gas;

/// near_vm_runner::types is not public.
type ReceiptIndex = u64;

type ActionReceipts = Vec<ActionReceiptMetadata>;
type DataReceipts = Vec<DataReceiptMetadata>;

#[derive(Debug, Clone, PartialEq)]
pub struct ActionReceiptMetadata {
    /// Receipt destination
    pub receiver_id: AccountId,
    /// If present, where to route the output data
    pub output_data_receivers: Vec<DataReceiver>,
    /// A list of the input data dependencies for this Receipt to process.
    /// If all `input_data_ids` for this receipt are delivered to the account
    /// that means we have all the `ReceivedData` input which will be than converted to a
    /// `PromiseResult::Successful(value)` or `PromiseResult::Failed`
    /// depending on `ReceivedData` is `Some(_)` or `None`
    pub input_data_ids: Vec<CryptoHash>,
    /// A list of actions to process when all input_data_ids are filled
    pub actions: Vec<Action>,
    /// Indicates whether the receipt should have type Action or PromiseYield
    pub is_promise_yield: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DataReceiptMetadata {
    /// Id under which the receipt should be created
    pub data_id: CryptoHash,
    /// Contents of the receipt
    pub data: Option<Vec<u8>>,
    /// Indicates whether the receipt should have type Data or PromiseResume
    pub is_promise_resume: bool,
}

#[derive(Default, Clone, PartialEq)]
pub struct ReceiptManager {
    pub(super) action_receipts: ActionReceipts,
    pub(super) data_receipts: DataReceipts,
    pub(super) gas_weights: Vec<(FunctionCallActionIndex, GasWeight)>,
    /// For new promise yields, map from input data id to index in `action_receipts`
    promise_yield_receipt_index: HashMap<CryptoHash, usize>,
}

/// Indexes the [`ReceiptManager`]'s action receipts and actions.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(super) struct FunctionCallActionIndex {
    /// Index of [`ReceiptMetadata`] in the action receipts of [`ReceiptManager`].
    pub(super) receipt_index: usize,
    /// Index of the [`Action`] within the [`ReceiptMetadata`].
    pub(super) action_index: usize,
}

impl ReceiptManager {
    pub(super) fn get_receipt_receiver(&self, receipt_index: ReceiptIndex) -> &AccountId {
        &self
            .action_receipts
            .get(receipt_index as usize)
            .expect("receipt index should be valid for getting receiver")
            .receiver_id
    }

    /// Appends an action and returns the index the action was inserted in the receipt
    fn append_action(&mut self, receipt_index: ReceiptIndex, action: Action) -> usize {
        let actions = &mut self
            .action_receipts
            .get_mut(receipt_index as usize)
            .expect("receipt index should be present")
            .actions;

        actions.push(action);

        // Return index that action was inserted at
        actions.len() - 1
    }

    /// Create a receipt which will be executed after all the receipts identified by
    /// `receipt_indices` are complete.
    ///
    /// If any of the [`ReceiptIndex`]es do not refer to a known receipt, this function will fail
    /// with an error.
    ///
    /// # Arguments
    ///
    /// * `generate_data_id` - function to generate a data id to connect receipt output to
    /// * `receipt_indices` - a list of receipt indices the new receipt is depend on
    /// * `receiver_id` - account id of the receiver of the receipt created
    pub(super) fn create_action_receipt(
        &mut self,
        input_data_ids: Vec<CryptoHash>,
        receipt_indices: Vec<ReceiptIndex>,
        receiver_id: AccountId,
    ) -> Result<ReceiptIndex, VMLogicError> {
        assert_eq!(input_data_ids.len(), receipt_indices.len());
        for (data_id, receipt_index) in input_data_ids.iter().zip(receipt_indices.into_iter()) {
            self.action_receipts
                .get_mut(receipt_index as usize)
                .ok_or(HostError::InvalidReceiptIndex { receipt_index })?
                .output_data_receivers
                .push(DataReceiver { data_id: *data_id, receiver_id: receiver_id.clone() });
        }

        let new_receipt = ActionReceiptMetadata {
            receiver_id,
            output_data_receivers: vec![],
            input_data_ids,
            actions: vec![],
            is_promise_yield: false,
        };
        let new_receipt_index = self.action_receipts.len() as ReceiptIndex;
        self.action_receipts.push(new_receipt);
        Ok(new_receipt_index)
    }

    /// Special case of create_receipt used by yielded promises.
    ///
    /// The receipt will be executed after the input data is explicitly submitted by calling
    /// `create_data_receipt` with specified `input_data_id`.
    ///
    /// # Arguments
    ///
    /// * `input_data_id` - data id which will be used to later submit the receipt input
    /// * `receiver_id` - account id of the receiver of the receipt created
    pub(super) fn create_promise_yield_receipt(
        &mut self,
        input_data_id: CryptoHash,
        receiver_id: AccountId,
    ) -> Result<ReceiptIndex, VMLogicError> {
        let new_receipt = ActionReceiptMetadata {
            receiver_id,
            output_data_receivers: vec![],
            input_data_ids: vec![input_data_id],
            actions: vec![],
            is_promise_yield: true,
        };
        let new_receipt_index = self.action_receipts.len();
        self.action_receipts.push(new_receipt);
        self.promise_yield_receipt_index.insert(input_data_id, new_receipt_index);
        Ok(new_receipt_index as ReceiptIndex)
    }

    /// Creates a PromiseResume receipt.
    ///
    /// Should only be used to resolve dependencies created by `create_yielded_action_receipt`.
    ///
    /// # Arguments
    ///
    /// * `data_id` - id of the PromiseResume receipt being submitted
    /// * `data` - contents of the PromiseResume receipt
    pub(super) fn create_promise_resume_receipt(
        &mut self,
        data_id: CryptoHash,
        data: Vec<u8>,
    ) -> Result<(), VMLogicError> {
        self.data_receipts.push(DataReceiptMetadata {
            data_id,
            data: Some(data),
            is_promise_resume: true,
        });
        Ok(())
    }

    /// Resolves a PromiseYield input dependency previously created under given `data_id`,
    /// if it exists.
    ///
    /// # Arguments
    ///
    /// * `data_id` - id of the Data receipt being submitted
    /// * `data` - contents of the Data receipt
    pub(super) fn checked_resolve_promise_yield(
        &mut self,
        data_id: CryptoHash,
        data: Vec<u8>,
    ) -> Result<bool, VMLogicError> {
        Ok(if let Some(receipt_index) = self.promise_yield_receipt_index.remove(&data_id) {
            // Convert existing PromiseYield to a standard Action receipt
            let receipt = &mut self.action_receipts[receipt_index];
            assert!(receipt.is_promise_yield, "receipt should be promise yield");
            receipt.is_promise_yield = false;

            // Create Data receipt delivering the payload
            self.data_receipts.push(DataReceiptMetadata {
                data_id,
                data: Some(data),
                is_promise_resume: false,
            });
            true
        } else {
            false
        })
    }

    /// Attach the [`CreateAccountAction`] action to an existing receipt.
    ///
    /// # Arguments
    ///
    /// * `receipt_index` - an index of Receipt to append an action
    ///
    /// # Panics
    ///
    /// Panics if the `receipt_index` does not refer to a known receipt.
    pub(super) fn append_action_create_account(
        &mut self,
        receipt_index: ReceiptIndex,
    ) -> Result<(), VMLogicError> {
        self.append_action(receipt_index, Action::CreateAccount(CreateAccountAction {}));
        Ok(())
    }

    /// Attach the [`DeployContractAction`] action to an existing receipt.
    ///
    /// # Arguments
    ///
    /// * `receipt_index` - an index of Receipt to append an action
    /// * `code` - a Wasm code to attach
    ///
    /// # Panics
    ///
    /// Panics if the `receipt_index` does not refer to a known receipt.
    pub(super) fn append_action_deploy_contract(
        &mut self,
        receipt_index: ReceiptIndex,
        code: Vec<u8>,
    ) -> Result<(), VMLogicError> {
        self.append_action(receipt_index, Action::DeployContract(DeployContractAction { code }));
        Ok(())
    }

    /// Attach the [`FunctionCallAction`] action to an existing receipt.
    ///
    /// `prepaid_gas` and `gas_weight` can either be specified or both. If a `gas_weight` is
    /// specified, the action should be allocated gas in
    /// [`distribute_unused_gas`](Self::distribute_unused_gas).
    ///
    /// For more information, see [super::VMLogic::promise_batch_action_function_call_weight].
    ///
    /// # Arguments
    ///
    /// * `receipt_index` - an index of Receipt to append an action
    /// * `method_name` - a name of the contract method to call
    /// * `arguments` - a Wasm code to attach
    /// * `attached_deposit` - amount of tokens to transfer with the call
    /// * `prepaid_gas` - amount of prepaid gas to attach to the call
    /// * `gas_weight` - relative weight of unused gas to distribute to the function call action
    ///
    /// # Panics
    ///
    /// Panics if the `receipt_index` does not refer to a known receipt.
    pub(super) fn append_action_function_call_weight(
        &mut self,
        receipt_index: ReceiptIndex,
        method_name: Vec<u8>,
        args: Vec<u8>,
        attached_deposit: Balance,
        prepaid_gas: Gas,
        gas_weight: GasWeight,
    ) -> Result<(), VMLogicError> {
        let action_index = self.append_action(
            receipt_index,
            Action::FunctionCall(Box::new(FunctionCallAction {
                method_name: String::from_utf8(method_name)
                    .map_err(|_| HostError::InvalidMethodName)?,
                args,
                gas: prepaid_gas,
                deposit: attached_deposit,
            })),
        );

        if gas_weight.0 > 0 {
            self.gas_weights.push((
                FunctionCallActionIndex { receipt_index: receipt_index as usize, action_index },
                gas_weight,
            ));
        }

        Ok(())
    }

    /// Attach the [`TransferAction`] action to an existing receipt.
    ///
    /// # Arguments
    ///
    /// * `receipt_index` - an index of Receipt to append an action
    /// * `amount` - amount of tokens to transfer
    ///
    /// # Panics
    ///
    /// Panics if the `receipt_index` does not refer to a known receipt.
    pub(super) fn append_action_transfer(
        &mut self,
        receipt_index: ReceiptIndex,
        deposit: Balance,
    ) -> Result<(), VMLogicError> {
        self.append_action(receipt_index, Action::Transfer(TransferAction { deposit }));
        Ok(())
    }

    /// Attach the [`StakeAction`] action to an existing receipt.
    ///
    /// # Arguments
    ///
    /// * `receipt_index` - an index of Receipt to append an action
    /// * `stake` - amount of tokens to stake
    /// * `public_key` - a validator public key
    ///
    /// # Panics
    ///
    /// Panics if the `receipt_index` does not refer to a known receipt.
    pub(super) fn append_action_stake(
        &mut self,
        receipt_index: ReceiptIndex,
        stake: Balance,
        public_key: PublicKey,
    ) {
        self.append_action(
            receipt_index,
            Action::Stake(Box::new(StakeAction { stake, public_key })),
        );
    }

    /// Attach the [`AddKeyAction`] action to an existing receipt.
    ///
    /// # Arguments
    ///
    /// * `receipt_index` - an index of Receipt to append an action
    /// * `public_key` - a public key for an access key
    /// * `nonce` - a nonce
    ///
    /// # Panics
    ///
    /// Panics if the `receipt_index` does not refer to a known receipt.
    pub(super) fn append_action_add_key_with_full_access(
        &mut self,
        receipt_index: ReceiptIndex,
        public_key: PublicKey,
        nonce: Nonce,
    ) {
        self.append_action(
            receipt_index,
            Action::AddKey(Box::new(AddKeyAction {
                public_key,
                access_key: AccessKey { nonce, permission: AccessKeyPermission::FullAccess },
            })),
        );
    }

    /// Attach the [`AddKeyAction`] action an existing receipt.
    ///
    /// The access key associated with the action will have the
    /// [`AccessKeyPermission::FunctionCall`] permission scope.
    ///
    /// # Arguments
    ///
    /// * `receipt_index` - an index of Receipt to append an action
    /// * `public_key` - a public key for an access key
    /// * `nonce` - a nonce
    /// * `allowance` - amount of tokens allowed to spend by this access key
    /// * `receiver_id` - a contract witch will be allowed to call with this access key
    /// * `method_names` - a list of method names is allowed to call with this access key (empty = any method)
    ///
    /// # Panics
    ///
    /// Panics if the `receipt_index` does not refer to a known receipt.
    pub(super) fn append_action_add_key_with_function_call(
        &mut self,
        receipt_index: ReceiptIndex,
        public_key: PublicKey,
        nonce: Nonce,
        allowance: Option<Balance>,
        receiver_id: AccountId,
        method_names: Vec<Vec<u8>>,
    ) -> Result<(), VMLogicError> {
        self.append_action(
            receipt_index,
            Action::AddKey(Box::new(AddKeyAction {
                public_key,
                access_key: AccessKey {
                    nonce,
                    permission: AccessKeyPermission::FunctionCall(FunctionCallPermission {
                        allowance,
                        receiver_id: receiver_id.into(),
                        method_names: method_names
                            .into_iter()
                            .map(|method_name| {
                                String::from_utf8(method_name)
                                    .map_err(|_| HostError::InvalidMethodName)
                            })
                            .collect::<std::result::Result<Vec<_>, _>>()?,
                    }),
                },
            })),
        );
        Ok(())
    }

    /// Attach the [`DeleteKeyAction`] action to an existing receipt.
    ///
    /// # Arguments
    ///
    /// * `receipt_index` - an index of Receipt to append an action
    /// * `public_key` - a public key for an access key to delete
    ///
    /// # Panics
    ///
    /// Panics if the `receipt_index` does not refer to a known receipt.
    pub(super) fn append_action_delete_key(
        &mut self,
        receipt_index: ReceiptIndex,
        public_key: PublicKey,
    ) {
        self.append_action(
            receipt_index,
            Action::DeleteKey(Box::new(DeleteKeyAction { public_key })),
        );
    }

    /// Attach the [`DeleteAccountAction`] action to an existing receipt
    ///
    /// # Arguments
    ///
    /// * `receipt_index` - an index of Receipt to append an action
    /// * `beneficiary_id` - an account id to which the rest of the funds of the removed account will be transferred
    ///
    /// # Panics
    ///
    /// Panics if the `receipt_index` does not refer to a known receipt.
    pub(super) fn append_action_delete_account(
        &mut self,
        receipt_index: ReceiptIndex,
        beneficiary_id: AccountId,
    ) -> Result<(), VMLogicError> {
        self.append_action(
            receipt_index,
            Action::DeleteAccount(DeleteAccountAction { beneficiary_id }),
        );
        Ok(())
    }

    /// Distribute the provided `gas` between receipts managed by this `ReceiptManager` according
    /// to their assigned weights.
    ///
    /// Returns the amount of gas distributed (either `0` or `unused_gas`.)
    pub(super) fn distribute_gas(&mut self, unused_gas: Gas) -> Result<Gas, RuntimeError> {
        let ReceiptManager {
            action_receipts,
            data_receipts: _,
            gas_weights,
            promise_yield_receipt_index: _,
        } = self;
        let gas_weight_sum: u128 = gas_weights.iter().map(|(_, gv)| u128::from(gv.0)).sum();
        if gas_weight_sum == 0 || unused_gas == 0 {
            return Ok(0);
        }
        let mut distributed = 0u64;
        let mut gas_weight_iterator = gas_weights.iter().peekable();
        loop {
            let Some((index, weight)) = gas_weight_iterator.next() else { break };
            let FunctionCallActionIndex { receipt_index, action_index } = *index;
            let Some(Action::FunctionCall(action)) = action_receipts
                .get_mut(receipt_index)
                .and_then(|receipt| receipt.actions.get_mut(action_index))
            else {
                panic!(
                        "Invalid function call index (promise_index={receipt_index}, action_index={action_index})",
                    );
            };
            let to_assign = (unused_gas as u128 * weight.0 as u128 / gas_weight_sum) as u64;
            action.gas = safe_add_gas(action.gas, to_assign)?;
            distributed = distributed
                .checked_add(to_assign)
                .unwrap_or_else(|| panic!("gas computation overflowed"));
            if gas_weight_iterator.peek().is_none() {
                let remainder = unused_gas.wrapping_sub(distributed);
                distributed = distributed
                    .checked_add(remainder)
                    .unwrap_or_else(|| panic!("gas computation overflowed"));
                action.gas = safe_add_gas(action.gas, remainder)?;
            }
        }
        assert_eq!(unused_gas, distributed);
        Ok(distributed)
    }
}

#[cfg(test)]
mod tests {
    use near_primitives::transaction::Action;
    use near_primitives_core::types::{Gas, GasWeight};

    #[track_caller]
    fn function_call_weight_verify(function_calls: &[(Gas, u64, Gas)], after_distribute: bool) {
        let mut gas_limit = 10_000_000_000u64;

        // Schedule all function calls
        let mut receipt_manager = super::ReceiptManager::default();
        for &(static_gas, gas_weight, _) in function_calls {
            let index = receipt_manager
                .create_action_receipt(vec![], vec![], "rick.test".parse().unwrap())
                .unwrap();
            gas_limit = gas_limit.saturating_sub(static_gas);
            receipt_manager
                .append_action_function_call_weight(
                    index,
                    vec![],
                    vec![],
                    0,
                    static_gas,
                    GasWeight(gas_weight),
                )
                .unwrap();
        }
        let accessor: fn(&(Gas, u64, Gas)) -> Gas = if after_distribute {
            receipt_manager.distribute_gas(gas_limit).unwrap();
            |(_, _, expected)| *expected
        } else {
            |(static_gas, _, _)| *static_gas
        };

        // Assert expected amount of gas was associated with the action
        let mut function_call_gas = 0;
        let mut function_calls_iter = function_calls.iter();
        for receipt in receipt_manager.action_receipts {
            for action in receipt.actions {
                if let Action::FunctionCall(function_call_action) = action {
                    let reference = function_calls_iter.next().unwrap();
                    assert_eq!(function_call_action.gas, accessor(reference));
                    function_call_gas += function_call_action.gas;
                }
            }
        }

        if after_distribute {
            // Verify that all gas was consumed (assumes at least one ratio is provided)
            assert_eq!(function_call_gas, 10_000_000_000u64);
        }
    }

    #[track_caller]
    fn function_call_weight_check(function_calls: &[(Gas, u64, Gas)]) {
        function_call_weight_verify(function_calls, false);
        function_call_weight_verify(function_calls, true);
    }

    #[test]
    fn function_call_weight_basic_cases_test() {
        // Following tests input are in the format (static gas, gas weight, expected gas)
        // and the gas limit is `10_000_000_000`

        // Single function call
        function_call_weight_check(&[(0, 1, 10_000_000_000)]);

        // Single function with static gas
        function_call_weight_check(&[(888, 1, 10_000_000_000)]);

        // Large weight
        function_call_weight_check(&[(0, 88888, 10_000_000_000)]);

        // Weight larger than gas limit
        function_call_weight_check(&[(0, 11u64.pow(14), 10_000_000_000)]);

        // Split two
        function_call_weight_check(&[(0, 3, 6_000_000_000), (0, 2, 4_000_000_000)]);

        // Split two with static gas
        function_call_weight_check(&[(1_000_000, 3, 5_998_600_000), (3_000_000, 2, 4_001_400_000)]);

        // Many different gas weights
        function_call_weight_check(&[
            (1_000_000, 3, 2_699_800_000),
            (3_000_000, 2, 1_802_200_000),
            (0, 1, 899_600_000),
            (1_000_000_000, 0, 1_000_000_000),
            (0, 4, 3_598_400_000),
        ]);

        // Weight over u64 bounds
        function_call_weight_check(&[(0, u64::MAX, 9_999_999_999), (0, 1000, 1)]);

        // Weight over gas limit with three function calls
        function_call_weight_check(&[
            (0, 10_000_000_000, 4_999_999_999),
            (0, 1, 0),
            (0, 10_000_000_000, 5_000_000_001),
        ]);

        // Weights with one zero and one non-zero
        function_call_weight_check(&[(0, 0, 0), (0, 1, 10_000_000_000)])
    }
}
