use super::action::{
    Action, AddKeyAction, CreateAccountAction, DeleteAccountAction, DeleteKeyAction,
    DeployContractAction, FunctionCallAction, StakeAction, TransferAction,
};
use super::errors::HostError;
use super::logic;
use super::types::ReceiptIndex;
use super::DataReceiver;
use super::External;
use near_crypto::PublicKey;
use near_primitives_core::account::{AccessKey, AccessKeyPermission, FunctionCallPermission};
use near_primitives_core::hash::CryptoHash;
use near_primitives_core::types::{AccountId, Gas};
use near_primitives_core::types::{Balance, Nonce};
use near_primitives_core::types::{GasDistribution, GasWeight};

type ActionReceipts = Vec<(AccountId, ReceiptMetadata)>;

#[derive(Debug, Clone, PartialEq)]
pub struct ReceiptMetadata {
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
}

#[derive(Default, Clone, PartialEq)]
pub(super) struct ReceiptManager {
    pub(super) action_receipts: ActionReceipts,
    gas_weights: Vec<(FunctionCallActionIndex, GasWeight)>,
}

/// Indexes the [`ReceiptManager`]'s action receipts and actions.
#[derive(Debug, Clone, Copy, PartialEq)]
struct FunctionCallActionIndex {
    /// Index of [`ReceiptMetadata`] in the action receipts of [`ReceiptManager`].
    receipt_index: usize,
    /// Index of the [`Action`] within the [`ReceiptMetadata`].
    action_index: usize,
}

fn get_fuction_call_action_mut(
    action_receipts: &mut ActionReceipts,
    index: FunctionCallActionIndex,
) -> &mut FunctionCallAction {
    let FunctionCallActionIndex { receipt_index, action_index } = index;
    if let Some(Action::FunctionCall(action)) = action_receipts
        .get_mut(receipt_index)
        .and_then(|(_, receipt)| receipt.actions.get_mut(action_index))
    {
        action
    } else {
        panic!(
            "Invalid function call index \
             (promise_index={receipt_index}, action_index={action_index})",
        );
    }
}

impl ReceiptManager {
    pub(super) fn get_receipt_receiver(&self, receipt_index: ReceiptIndex) -> &AccountId {
        self.action_receipts
            .get(receipt_index as usize)
            .map(|(id, _)| id)
            .expect("receipt index should be valid for getting receiver")
    }

    /// Appends an action and returns the index the action was inserted in the receipt
    fn append_action(&mut self, receipt_index: ReceiptIndex, action: Action) -> usize {
        let actions = &mut self
            .action_receipts
            .get_mut(receipt_index as usize)
            .expect("receipt index should be present")
            .1
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
    pub(super) fn create_receipt(
        &mut self,
        ext: &mut dyn External,
        receipt_indices: Vec<ReceiptIndex>,
        receiver_id: AccountId,
    ) -> logic::Result<ReceiptIndex> {
        let mut input_data_ids = vec![];
        for receipt_index in receipt_indices {
            let data_id = ext.generate_data_id();
            self.action_receipts
                .get_mut(receipt_index as usize)
                .ok_or_else(|| HostError::InvalidReceiptIndex { receipt_index })?
                .1
                .output_data_receivers
                .push(DataReceiver { data_id, receiver_id: receiver_id.clone() });
            input_data_ids.push(data_id);
        }

        let new_receipt =
            ReceiptMetadata { output_data_receivers: vec![], input_data_ids, actions: vec![] };
        let new_receipt_index = self.action_receipts.len() as ReceiptIndex;
        self.action_receipts.push((receiver_id, new_receipt));
        Ok(new_receipt_index)
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
    ) -> logic::Result<()> {
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
    ) -> logic::Result<()> {
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
    ) -> logic::Result<()> {
        let action_index = self.append_action(
            receipt_index,
            Action::FunctionCall(FunctionCallAction {
                method_name: String::from_utf8(method_name)
                    .map_err(|_| HostError::InvalidMethodName)?,
                args,
                gas: prepaid_gas,
                deposit: attached_deposit,
            }),
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
    ) -> logic::Result<()> {
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
        self.append_action(receipt_index, Action::Stake(StakeAction { stake, public_key }));
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
            Action::AddKey(AddKeyAction {
                public_key,
                access_key: AccessKey { nonce, permission: AccessKeyPermission::FullAccess },
            }),
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
    ) -> logic::Result<()> {
        self.append_action(
            receipt_index,
            Action::AddKey(AddKeyAction {
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
            }),
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
        self.append_action(receipt_index, Action::DeleteKey(DeleteKeyAction { public_key }));
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
    ) -> logic::Result<()> {
        self.append_action(
            receipt_index,
            Action::DeleteAccount(DeleteAccountAction { beneficiary_id }),
        );
        Ok(())
    }

    /// Distribute the gas among the scheduled function calls that specify a gas weight.
    ///
    /// Distributes the gas passed in by splitting it among weights defined in `gas_weights`.
    /// This will sum all weights, retrieve the gas per weight, then update each function
    /// to add the respective amount of gas. Once all gas is distributed, the remainder of
    /// the gas not assigned due to precision loss is added to the last function with a weight.
    ///
    /// # Arguments
    ///
    /// * `gas` - amount of unused gas to distribute
    ///
    /// # Returns
    ///
    /// Function returns a [GasDistribution] that indicates how the gas was distributed.
    pub(super) fn distribute_unused_gas(&mut self, unused_gas: Gas) -> GasDistribution {
        let gas_weight_sum: u128 =
            self.gas_weights.iter().map(|(_, GasWeight(weight))| *weight as u128).sum();

        if gas_weight_sum == 0 {
            return GasDistribution::NoRatios;
        }

        let mut distribute_gas = |index: &FunctionCallActionIndex, assigned_gas: Gas| {
            let FunctionCallAction { gas, .. } =
                get_fuction_call_action_mut(&mut self.action_receipts, *index);

            // Operation cannot overflow because the amount of assigned gas is a fraction of
            // the unused gas and is using floor division.
            *gas += assigned_gas;
        };

        let mut distributed = 0;
        for (action_index, GasWeight(weight)) in &self.gas_weights {
            // Multiplication is done in u128 with max values of u64::MAX so this cannot overflow.
            // Division result can be truncated to 64 bits because gas_weight_sum >= weight.
            let assigned_gas = (unused_gas as u128 * *weight as u128 / gas_weight_sum) as u64;

            distribute_gas(action_index, assigned_gas as u64);

            distributed += assigned_gas
        }

        // Distribute remaining gas to final action.
        if let Some((last_idx, _)) = self.gas_weights.last() {
            distribute_gas(last_idx, unused_gas - distributed);
        }
        self.gas_weights.clear();
        GasDistribution::All
    }
}
