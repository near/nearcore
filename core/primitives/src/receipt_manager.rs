use crate::account::{AccessKey, AccessKeyPermission, FunctionCallPermission};
use crate::hash::CryptoHash;
use crate::receipt::{ActionReceipt, DataReceiver, Receipt, ReceiptEnum};
use crate::transaction::{
    Action, AddKeyAction, CreateAccountAction, DeleteAccountAction, DeleteKeyAction,
    DeployContractAction, FunctionCallAction, StakeAction, TransferAction,
};
use borsh::BorshDeserialize;
use near_crypto::PublicKey;
use near_primitives_core::types::{AccountId, Balance, Gas};
#[cfg(feature = "protocol_feature_function_call_weight")]
use near_primitives_core::types::{GasDistribution, GasWeight};
use near_vm_errors::{HostError, VMLogicError};

type ExtResult<T> = ::std::result::Result<T, VMLogicError>;

#[derive(Debug, Clone, PartialEq)]
struct ReceiptMetadata {
    /// If present, where to route the output data
    output_data_receivers: Vec<DataReceiver>,
    /// A list of the input data dependencies for this Receipt to process.
    /// If all `input_data_ids` for this receipt are delivered to the account
    /// that means we have all the `ReceivedData` input which will be than converted to a
    /// `PromiseResult::Successful(value)` or `PromiseResult::Failed`
    /// depending on `ReceivedData` is `Some(_)` or `None`
    input_data_ids: Vec<CryptoHash>,
    /// A list of actions to process when all input_data_ids are filled
    actions: Vec<Action>,
}

#[derive(Default, Clone, PartialEq)]
pub struct ReceiptManager {
    action_receipts: Vec<(AccountId, ReceiptMetadata)>,
    #[cfg(feature = "protocol_feature_function_call_weight")]
    gas_weights: Vec<(FunctionCallActionIndex, GasWeight)>,
}

#[cfg(feature = "protocol_feature_function_call_weight")]
#[derive(Debug, Clone, Copy, PartialEq)]
struct FunctionCallActionIndex {
    receipt_index: usize,
    action_index: usize,
}

impl ReceiptManager {
    pub fn get_receipt_receiver(&self, receipt_index: u64) -> Option<&AccountId> {
        self.action_receipts.get(receipt_index as usize).map(|(id, _)| id)
    }
    pub fn into_receipts(
        self,
        predecessor_id: &AccountId,
        signer_id: &AccountId,
        signer_public_key: &PublicKey,
        gas_price: Balance,
    ) -> Vec<Receipt> {
        self.action_receipts
            .into_iter()
            .map(|(receiver_id, receipt)| Receipt {
                predecessor_id: predecessor_id.clone(),
                receiver_id,
                // Actual receipt ID is set in the Runtime.apply_action_receipt(...) in the
                // "Generating receipt IDs" section
                receipt_id: CryptoHash::default(),
                receipt: ReceiptEnum::Action(ActionReceipt {
                    signer_id: signer_id.clone(),
                    signer_public_key: signer_public_key.clone(),
                    gas_price,
                    output_data_receivers: receipt.output_data_receivers,
                    input_data_ids: receipt.input_data_ids,
                    actions: receipt.actions,
                }),
            })
            .collect()
    }

    /// Appends an action and returns the index the action was inserted in the receipt
    pub fn append_action(&mut self, receipt_index: u64, action: Action) -> usize {
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

    // TODO pull docs for all these methods into here
    pub fn create_receipt(
        &mut self,
        receipt_indices: Vec<u64>,
        receiver_id: AccountId,
    ) -> ExtResult<u64> {
        let mut input_data_ids = vec![];
        for receipt_index in receipt_indices {
            // let data_id = self.new_data_id();
            // TODO
            let data_id = CryptoHash::default();
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
        let new_receipt_index = self.action_receipts.len() as u64;
        self.action_receipts.push((receiver_id, new_receipt));
        Ok(new_receipt_index)
    }

    pub fn append_action_create_account(&mut self, receipt_index: u64) -> ExtResult<()> {
        self.append_action(receipt_index, Action::CreateAccount(CreateAccountAction {}));
        Ok(())
    }

    pub fn append_action_deploy_contract(
        &mut self,
        receipt_index: u64,
        code: Vec<u8>,
    ) -> ExtResult<()> {
        self.append_action(receipt_index, Action::DeployContract(DeployContractAction { code }));
        Ok(())
    }

    #[cfg(feature = "protocol_feature_function_call_weight")]
    pub fn append_action_function_call_weight(
        &mut self,
        receipt_index: u64,
        method_name: Vec<u8>,
        args: Vec<u8>,
        attached_deposit: u128,
        prepaid_gas: Gas,
        gas_weight: GasWeight,
    ) -> ExtResult<()> {
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

    pub fn append_action_function_call(
        &mut self,
        receipt_index: u64,
        method_name: Vec<u8>,
        args: Vec<u8>,
        attached_deposit: u128,
        prepaid_gas: Gas,
    ) -> ExtResult<()> {
        self.append_action(
            receipt_index,
            Action::FunctionCall(FunctionCallAction {
                method_name: String::from_utf8(method_name)
                    .map_err(|_| HostError::InvalidMethodName)?,
                args,
                gas: prepaid_gas,
                deposit: attached_deposit,
            }),
        );
        Ok(())
    }

    pub fn append_action_transfer(&mut self, receipt_index: u64, deposit: u128) -> ExtResult<()> {
        self.append_action(receipt_index, Action::Transfer(TransferAction { deposit }));
        Ok(())
    }

    pub fn append_action_stake(
        &mut self,
        receipt_index: u64,
        stake: u128,
        public_key: Vec<u8>,
    ) -> ExtResult<()> {
        self.append_action(
            receipt_index,
            Action::Stake(StakeAction {
                stake,
                public_key: PublicKey::try_from_slice(&public_key)
                    .map_err(|_| HostError::InvalidPublicKey)?,
            }),
        );
        Ok(())
    }

    pub fn append_action_add_key_with_full_access(
        &mut self,
        receipt_index: u64,
        public_key: Vec<u8>,
        nonce: u64,
    ) -> ExtResult<()> {
        self.append_action(
            receipt_index,
            Action::AddKey(AddKeyAction {
                public_key: PublicKey::try_from_slice(&public_key)
                    .map_err(|_| HostError::InvalidPublicKey)?,
                access_key: AccessKey { nonce, permission: AccessKeyPermission::FullAccess },
            }),
        );
        Ok(())
    }

    pub fn append_action_add_key_with_function_call(
        &mut self,
        receipt_index: u64,
        public_key: Vec<u8>,
        nonce: u64,
        allowance: Option<u128>,
        receiver_id: AccountId,
        method_names: Vec<Vec<u8>>,
    ) -> ExtResult<()> {
        self.append_action(
            receipt_index,
            Action::AddKey(AddKeyAction {
                public_key: PublicKey::try_from_slice(&public_key)
                    .map_err(|_| HostError::InvalidPublicKey)?,
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

    pub fn append_action_delete_key(
        &mut self,
        receipt_index: u64,
        public_key: Vec<u8>,
    ) -> ExtResult<()> {
        self.append_action(
            receipt_index,
            Action::DeleteKey(DeleteKeyAction {
                public_key: PublicKey::try_from_slice(&public_key)
                    .map_err(|_| HostError::InvalidPublicKey)?,
            }),
        );
        Ok(())
    }

    pub fn append_action_delete_account(
        &mut self,
        receipt_index: u64,
        beneficiary_id: AccountId,
    ) -> ExtResult<()> {
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
    #[cfg(feature = "protocol_feature_function_call_weight")]
    pub fn distribute_unused_gas(&mut self, gas: u64) -> GasDistribution {
        let gas_weight_sum: u128 =
            self.gas_weights.iter().map(|(_, GasWeight(weight))| *weight as u128).sum();
        if gas_weight_sum != 0 {
            // Floor division that will ensure gas allocated is <= gas to distribute
            let gas_per_weight = (gas as u128 / gas_weight_sum) as u64;

            let mut distribute_gas = |metadata: &FunctionCallActionIndex, assigned_gas: u64| {
                let FunctionCallActionIndex { receipt_index, action_index } = metadata;
                if let Some(Action::FunctionCall(FunctionCallAction { ref mut gas, .. })) = self
                    .action_receipts
                    .get_mut(*receipt_index)
                    .and_then(|(_, receipt)| receipt.actions.get_mut(*action_index))
                {
                    *gas += assigned_gas;
                } else {
                    panic!(
                        "Invalid index for assigning unused gas weight \
                        (promise_index={}, action_index={})",
                        receipt_index, action_index
                    );
                }
            };

            let mut distributed = 0;
            for (action_index, GasWeight(weight)) in &self.gas_weights {
                // This can't overflow because the gas_per_weight is floor division
                // of the weight sum.
                let assigned_gas = gas_per_weight * weight;

                distribute_gas(action_index, assigned_gas);

                distributed += assigned_gas
            }

            // Distribute remaining gas to final action.
            if let Some((last_idx, _)) = self.gas_weights.last() {
                distribute_gas(last_idx, gas - distributed);
            }
            self.gas_weights.clear();
            GasDistribution::All
        } else {
            GasDistribution::NoRatios
        }
    }
}
