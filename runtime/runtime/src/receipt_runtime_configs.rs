//! Pricing of the static fees of action receipts.

use crate::ApplyState;
use crate::config::{exec_fee, total_prepaid_send_fees};
use near_parameters::{ActionCosts, ParameterCost, RuntimeConfig};
use near_primitives::errors::IntegerOverflowError;
use near_primitives::receipt::{CreationProtocolVersion, VersionedActionReceipt};
use near_primitives::transaction::Action;
use near_primitives::types::AccountId;

/// The information about the runtime configs of a receipt at two important
/// time points: when the receipt was created and at the current protocol
/// version.
///
/// The creation config is used to price the static fees of the receipt. It's
/// important that the same config is used when the receipt is created and when
/// the receipt is being executed - otherwise tokens could be minted or burned.
///
/// The current config is used to price the compute of the receipt's execution.
pub(crate) struct ReceiptRuntimeConfigs<'a> {
    /// The protocol version the receipt was created at.
    creation_protocol_version: CreationProtocolVersion,
    /// The config of the protocol version the receipt was created at.
    creation: &'a RuntimeConfig,
    /// The config of the current protocol version.
    current: &'a RuntimeConfig,
}

impl<'a> ReceiptRuntimeConfigs<'a> {
    pub(crate) fn new(
        apply_state: &'a ApplyState,
        action_receipt: &VersionedActionReceipt,
    ) -> Self {
        let current_protocol_version = apply_state.current_protocol_version;
        let creation_protocol_version = match action_receipt.creation_protocol_version() {
            Some(creation_protocol_version) => creation_protocol_version,
            None => CreationProtocolVersion(current_protocol_version),
        };
        let CreationProtocolVersion(creation_version) = creation_protocol_version;
        let creation = apply_state.config_store.get_config(creation_version).as_ref();
        let current = apply_state.config.as_ref();
        Self { creation_protocol_version, creation, current }
    }

    /// The protocol version the receipt was created at. A receipt that it
    /// prepays the static fees of, such as the receipt of the inner actions of
    /// a meta transaction, records the same version.
    pub(crate) fn creation_protocol_version(&self) -> CreationProtocolVersion {
        self.creation_protocol_version
    }

    /// The config that prices the gas of the static fees.
    pub(crate) fn creation_config(&self) -> &'a RuntimeConfig {
        self.creation
    }

    /// The config of the current protocol version.
    pub(crate) fn current_config(&self) -> &'a RuntimeConfig {
        self.current
    }

    /// A static fee with the gas priced with the creation config and the
    /// compute with the current one.
    fn pinned_cost(creation_cost: ParameterCost, current_cost: ParameterCost) -> ParameterCost {
        ParameterCost { gas: creation_cost.gas, compute: current_cost.compute }
    }

    /// The execution fee of the receipt itself.
    pub(crate) fn new_action_receipt_exec_fee(&self) -> ParameterCost {
        let creation_fee = self.creation.fees.fee(ActionCosts::new_action_receipt).exec_fee();
        let current_fee = self.current.fees.fee(ActionCosts::new_action_receipt).exec_fee();
        Self::pinned_cost(creation_fee, current_fee)
    }

    /// The execution fee of `action`.
    pub(crate) fn exec_fee(
        &self,
        action: &Action,
        receiver_id: &AccountId,
    ) -> Result<ParameterCost, IntegerOverflowError> {
        let creation_fee = exec_fee(self.creation, action, receiver_id)?;
        let current_fee = exec_fee(self.current, action, receiver_id)?;
        Ok(Self::pinned_cost(creation_fee, current_fee))
    }

    /// The send fees of the inner actions of the meta transactions in
    /// `actions`, which the receipt prepaid.
    pub(crate) fn total_prepaid_send_fees(
        &self,
        actions: &[Action],
    ) -> Result<ParameterCost, IntegerOverflowError> {
        let creation_fees = total_prepaid_send_fees(self.creation, actions)?;
        let current_fees = total_prepaid_send_fees(self.current, actions)?;
        Ok(Self::pinned_cost(creation_fees, current_fees))
    }
}
