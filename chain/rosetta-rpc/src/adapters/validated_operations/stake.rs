use super::ValidatedOperation;

pub(crate) struct StakeOperation {
    pub(crate) account: crate::models::AccountIdentifier,
    pub(crate) amount: near_primitives::types::Balance,
    pub(crate) public_key: crate::models::PublicKey,
}

impl ValidatedOperation for StakeOperation {
    const OPERATION_TYPE: crate::models::OperationType = crate::models::OperationType::Stake;

    fn into_operation(
        self,
        operation_identifier: crate::models::OperationIdentifier,
    ) -> crate::models::Operation {
        crate::models::Operation {
            operation_identifier,

            account: self.account,
            amount: Some(crate::models::Amount::from_yoctonear(self.amount)),
            metadata: Some(crate::models::OperationMetadata {
                public_key: Some(self.public_key),
                ..Default::default()
            }),

            related_operations: None,
            type_: Self::OPERATION_TYPE,
            status: None,
        }
    }
}

fn required_fields_error() -> crate::errors::ErrorKind {
    crate::errors::ErrorKind::InvalidInput(
        "STAKE operation requires `public_key` (passed in the metadata) and non-negative `amount`"
            .into(),
    )
}

impl TryFrom<crate::models::Operation> for StakeOperation {
    type Error = crate::errors::ErrorKind;

    fn try_from(operation: crate::models::Operation) -> Result<Self, Self::Error> {
        Self::validate_operation_type(operation.type_)?;
        let amount = operation.amount.ok_or_else(required_fields_error)?;
        let amount = if amount.value.is_positive() {
            amount.value.absolute_difference()
        } else {
            return Err(required_fields_error());
        };
        let metadata = operation.metadata.ok_or_else(required_fields_error)?;
        let public_key = metadata.public_key.ok_or_else(required_fields_error)?;

        Ok(Self { account: operation.account, amount, public_key })
    }
}
