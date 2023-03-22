use super::ValidatedOperation;

pub(crate) struct FunctionCallOperation {
    pub(crate) account: crate::models::AccountIdentifier,
    pub(crate) method_name: String,
    pub(crate) args: Vec<u8>,
    pub(crate) attached_gas: near_primitives::types::Gas,
    pub(crate) attached_amount: near_primitives::types::Balance,
}

impl ValidatedOperation for FunctionCallOperation {
    const OPERATION_TYPE: crate::models::OperationType = crate::models::OperationType::FunctionCall;

    fn into_operation(
        self,
        operation_identifier: crate::models::OperationIdentifier,
    ) -> crate::models::Operation {
        crate::models::Operation {
            operation_identifier,

            account: self.account,
            amount: if self.attached_amount > 0 {
                Some(crate::models::Amount::from_yoctonear(self.attached_amount))
            } else {
                None
            },
            metadata: Some(crate::models::OperationMetadata {
                method_name: Some(self.method_name),
                args: Some(self.args.into()),
                attached_gas: Some(self.attached_gas.into()),
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
        "FUNCTION_CALL operation requires `method_name`, `args`, and `attached_gas` being passed in the metadata".into(),
    )
}

impl TryFrom<crate::models::Operation> for FunctionCallOperation {
    type Error = crate::errors::ErrorKind;

    fn try_from(operation: crate::models::Operation) -> Result<Self, Self::Error> {
        Self::validate_operation_type(operation.type_)?;
        let metadata = operation.metadata.ok_or_else(required_fields_error)?;
        let method_name = metadata.method_name.ok_or_else(required_fields_error)?;
        let args = metadata.args.ok_or_else(required_fields_error)?.into_inner();
        let attached_gas = metadata.attached_gas.ok_or_else(required_fields_error)?;
        let attached_gas = if attached_gas.is_positive() {
            attached_gas.absolute_difference()
        } else {
            return Err(crate::errors::ErrorKind::InvalidInput(
                "FUNCTION_CALL operation requires `attached_gas` to be positive".into(),
            ));
        };
        let attached_amount = if let Some(ref attached_amount) = operation.amount {
            if !attached_amount.value.is_positive() {
                return Err(crate::errors::ErrorKind::InvalidInput(
                    "FUNCTION_CALL operations must have non-negative `amount`".to_string(),
                ));
            }
            attached_amount.value.absolute_difference()
        } else {
            0
        };

        Ok(Self { account: operation.account, method_name, args, attached_gas, attached_amount })
    }
}
