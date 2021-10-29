use super::ValidatedOperation;

pub(crate) struct CreateAccountOperation {
    pub(crate) account: crate::models::AccountIdentifier,
}

impl super::ValidatedOperation for CreateAccountOperation {
    const OPERATION_TYPE: crate::models::OperationType =
        crate::models::OperationType::CreateAccount;

    fn into_operation(
        self,
        operation_identifier: crate::models::OperationIdentifier,
    ) -> crate::models::Operation {
        crate::models::Operation {
            operation_identifier,

            account: self.account,
            amount: None,
            metadata: None,

            related_operations: None,
            type_: Self::OPERATION_TYPE,
            status: None,
        }
    }
}

impl TryFrom<crate::models::Operation> for CreateAccountOperation {
    type Error = crate::errors::ErrorKind;

    fn try_from(operation: crate::models::Operation) -> Result<Self, Self::Error> {
        Self::validate_operation_type(operation.type_)?;
        Ok(Self { account: operation.account })
    }
}
