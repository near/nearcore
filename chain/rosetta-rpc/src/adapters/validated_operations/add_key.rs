use super::ValidatedOperation;

pub(crate) struct AddKeyOperation {
    pub(crate) account: crate::models::AccountIdentifier,
    pub(crate) public_key: crate::models::PublicKey,
}

impl ValidatedOperation for AddKeyOperation {
    const OPERATION_TYPE: crate::models::OperationType = crate::models::OperationType::AddKey;

    fn into_operation(
        self,
        operation_identifier: crate::models::OperationIdentifier,
    ) -> crate::models::Operation {
        crate::models::Operation {
            operation_identifier,

            account: self.account,
            amount: None,
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
        "ADD_KEY operation requires `public_key` being passed in the metadata".into(),
    )
}

impl TryFrom<crate::models::Operation> for AddKeyOperation {
    type Error = crate::errors::ErrorKind;

    fn try_from(operation: crate::models::Operation) -> Result<Self, Self::Error> {
        Self::validate_operation_type(operation.type_)?;
        let metadata = operation.metadata.ok_or_else(required_fields_error)?;
        let public_key = metadata.public_key.ok_or_else(required_fields_error)?;

        Ok(Self { account: operation.account, public_key })
    }
}
