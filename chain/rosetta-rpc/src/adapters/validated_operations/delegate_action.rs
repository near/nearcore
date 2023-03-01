use super::ValidatedOperation;

pub(crate) struct DelegateActionOperation {
    pub(crate) account: crate::models::AccountIdentifier,
    pub(crate) operations: Vec<NonDelegateActionOperation>,
    pub(crate) max_block_height: near_primitives::types::BlockHeight,
    pub(crate) public_key: crate::models::PublicKey,
}

impl ValidatedOperation for DelegateActionOperation {
    const OPERATION_TYPE: crate::models::OperationType =
        crate::models::OperationType::DelegateAction;

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
                max_block_height: Some(self.max_block_height),
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
        "DELEGATE_ACTION operation requires `public_key`, 'max_block_height' being passed in the metadata".into(),
    )
}
impl TryFrom<crate::models::Operation> for DelegateActionOperation {
    type Error = crate::errors::ErrorKind;

    fn try_from(operation: crate::models::Operation) -> Result<Self, Self::Error> {
        Self::validate_operation_type(operation.type_)?;
        let metadata = operation.metadata.ok_or_else(required_fields_error)?;
        let public_key = metadata.public_key.ok_or_else(required_fields_error)?;
        let max_block_height = metadata.max_block_height.ok_or_else(required_fields_error)?;

        Ok(Self { account: operation.account, public_key, max_block_height })
    }
}

pub struct NonDelegateActionOperation(crate::models::Operation);

impl From<NonDelegateActionOperation> for crate::models::Operation {
    fn from(action: NonDelegateActionOperation) -> Self {
        action.0
    }
}
