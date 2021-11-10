use super::ValidatedOperation;

pub(crate) struct DeployContractOperation {
    pub(crate) account: crate::models::AccountIdentifier,
    pub(crate) code: Vec<u8>,
}

impl ValidatedOperation for DeployContractOperation {
    const OPERATION_TYPE: crate::models::OperationType =
        crate::models::OperationType::DeployContract;

    fn into_operation(
        self,
        operation_identifier: crate::models::OperationIdentifier,
    ) -> crate::models::Operation {
        crate::models::Operation {
            operation_identifier,

            account: self.account,
            amount: None,
            metadata: Some(crate::models::OperationMetadata {
                code: Some(self.code.into()),
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
        "DEPLOY_CONTRACT operation requires `code` being passed in the metadata".into(),
    )
}

impl TryFrom<crate::models::Operation> for DeployContractOperation {
    type Error = crate::errors::ErrorKind;

    fn try_from(operation: crate::models::Operation) -> Result<Self, Self::Error> {
        Self::validate_operation_type(operation.type_)?;
        let metadata = operation.metadata.ok_or_else(required_fields_error)?;
        let code = metadata.code.ok_or_else(required_fields_error)?.into_inner();

        Ok(Self { account: operation.account, code })
    }
}
