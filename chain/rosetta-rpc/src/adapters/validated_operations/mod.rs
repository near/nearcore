use std::convert::TryFrom;

pub(crate) use self::{
    add_key::AddKeyOperation, create_account::CreateAccountOperation,
    delete_account::DeleteAccountOperation, delete_key::DeleteKeyOperation,
    deploy_contract::DeployContractOperation, function_call::FunctionCallOperation,
    initiate_add_key::InitiateAddKeyOperation,
    initiate_create_account::InitiateCreateAccountOperation,
    initiate_delete_account::InitiateDeleteAccountOperation,
    initiate_delete_key::InitiateDeleteKeyOperation,
    initiate_deploy_contract::InitiateDeployContractOperation,
    initiate_function_call::InitiateFunctionCallOperation,
    refund_delete_account::RefundDeleteAccountOperation, stake::StakeOperation,
    transfer::TransferOperation,
};

mod add_key;
mod create_account;
mod delete_account;
mod delete_key;
mod deploy_contract;
mod function_call;
mod initiate_add_key;
mod initiate_create_account;
mod initiate_delete_account;
mod initiate_delete_key;
mod initiate_deploy_contract;
mod initiate_function_call;
mod refund_delete_account;
mod stake;
mod transfer;

pub(crate) trait ValidatedOperation:
    TryFrom<crate::models::Operation, Error = crate::errors::ErrorKind>
{
    const OPERATION_TYPE: crate::models::OperationType;

    fn try_from_option(operation: Option<crate::models::Operation>) -> Result<Self, Self::Error> {
        let operation = operation.ok_or_else(|| {
            crate::errors::ErrorKind::InvalidInput(format!(
                "{} operation is missing",
                Into::<&str>::into(Self::OPERATION_TYPE)
            ))
        })?;
        Self::try_from(operation)
    }

    fn validate_operation_type(
        operation_type: crate::models::OperationType,
    ) -> Result<(), crate::errors::ErrorKind> {
        if operation_type == Self::OPERATION_TYPE {
            Ok(())
        } else {
            Err(crate::errors::ErrorKind::InvalidInput(format!(
                "{} operation was expected, but {} found",
                Into::<&str>::into(Self::OPERATION_TYPE),
                Into::<&str>::into(operation_type),
            )))
        }
    }

    fn into_operation(
        self,
        operation_identifier: crate::models::OperationIdentifier,
    ) -> crate::models::Operation;

    fn into_related_operation(
        self,
        operation_identifier: crate::models::OperationIdentifier,
        related_operations: Vec<crate::models::OperationIdentifier>,
    ) -> crate::models::Operation {
        let mut operation = self.into_operation(operation_identifier);
        operation.related_operations = Some(related_operations);
        operation
    }
}
