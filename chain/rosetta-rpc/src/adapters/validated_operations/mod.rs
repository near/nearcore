pub(crate) use self::add_key::AddKeyOperation;
pub(crate) use self::create_account::CreateAccountOperation;
pub(crate) use self::delegate_action::DelegateActionOperation;
pub(crate) use self::delete_account::DeleteAccountOperation;
pub(crate) use self::delete_key::DeleteKeyOperation;
pub(crate) use self::deploy_contract::DeployContractOperation;
pub(crate) use self::function_call::FunctionCallOperation;
pub(crate) use self::initiate_add_key::InitiateAddKeyOperation;
pub(crate) use self::initiate_create_account::InitiateCreateAccountOperation;
pub(crate) use self::initiate_delete_account::InitiateDeleteAccountOperation;
pub(crate) use self::initiate_delete_key::InitiateDeleteKeyOperation;
pub(crate) use self::initiate_deploy_contract::InitiateDeployContractOperation;
pub(crate) use self::initiate_function_call::InitiateFunctionCallOperation;
pub(crate) use self::intitiate_signed_delegate_action::InitiateSignedDelegateActionOperation;
pub(crate) use self::refund_delete_account::RefundDeleteAccountOperation;
pub(crate) use self::stake::StakeOperation;
pub(crate) use self::transfer::TransferOperation;

mod add_key;
mod create_account;
pub mod delegate_action;
mod delete_account;
mod delete_key;
mod deploy_contract;
mod function_call;
mod initiate_add_key;
mod initiate_create_account;
pub mod initiate_delegate_action;
mod initiate_delete_account;
mod initiate_delete_key;
mod initiate_deploy_contract;
mod initiate_function_call;
pub mod intitiate_signed_delegate_action;
mod refund_delete_account;
pub mod signed_delegate_action;
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
    ) -> crate::errors::Result<()> {
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
