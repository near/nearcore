use std::fmt;

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum Error {
    AccountNonceExhausted,
    AccountId(AccountIdError),
    Relayer(RelayerError),
    User(UserError),
}

/// Errors that should never happen when the Eth Implicit accounts feature
/// is available on Near. These errors relate to parsing a 20-byte address
/// from a Near account ID.
#[derive(Debug, PartialEq, Eq, Clone)]
pub enum AccountIdError {
    AccountIdTooShort,
    Missing0xPrefix,
    InvalidHex,
}

/// Errors which should never happen if the relayer is honest.
/// If these errors happen then we should ban the relayer (revoke their access key).
/// An external caller (as opposed to a relayer with a Function Call access key) may
/// also trigger these errors by passing bad arguments, but this is not an issue
/// (there is no ban list for external callers) because they are paying the gas fees
/// for their own mistakes.
#[derive(Debug, PartialEq, Eq, Clone)]
pub enum RelayerError {
    /// Relayers should always check the nonce before sending
    InvalidNonce,
    /// Relayers should always encode the transaction correctly
    InvalidBase64,
    /// Relayers should always send valid transactions
    TxParsing(aurora_engine_transactions::Error),
    /// Relayers should always send correctly signed transactions
    InvalidSender,
    /// Relayers should always give the correct target account
    InvalidTarget,
    /// Relayers should always check the transaction is signed with the correct chain id.
    InvalidChainId,
    /// Relayers should always attach at least as much gas as the user requested.
    InsufficientGas,
}

/// Errors that arise from problems in the data signed by the user
/// (i.e. in the Ethereum transaction itself). A careful power-user
/// should never see these errors because they can review the data
/// they are signing. If a user does see these errors then there is
/// likely a bug in the front-end code that is constructing the Ethereum
/// transaction to be signed.
#[derive(Debug, PartialEq, Eq, Clone)]
pub enum UserError {
    EvmDeployDisallowed,
    ValueTooLarge,
    UnknownPublicKeyKind,
    InvalidEd25519Key,
    InvalidSecp256k1Key,
    InvalidAccessKeyAccountId,
    UnsupportedAction(UnsupportedAction),
    UnknownFunctionSelector,
    InvalidAbiEncodedData,
    ExcessYoctoNear,
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum UnsupportedAction {
    AddFullAccessKey,
    CreateAccount,
    Delegate,
    DeleteAccount,
    DeployContract,
    Stake,
}

impl From<aurora_engine_transactions::Error> for Error {
    fn from(value: aurora_engine_transactions::Error) -> Self {
        Self::Relayer(RelayerError::TxParsing(value))
    }
}

impl fmt::Display for AccountIdError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::AccountIdTooShort => f.write_str("Error: account ID too short"),
            Self::Missing0xPrefix => f.write_str("Error: account ID missing 0x"),
            Self::InvalidHex => f.write_str("Error: account ID is not valid hex encoding"),
        }
    }
}

impl fmt::Display for RelayerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TxParsing(e) => std::write!(f, "Error parsing RLP {}", e.as_str()),
            Self::InvalidSender => f.write_str("Error: signature is not from account owner"),
            Self::InvalidBase64 => f.write_str("Error: invalid base64 encoding"),
            Self::InvalidTarget => {
                f.write_str("Error: target does not match to in signed transaction")
            }
            Self::InvalidNonce => f.write_str("Error: invalid nonce value"),
            Self::InvalidChainId => f.write_str("Error: invalid chain id value"),
            Self::InsufficientGas => f.write_str("Error: insufficient prepaid gas"),
        }
    }
}

impl fmt::Display for UserError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EvmDeployDisallowed => {
                f.write_str("Error: transactions deploying EVM contracts not allowed")
            }
            Self::ValueTooLarge => {
                f.write_str("Error: transaction value must be representable by 128 bits")
            }
            Self::UnknownPublicKeyKind => f.write_str("Error: unknown public key kind"),
            Self::InvalidEd25519Key => f.write_str("Error: invalid ED25519 public key"),
            Self::InvalidSecp256k1Key => f.write_str("Error: invalid SECP256k1 public key"),
            Self::InvalidAccessKeyAccountId => f.write_str(
                "Error: attempt to add function call access key with invalid account id",
            ),
            Self::UnsupportedAction(a) => {
                std::write!(f, "Error unsupported action {:?}", a)
            }
            Self::UnknownFunctionSelector => f.write_str("Error: unknown function selector"),
            Self::InvalidAbiEncodedData => {
                f.write_str("Error: invalid ABI encoding in transaction data")
            }
            Self::ExcessYoctoNear => f.write_str(
                "Error: only at most 1_000_000 yoctoNear can be included directly in an action",
            ),
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::AccountNonceExhausted => f.write_str("Error: no nonce values remain"),
            Self::AccountId(e) => e.fmt(f),
            Self::Relayer(e) => e.fmt(f),
            Self::User(e) => e.fmt(f),
        }
    }
}

impl std::error::Error for Error {}
