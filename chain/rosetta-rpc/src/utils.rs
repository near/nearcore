use crate::{
    errors,
    models::{self, AccountBalanceResponseMetadata},
    types::AccountId,
};
use actix::Addr;
use futures::StreamExt;
use near_chain_configs::ProtocolConfigView;
use near_client::ViewClientActor;
use near_o11y::WithSpanContextExt;
use near_primitives::borsh::{BorshDeserialize, BorshSerialize};

#[derive(Debug, Clone, PartialEq, derive_more::AsRef, derive_more::From)]
pub(crate) struct BorshInHexString<T: BorshSerialize + BorshDeserialize>(T);

impl<T> BorshInHexString<T>
where
    T: BorshSerialize + BorshDeserialize,
{
    pub fn into_inner(self) -> T {
        self.0
    }
}

impl<T> paperclip::v2::schema::TypedData for BorshInHexString<T>
where
    T: BorshSerialize + BorshDeserialize,
{
    fn data_type() -> paperclip::v2::models::DataType {
        paperclip::v2::models::DataType::String
    }
}

impl<T> serde::Serialize for BorshInHexString<T>
where
    T: BorshSerialize + BorshDeserialize,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&hex::encode(
            self.0.try_to_vec().expect("borsh serialization should never fail"),
        ))
    }
}

impl<'de, T> serde::Deserialize<'de> for BorshInHexString<T>
where
    T: BorshSerialize + BorshDeserialize,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let blob = hex::decode(<String as serde::Deserialize>::deserialize(deserializer)?)
            .map_err(|err| {
                serde::de::Error::invalid_value(
                    serde::de::Unexpected::Other(&format!(
                        "signed transaction could not be decoded due to: {:?}",
                        err
                    )),
                    &"base64-encoded transaction was expected",
                )
            })?;
        Ok(Self(T::try_from_slice(&blob).map_err(|err| {
            serde::de::Error::invalid_value(
                serde::de::Unexpected::Other(&format!(
                    "signed transaction could not be deserialized due to: {:?}",
                    err
                )),
                &"a valid Borsh-serialized transaction was expected",
            )
        })?))
    }
}

#[derive(Debug, Clone, Eq, PartialEq, derive_more::AsRef, derive_more::From)]
#[as_ref(forward)]
pub(crate) struct BlobInHexString<T: AsRef<[u8]> + From<Vec<u8>>>(T);

impl<T> paperclip::v2::schema::TypedData for BlobInHexString<T>
where
    T: AsRef<[u8]> + From<Vec<u8>>,
{
    fn data_type() -> paperclip::v2::models::DataType {
        paperclip::v2::models::DataType::String
    }
}

impl<T> BlobInHexString<T>
where
    T: AsRef<[u8]> + From<Vec<u8>>,
{
    pub fn into_inner(self) -> T {
        self.0
    }
}

impl<T> serde::Serialize for BlobInHexString<T>
where
    T: AsRef<[u8]> + From<Vec<u8>>,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&hex::encode(self.as_ref()))
    }
}

impl<'de, T> serde::Deserialize<'de> for BlobInHexString<T>
where
    T: AsRef<[u8]> + From<Vec<u8>>,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        Ok(Self(T::from(
            hex::decode(<String as serde::Deserialize>::deserialize(deserializer)?).map_err(
                |err| {
                    serde::de::Error::invalid_value(
                        serde::de::Unexpected::Other(&format!(
                            "the value could not be decoded due to: {:?}",
                            err
                        )),
                        &"hex-encoded value was expected",
                    )
                },
            )?,
        )))
    }
}

#[derive(Copy, Clone, PartialEq, Eq)]
pub(crate) struct SignedDiff<T>
where
    T: Copy + PartialEq,
{
    is_positive: bool,
    absolute_difference: T,
}

impl<T> paperclip::v2::schema::TypedData for SignedDiff<T>
where
    T: Copy + PartialEq,
{
    fn data_type() -> paperclip::v2::models::DataType {
        paperclip::v2::models::DataType::String
    }
}

impl From<u64> for SignedDiff<u64> {
    fn from(value: u64) -> Self {
        Self { is_positive: true, absolute_difference: value }
    }
}

impl From<u128> for SignedDiff<u128> {
    fn from(value: u128) -> Self {
        Self { is_positive: true, absolute_difference: value }
    }
}

impl<T> SignedDiff<T>
where
    T: Copy + PartialEq + std::ops::Sub<Output = T> + std::cmp::Ord,
{
    pub fn cmp(lhs: T, rhs: T) -> Self {
        if lhs <= rhs {
            Self { is_positive: true, absolute_difference: rhs - lhs }
        } else {
            Self { is_positive: false, absolute_difference: lhs - rhs }
        }
    }

    pub fn is_positive(&self) -> bool {
        self.is_positive
    }

    pub fn absolute_difference(&self) -> T {
        self.absolute_difference
    }
}

impl<T> std::fmt::Display for SignedDiff<T>
where
    T: Copy + PartialEq + std::string::ToString,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}{}",
            if self.is_positive { "" } else { "-" },
            self.absolute_difference.to_string()
        )
    }
}

impl<T> std::fmt::Debug for SignedDiff<T>
where
    T: Copy + PartialEq + std::string::ToString,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SignedDiff({})", self)
    }
}

impl<T> std::ops::Neg for SignedDiff<T>
where
    T: Copy + PartialEq,
{
    type Output = Self;

    fn neg(mut self) -> Self::Output {
        self.is_positive = !self.is_positive;
        self
    }
}

impl<T> serde::Serialize for SignedDiff<T>
where
    T: Copy + PartialEq + std::string::ToString,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de, T> serde::Deserialize<'de> for SignedDiff<T>
where
    T: Copy + PartialEq + std::str::FromStr,
    T::Err: std::fmt::Debug,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let string_value = <String as serde::Deserialize>::deserialize(deserializer)?;
        let mut chars_value = string_value.chars();
        if let Some(first_char) = chars_value.next() {
            let (is_positive, absolute_difference) = if first_char == '-' {
                (false, chars_value.as_str())
            } else {
                (true, string_value.as_str())
            };
            Ok(Self {
                is_positive,
                absolute_difference: absolute_difference.parse().map_err(|err| {
                    serde::de::Error::invalid_value(
                        serde::de::Unexpected::Other(&format!(
                            "the value could not be decoded due to: {:?}",
                            err
                        )),
                        &"an integer value was expected in range of [-u128::MAX; +u128::MAX]",
                    )
                })?,
            })
        } else {
            Err(serde::de::Error::invalid_value(
                serde::de::Unexpected::Other("empty value is not a valid number"),
                &"a non-empty value was expected",
            ))
        }
    }
}

/// Zero-balance account (NEP-448)
fn is_zero_balance_account(account: &near_primitives::account::Account) -> bool {
    account.storage_usage() <= node_runtime::ZERO_BALANCE_ACCOUNT_STORAGE_LIMIT
}

/// Tokens not locked due to staking (=liquid) but reserved for state.
fn get_liquid_balance_for_storage(
    account: &near_primitives::account::Account,
    storage_amount_per_byte: near_primitives::types::Balance,
) -> near_primitives::types::Balance {
    let staked_for_storage = if is_zero_balance_account(account) {
        0
    } else {
        near_primitives::types::Balance::from(account.storage_usage()) * storage_amount_per_byte
    };

    staked_for_storage.saturating_sub(account.locked())
}

pub(crate) struct RosettaAccountBalances {
    pub liquid: near_primitives::types::Balance,
    pub liquid_for_storage: near_primitives::types::Balance,
    pub locked: near_primitives::types::Balance,
}

impl RosettaAccountBalances {
    pub fn zero() -> Self {
        Self { liquid: 0, liquid_for_storage: 0, locked: 0 }
    }

    pub fn from_account<T: Into<near_primitives::account::Account>>(
        account: T,
        runtime_config: &near_primitives::views::RuntimeConfigView,
    ) -> Self {
        let account = account.into();
        let amount = account.amount();
        let locked = account.locked();
        let liquid_for_storage =
            get_liquid_balance_for_storage(&account, runtime_config.storage_amount_per_byte);

        Self { liquid_for_storage, liquid: amount.saturating_sub(liquid_for_storage), locked }
    }
}

pub(crate) async fn query_account(
    block_id: near_primitives::types::BlockReference,
    account_id: near_primitives::types::AccountId,
    view_client_addr: &Addr<ViewClientActor>,
) -> Result<
    (
        near_primitives::hash::CryptoHash,
        near_primitives::types::BlockHeight,
        near_primitives::views::AccountView,
    ),
    crate::errors::ErrorKind,
> {
    let query = near_client::Query::new(
        block_id,
        near_primitives::views::QueryRequest::ViewAccount { account_id },
    );
    let account_info_response = match view_client_addr.send(query.with_span_context()).await? {
        Ok(query_response) => query_response,
        Err(err) => match err {
            near_client_primitives::types::QueryError::UnknownAccount { .. } => {
                return Err(crate::errors::ErrorKind::NotFound(err.to_string()))
            }
            _ => return Err(crate::errors::ErrorKind::InternalError(err.to_string())),
        },
    };

    match account_info_response.kind {
        near_primitives::views::QueryResponseKind::ViewAccount(account_info) => {
            Ok((account_info_response.block_hash, account_info_response.block_height, account_info))
        }
        _ => Err(crate::errors::ErrorKind::InternalInvariantError(format!(
            "queried ViewAccount, but received {:?}.",
            account_info_response.kind
        ))),
    }
}

pub(crate) async fn query_accounts<R>(
    block_id: &near_primitives::types::BlockReference,
    account_ids: impl Iterator<Item = &near_primitives::types::AccountId>,
    view_client_addr: &Addr<ViewClientActor>,
) -> Result<R, crate::errors::ErrorKind>
where
    R: std::iter::FromIterator<(
        near_primitives::types::AccountId,
        near_primitives::views::AccountView,
    )>,
{
    futures::stream::iter(account_ids)
        .map(|account_id| async move {
            let (_, _, account_info) =
                query_account(block_id.clone(), account_id.clone(), view_client_addr).await?;
            Ok((account_id.clone(), account_info))
        })
        .buffer_unordered(10)
        .collect::<Vec<
            Result<
                (near_primitives::types::AccountId, near_primitives::views::AccountView),
                crate::errors::ErrorKind,
            >,
        >>()
        .await
        .into_iter()
        .filter(|account_info| !matches!(account_info, Err(crate::errors::ErrorKind::NotFound(_))))
        .collect()
}

pub(crate) async fn query_access_key(
    block_id: near_primitives::types::BlockReference,
    account_id: near_primitives::types::AccountId,
    public_key: near_crypto::PublicKey,
    view_client_addr: &Addr<ViewClientActor>,
) -> Result<
    (
        near_primitives::hash::CryptoHash,
        near_primitives::types::BlockHeight,
        near_primitives::views::AccessKeyView,
    ),
    crate::errors::ErrorKind,
> {
    let access_key_query = near_client::Query::new(
        block_id,
        near_primitives::views::QueryRequest::ViewAccessKey { account_id, public_key },
    );
    let access_key_query_response =
        match view_client_addr.send(access_key_query.with_span_context()).await? {
            Ok(query_response) => query_response,
            Err(err) => {
                return match err {
                    near_client_primitives::types::QueryError::UnknownAccount { .. }
                    | near_client_primitives::types::QueryError::UnknownAccessKey { .. } => {
                        Err(crate::errors::ErrorKind::NotFound(err.to_string()))
                    }
                    _ => Err(crate::errors::ErrorKind::InternalError(err.to_string())),
                }
            }
        };

    match access_key_query_response.kind {
        near_primitives::views::QueryResponseKind::AccessKey(access_key) => Ok((
            access_key_query_response.block_hash,
            access_key_query_response.block_height,
            access_key,
        )),
        _ => Err(crate::errors::ErrorKind::InternalInvariantError(
            "queried ViewAccessKey, but received something else.".to_string(),
        )),
    }
}

pub(crate) async fn query_protocol_config(
    block_hash: near_primitives::hash::CryptoHash,
    view_client_addr: &Addr<ViewClientActor>,
) -> crate::errors::Result<ProtocolConfigView> {
    view_client_addr
        .send(
            near_client::GetProtocolConfig(near_primitives::types::BlockReference::from(
                near_primitives::types::BlockId::Hash(block_hash),
            ))
            .with_span_context(),
        )
        .await?
        .map_err(|err| crate::errors::ErrorKind::NotFound(err.to_string()))
}

/// This is a helper to ensure that all the values you try to assign are the
/// same, and return an error otherwise (useful in ensuring that all the
/// "sender" Operations have the same account).
pub(crate) struct InitializeOnce<'a, T>
where
    T: std::fmt::Debug + Eq + ToOwned<Owned = T>,
{
    error_message: &'a str,
    known_value: Option<T>,
}

impl<'a, T> InitializeOnce<'a, T>
where
    T: std::fmt::Debug + Eq + ToOwned<Owned = T>,
{
    pub fn new(error_message: &'a str) -> Self {
        Self { error_message, known_value: None }
    }

    pub fn try_set(&mut self, new_value: &T) -> crate::errors::Result<()> {
        if let Some(ref known_value) = self.known_value {
            if new_value != known_value {
                Err(crate::errors::ErrorKind::InvalidInput(format!(
                    "{} ('{:?}' and '{:?}')",
                    self.error_message, new_value, known_value
                )))
            } else {
                Ok(())
            }
        } else {
            self.known_value = Some(new_value.to_owned());
            Ok(())
        }
    }

    pub fn into_inner(self) -> Option<T> {
        self.known_value
    }
}

impl<'a, T> AsRef<Option<T>> for InitializeOnce<'a, T>
where
    T: std::fmt::Debug + std::clone::Clone + std::cmp::Eq,
{
    fn as_ref(&self) -> &Option<T> {
        &self.known_value
    }
}

/// Get a block with `block_id`.
/// Returns `Ok(Some(_))` if the block exists and is final.
/// Returns `Ok(None)` if the block does not exist or is not final.
pub(crate) async fn get_block_if_final(
    block_id: &near_primitives::types::BlockReference,
    view_client_addr: &Addr<ViewClientActor>,
) -> Result<Option<near_primitives::views::BlockView>, models::Error> {
    let final_block = get_final_block(view_client_addr).await?;
    let is_query_by_height = match block_id {
        near_primitives::types::BlockReference::Finality(
            near_primitives::types::Finality::Final,
        ) => return Ok(Some(final_block)),
        near_primitives::types::BlockReference::BlockId(
            near_primitives::types::BlockId::Height(height),
        ) => {
            if height > &final_block.header.height {
                return Ok(None);
            }
            if height == &final_block.header.height {
                return Ok(Some(final_block));
            }
            true
        }
        _ => false,
    };
    let block = match view_client_addr
        .send(near_client::GetBlock(block_id.clone()).with_span_context())
        .await?
    {
        Ok(block) => block,
        Err(near_client_primitives::types::GetBlockError::UnknownBlock { .. }) => return Ok(None),
        Err(err) => return Err(errors::ErrorKind::InternalError(err.to_string()).into()),
    };
    // if block height is larger than the last final block height, then the block is not final
    if block.header.height > final_block.header.height {
        return Ok(None);
    }
    // check that this block is on the canonical chain
    if is_query_by_height {
        return Ok(Some(block));
    }
    let block_on_canonical_chain = view_client_addr
        .send(
            near_client::GetBlock(
                near_primitives::types::BlockId::Height(block.header.height).into(),
            )
            .with_span_context(),
        )
        .await?
        .map_err(|_| errors::ErrorKind::InternalError("final block not found".to_string()))?;
    if block.header.hash == block_on_canonical_chain.header.hash {
        Ok(Some(block))
    } else {
        Ok(None)
    }
}

pub(crate) async fn get_final_block(
    view_client_addr: &Addr<ViewClientActor>,
) -> Result<near_primitives::views::BlockView, errors::ErrorKind> {
    view_client_addr
        .send(
            near_client::GetBlock(near_primitives::types::BlockReference::Finality(
                near_primitives::types::Finality::Final,
            ))
            .with_span_context(),
        )
        .await?
        .map_err(|_| errors::ErrorKind::InternalError("final block not found".to_string()))
}

pub(crate) async fn get_nonces(
    view_client_addr: &Addr<ViewClientActor>,
    account_id: AccountId,
    public_keys: Vec<models::PublicKey>,
) -> Result<AccountBalanceResponseMetadata, models::Error> {
    let mut nonces = Vec::with_capacity(public_keys.len());
    for public_key in public_keys {
        let account_id_for_public_key = account_id.clone();
        let (_block_hash, _block_height, access_key) = crate::utils::query_access_key(
            near_primitives::types::BlockReference::latest(),
            account_id_for_public_key.into(),
            (&public_key).try_into().map_err(|err| {
                errors::ErrorKind::InvalidInput(format!(
                    "public key could not be parsed due to: {:?}",
                    err
                ))
            })?,
            view_client_addr,
        )
        .await?;
        nonces.push(access_key.nonce);
    }
    Ok(models::AccountBalanceResponseMetadata { nonces })
}
