use near_primitives::account::{AccessKey, FunctionCallPermission};
use near_primitives::types::{Nonce, NonceIndex};

/// Abstraction over access key and gas key for transaction verification.
///
/// This type allows `verify_and_charge_tx` to work with both regular access keys
/// and gas keys, handling the different nonce storage mechanisms.
///
/// For access keys: nonce is stored in `AccessKey.nonce`
/// For gas keys: nonce is stored separately in the trie via `TrieKey::GasKeyNonce`
pub enum KeyState<'a> {
    /// Regular access key - nonce is stored in the AccessKey itself
    AccessKey(&'a mut AccessKey),
    /// Gas key - nonce is stored separately, AccessKey is only for permission checks
    GasKey { access_key: &'a mut AccessKey, nonce: Nonce, nonce_index: NonceIndex },
}

impl<'a> KeyState<'a> {
    pub fn from_access_key(access_key: &'a mut AccessKey) -> Self {
        KeyState::AccessKey(access_key)
    }

    pub fn from_gas_key(
        access_key: &'a mut AccessKey,
        nonce_index: NonceIndex,
        nonce: Nonce,
    ) -> Self {
        KeyState::GasKey { access_key, nonce, nonce_index }
    }

    pub fn nonce(&self) -> Nonce {
        match self {
            KeyState::AccessKey(access_key) => access_key.nonce,
            KeyState::GasKey { nonce, .. } => *nonce,
        }
    }

    pub(crate) fn set_nonce(&mut self, new_nonce: Nonce) {
        match self {
            KeyState::AccessKey(access_key) => access_key.nonce = new_nonce,
            KeyState::GasKey { nonce, .. } => *nonce = new_nonce,
        }
    }

    pub fn function_call_permission(&self) -> Option<&FunctionCallPermission> {
        let permission = match self {
            KeyState::AccessKey(access_key) => &access_key.permission,
            KeyState::GasKey { access_key, .. } => &access_key.permission,
        };
        permission.function_call_permission()
    }

    pub fn function_call_permission_mut(&mut self) -> Option<&mut FunctionCallPermission> {
        let permission = match self {
            KeyState::AccessKey(access_key) => &mut access_key.permission,
            KeyState::GasKey { access_key, .. } => &mut access_key.permission,
        };
        permission.function_call_permission_mut()
    }

    /// For gas keys, returns the nonce index and updated nonce value.
    /// For access keys, returns None.
    pub fn gas_key_nonce_update(&self) -> Option<(NonceIndex, Nonce)> {
        match self {
            KeyState::AccessKey(_) => None,
            KeyState::GasKey { nonce, nonce_index, .. } => Some((*nonce_index, *nonce)),
        }
    }
}
