use near_sdk::{
    borsh::{BorshDeserialize, BorshSerialize},
    env, near_bindgen,
    store::{lookup_map::Entry, LookupMap},
    AccountId, BorshStorageKey, PanicOnDefault,
};

type Address = [u8; 20];

#[derive(BorshSerialize, BorshStorageKey)]
#[borsh(crate = "near_sdk::borsh")]
enum StorageKey {
    Addresses,
}

#[near_bindgen]
#[derive(PanicOnDefault, BorshDeserialize, BorshSerialize)]
#[borsh(crate = "near_sdk::borsh")]
pub struct AddressRegistrar {
    pub addresses: LookupMap<Address, AccountId>,
}

#[near_bindgen]
impl AddressRegistrar {
    #[init]
    pub fn new() -> Self {
        Self { addresses: LookupMap::new(StorageKey::Addresses) }
    }

    /// Computes the address associated with the given `account_id` and
    /// attempts to store the mapping `address -> account_id`. If there is
    /// a collision where the given `account_id` has the same address as a
    /// previously registered one then the mapping is NOT updated and `None`
    /// is returned. Otherwise, the mapping is stored and the address is
    /// returned as a hex-encoded string with `0x` prefix.
    pub fn register(&mut self, account_id: AccountId) -> Option<String> {
        // It is not allowed to register eth-implicit accounts because the purpose
        // of the registry is to allow looking up the named account associated with
        // an address obtained via hashing, but eth-implicit accounts are already
        // parsable as addresses.
        if is_eth_implicit(&account_id) {
            let log_message = format!("Refuse to register eth-implicit account {account_id}");
            env::log_str(&log_message);
            return None;
        }

        let address = account_id_to_address(&account_id);

        match self.addresses.entry(address) {
            Entry::Vacant(entry) => {
                let address = format!("0x{}", hex::encode(address));
                let log_message = format!("Added entry {} -> {}", address, account_id);
                entry.insert(account_id);
                env::log_str(&log_message);
                Some(address)
            }
            Entry::Occupied(entry) => {
                let log_message = format!(
                    "Address collision between {} and {}. Keeping the former.",
                    entry.get(),
                    account_id
                );
                env::log_str(&log_message);
                None
            }
        }
    }

    /// Attempt to look up the account ID associated with the given address.
    /// If an entry for that address is found then the associated account id
    /// is returned, otherwise `None` is returned. Use the `register` method
    /// to add entries to the map.
    /// This function will panic if the given address is not the hex-encoding
    /// of a 20-byte array. The `0x` prefix is optional.
    pub fn lookup(&self, address: String) -> Option<AccountId> {
        let address = {
            let mut buf = [0u8; 20];
            hex::decode_to_slice(address.strip_prefix("0x").unwrap_or(&address), &mut buf)
                .unwrap_or_else(|_| env::panic_str("Invalid hex encoding"));
            buf
        };
        self.addresses.get(&address).cloned()
    }

    /// Computes the address associated with the given `account_id` and
    /// returns it as a hex-encoded string with `0x` prefix. This function
    /// does not update the mapping stored in this contract. If you want
    /// to register an account ID use the `register` method.
    pub fn get_address(&self, account_id: AccountId) -> String {
        let address = account_id_to_address(&account_id);
        format!("0x{}", hex::encode(address))
    }
}

fn account_id_to_address(account_id: &AccountId) -> Address {
    let hash = near_sdk::env::keccak256_array(account_id.as_bytes());
    let mut result = [0u8; 20];
    result.copy_from_slice(&hash[12..32]);
    result
}

fn is_eth_implicit(account_id: &AccountId) -> bool {
    let id = account_id.as_str();
    id.len() == 42 && id.starts_with("0x") && id[2..].chars().all(|c| c.is_ascii_hexdigit())
}
