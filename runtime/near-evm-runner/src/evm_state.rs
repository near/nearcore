use std::collections::{BTreeMap, HashMap, HashSet};

use ethereum_types::{Address, U256};

use borsh::{BorshDeserialize, BorshSerialize};

use crate::errors::EvmError;
use crate::types::RawAddress;
use crate::utils;
use std::io::{Error, Write};

#[derive(Default, Clone, Copy, Debug)]
pub struct EvmAccount {
    pub balance: U256,
    pub nonce: U256,
}

impl BorshSerialize for EvmAccount {
    fn serialize<W: Write>(&self, writer: &mut W) -> Result<(), Error> {
        self.balance.0.serialize(writer)?;
        self.nonce.0.serialize(writer)
    }
}

impl BorshDeserialize for EvmAccount {
    fn deserialize(buf: &mut &[u8]) -> Result<Self, Error> {
        let balance = U256(<[u64; 4]>::deserialize(buf)?);
        let nonce = U256(<[u64; 4]>::deserialize(buf)?);
        Ok(Self { balance, nonce })
    }
}

pub trait EvmState {
    fn code_at(&self, address: &Address) -> Option<Vec<u8>>;
    fn set_code(&mut self, address: &Address, bytecode: &[u8]);

    fn set_account(&mut self, address: &Address, account: &EvmAccount);
    fn get_account(&self, address: &Address) -> Option<EvmAccount>;

    fn balance_of(&self, address: &Address) -> U256 {
        let account = self.get_account(address).unwrap_or_default();
        account.balance.into()
    }

    fn nonce_of(&self, address: &Address) -> U256 {
        let account = self.get_account(address).unwrap_or_default();
        account.nonce.into()
    }

    fn set_nonce(&mut self, address: &Address, nonce: U256) -> Option<U256> {
        let mut account = self.get_account(address).unwrap_or_default();
        account.nonce = nonce;
        self.set_account(address, &account);
        Some(account.nonce)
    }

    fn set_balance(&mut self, address: &Address, balance: U256) {
        let mut account = self.get_account(address).unwrap_or_default();
        account.balance = balance;
        self.set_account(address, &account);
    }

    fn next_nonce(&mut self, address: &Address) -> U256 {
        let mut account = self.get_account(address).unwrap_or_default();
        let nonce = account.nonce;
        account.nonce += U256::from(1);
        self.set_account(address, &account);
        nonce
    }

    fn _read_contract_storage(&self, key: [u8; 52]) -> Option<[u8; 32]>;
    fn read_contract_storage(&self, address: &Address, key: [u8; 32]) -> Option<[u8; 32]> {
        self._read_contract_storage(utils::internal_storage_key(address, key))
    }

    fn _set_contract_storage(&mut self, key: [u8; 52], value: [u8; 32]);

    fn set_contract_storage(&mut self, address: &Address, key: [u8; 32], value: [u8; 32]) {
        self._set_contract_storage(utils::internal_storage_key(address, key), value)
    }

    fn commit_changes(&mut self, other: &StateStore);

    fn add_balance(&mut self, address: &Address, incr: U256) -> Result<(), EvmError> {
        let mut account = self.get_account(address).unwrap_or_default();
        account.balance =
            account.balance.checked_add(incr).ok_or_else(|| EvmError::IntegerOverflow)?;
        self.set_account(address, &account);
        Ok(())
    }

    fn sub_balance(&mut self, address: &Address, decr: U256) -> Result<(), EvmError> {
        let mut account = self.get_account(address).unwrap_or_default();
        account.balance =
            account.balance.checked_add(decr).ok_or_else(|| EvmError::InsufficientFunds)?;
        self.set_account(address, &account);
        Ok(())
    }

    fn transfer_balance(
        &mut self,
        sender: &Address,
        recipient: &Address,
        amnt: U256,
    ) -> Result<(), EvmError> {
        self.sub_balance(sender, amnt)?;
        self.add_balance(recipient, amnt)
    }

    fn recreate(&mut self, address: [u8; 20]);
}

#[derive(Default, Debug)]
pub struct StateStore {
    pub code: HashMap<RawAddress, Vec<u8>>,
    pub accounts: HashMap<RawAddress, EvmAccount>,
    pub storages: BTreeMap<Vec<u8>, [u8; 32]>,
    pub logs: Vec<String>,
    pub self_destructs: HashSet<[u8; 20]>,
    pub recreated: HashSet<[u8; 20]>,
}

impl StateStore {
    fn overwrite_storage(&mut self, addr: [u8; 20]) {
        let address_key = addr.to_vec();
        let mut next_address_key = address_key.clone();
        *(next_address_key.last_mut().unwrap()) += 1;

        let range =
            (std::ops::Bound::Excluded(address_key), std::ops::Bound::Excluded(next_address_key));

        let keys: Vec<_> = self.storages.range(range).map(|(k, _)| k.clone()).collect();
        for k in keys.iter() {
            self.storages.remove(k);
        }
    }

    pub fn commit_code(&mut self, other: &HashMap<[u8; 20], Vec<u8>>) {
        self.code.extend(other.iter().map(|(k, v)| (*k, v.clone())));
    }

    pub fn commit_accounts(&mut self, other: &HashMap<[u8; 20], EvmAccount>) {
        self.accounts.extend(other.iter().map(|(k, v)| (*k, *v)));
    }

    pub fn commit_storages(&mut self, other: &BTreeMap<Vec<u8>, [u8; 32]>) {
        self.storages.extend(other.iter().map(|(k, v)| (k.clone(), *v)))
    }

    pub fn commit_self_destructs(&mut self, other: &HashSet<[u8; 20]>) {
        self.self_destructs.extend(other);
    }

    pub fn commit_recreated(&mut self, other: &HashSet<[u8; 20]>) {
        self.recreated.extend(other);
    }
}

impl EvmState for StateStore {
    fn code_at(&self, address: &Address) -> Option<Vec<u8>> {
        let internal_addr = utils::evm_account_to_internal_address(*address);
        if self.self_destructs.contains(&internal_addr) {
            None
        } else {
            self.code.get(&internal_addr).cloned()
        }
    }

    fn set_code(&mut self, address: &Address, bytecode: &[u8]) {
        let internal_addr = utils::evm_account_to_internal_address(*address);
        self.code.insert(internal_addr, bytecode.to_vec());
    }

    fn set_account(&mut self, address: &Address, account: &EvmAccount) {
        self.accounts.insert(address.0, account.clone());
    }

    fn get_account(&self, address: &Address) -> Option<EvmAccount> {
        self.accounts.get(&address.0).map(|account| account.clone())
    }

    fn _read_contract_storage(&self, key: [u8; 52]) -> Option<[u8; 32]> {
        let mut addr = [0u8; 20];
        addr.copy_from_slice(&key[..20]);
        if self.self_destructs.contains(&addr) {
            None
        } else {
            self.storages.get(&key.to_vec()).cloned()
        }
    }

    fn _set_contract_storage(&mut self, key: [u8; 52], value: [u8; 32]) {
        self.storages.insert(key.to_vec(), value);
    }

    fn commit_changes(&mut self, other: &StateStore) {
        self.commit_self_destructs(&other.self_destructs);
        self.commit_recreated(&other.recreated);
        self.commit_code(&other.code);
        self.commit_accounts(&other.accounts);
        self.commit_storages(&other.storages);
        self.logs.extend(other.logs.iter().cloned());
    }

    fn recreate(&mut self, addr: [u8; 20]) {
        self.code.remove(&addr);
        // We do not remove account because balances persist across recreation.
        self.accounts.entry(addr).or_insert(EvmAccount::default()).nonce = U256::from(0);
        self.overwrite_storage(addr);
        self.self_destructs.remove(&addr);
        self.recreated.insert(addr);
    }
}

pub struct SubState<'a> {
    pub msg_sender: &'a Address,
    pub state: &'a mut StateStore,
    pub parent: &'a dyn EvmState,
}

impl SubState<'_> {
    pub fn new<'a>(
        msg_sender: &'a Address,
        state: &'a mut StateStore,
        parent: &'a dyn EvmState,
    ) -> SubState<'a> {
        SubState { msg_sender, state, parent }
    }

    pub fn self_destruct(&mut self, address: &Address) {
        self.state.self_destructs.insert(address.0);
        let mut account = self.get_account(address).unwrap_or_default();
        account.nonce = U256::from(0);
        self.state.set_account(address, &account);
    }
}

impl EvmState for SubState<'_> {
    fn code_at(&self, address: &Address) -> Option<Vec<u8>> {
        let internal_addr = utils::evm_account_to_internal_address(*address);
        if self.state.self_destructs.contains(&internal_addr) {
            None
        } else {
            self.state
                .code
                .get(&internal_addr)
                .map_or_else(|| self.parent.code_at(address), |k| Some(k.to_vec()))
        }
    }

    fn set_code(&mut self, address: &Address, bytecode: &[u8]) {
        let internal_addr = utils::evm_account_to_internal_address(*address);
        self.state.code.insert(internal_addr, bytecode.to_vec());
    }

    fn set_account(&mut self, address: &Address, account: &EvmAccount) {
        self.state.set_account(address, account);
    }

    fn get_account(&self, address: &Address) -> Option<EvmAccount> {
        self.state.get_account(address).or_else(|| self.parent.get_account(address))
    }

    fn _read_contract_storage(&self, key: [u8; 52]) -> Option<[u8; 32]> {
        let mut addr = [0u8; 20];
        addr.copy_from_slice(&key[..20]);
        if self.state.self_destructs.contains(&addr) {
            None
        } else {
            self.state
                .storages
                .get(&key.to_vec())
                .copied()
                .or_else(|| self.parent._read_contract_storage(key))
        }
    }

    fn _set_contract_storage(&mut self, key: [u8; 52], value: [u8; 32]) {
        self.state.storages.insert(key.to_vec(), value);
    }

    fn commit_changes(&mut self, other: &StateStore) {
        self.state.commit_self_destructs(&other.self_destructs);
        self.state.commit_recreated(&other.recreated);
        self.state.commit_code(&other.code);
        self.state.commit_accounts(&other.accounts);
        self.state.commit_storages(&other.storages);
        self.state.logs.extend(other.logs.iter().cloned());
    }

    fn recreate(&mut self, address: [u8; 20]) {
        self.state.recreate(address);
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn substate_tests() {
        let addr_0 = Address::repeat_byte(0);
        let addr_1 = Address::repeat_byte(1);
        let addr_2 = Address::repeat_byte(2);
        let zero = U256::zero();
        let code: [u8; 3] = [0, 1, 2];
        let nonce = U256::from_dec_str("103030303").unwrap();
        let balance = U256::from_dec_str("3838209").unwrap();
        let storage_key_0 = [4u8; 32];
        let storage_key_1 = [5u8; 32];
        let storage_value_0 = [6u8; 32];
        let storage_value_1 = [7u8; 32];

        // Create the top-level store
        let mut top = StateStore::default();

        top.set_code(&addr_0, &code);
        assert_eq!(top.code_at(&addr_0), Some(code.to_vec()));
        assert_eq!(top.code_at(&addr_1), None);
        assert_eq!(top.code_at(&addr_2), None);

        top.set_nonce(&addr_0, nonce);
        assert_eq!(top.nonce_of(&addr_0), nonce);
        assert_eq!(top.nonce_of(&addr_1), zero);
        assert_eq!(top.nonce_of(&addr_2), zero);

        top.set_balance(&addr_0, balance);
        assert_eq!(top.balance_of(&addr_0), balance);
        assert_eq!(top.balance_of(&addr_1), zero);
        assert_eq!(top.balance_of(&addr_2), zero);

        top.set_contract_storage(&addr_0, storage_key_0, storage_value_0);
        assert_eq!(top.read_contract_storage(&addr_0, storage_key_0), Some(storage_value_0));
        assert_eq!(top.read_contract_storage(&addr_1, storage_key_0), None);
        assert_eq!(top.read_contract_storage(&addr_2, storage_key_0), None);

        let next = {
            // Open a new store
            let mut next = StateStore::default();
            let mut sub_1 = SubState::new(&addr_0, &mut next, &mut top);

            sub_1.set_code(&addr_1, &code);
            assert_eq!(sub_1.code_at(&addr_0), Some(code.to_vec()));
            assert_eq!(sub_1.code_at(&addr_1), Some(code.to_vec()));
            assert_eq!(sub_1.code_at(&addr_2), None);

            sub_1.set_nonce(&addr_1, nonce);
            assert_eq!(sub_1.nonce_of(&addr_0), nonce);
            assert_eq!(sub_1.nonce_of(&addr_1), nonce);
            assert_eq!(sub_1.nonce_of(&addr_2), zero);

            sub_1.set_balance(&addr_1, balance);
            assert_eq!(sub_1.balance_of(&addr_0), balance);
            assert_eq!(sub_1.balance_of(&addr_1), balance);
            assert_eq!(sub_1.balance_of(&addr_2), zero);

            sub_1.set_contract_storage(&addr_1, storage_key_0, storage_value_0);
            assert_eq!(sub_1.read_contract_storage(&addr_0, storage_key_0), Some(storage_value_0));
            assert_eq!(sub_1.read_contract_storage(&addr_1, storage_key_0), Some(storage_value_0));
            assert_eq!(sub_1.read_contract_storage(&addr_2, storage_key_0), None);

            sub_1.set_contract_storage(&addr_1, storage_key_0, storage_value_1);
            assert_eq!(sub_1.read_contract_storage(&addr_0, storage_key_0), Some(storage_value_0));
            assert_eq!(sub_1.read_contract_storage(&addr_1, storage_key_0), Some(storage_value_1));
            assert_eq!(sub_1.read_contract_storage(&addr_2, storage_key_0), None);

            sub_1.set_contract_storage(&addr_1, storage_key_1, storage_value_1);
            assert_eq!(sub_1.read_contract_storage(&addr_1, storage_key_0), Some(storage_value_1));
            assert_eq!(sub_1.read_contract_storage(&addr_1, storage_key_1), Some(storage_value_1));

            sub_1.set_contract_storage(&addr_1, storage_key_0, storage_value_0);
            assert_eq!(sub_1.read_contract_storage(&addr_1, storage_key_0), Some(storage_value_0));
            assert_eq!(sub_1.read_contract_storage(&addr_1, storage_key_1), Some(storage_value_1));

            next
        };

        top.commit_changes(&next);
        assert_eq!(top.code_at(&addr_0), Some(code.to_vec()));
        assert_eq!(top.code_at(&addr_1), Some(code.to_vec()));
        assert_eq!(top.code_at(&addr_2), None);
        assert_eq!(top.nonce_of(&addr_0), nonce);
        assert_eq!(top.nonce_of(&addr_1), nonce);
        assert_eq!(top.nonce_of(&addr_2), zero);
        assert_eq!(top.balance_of(&addr_0), balance);
        assert_eq!(top.balance_of(&addr_1), balance);
        assert_eq!(top.balance_of(&addr_2), zero);
        assert_eq!(top.read_contract_storage(&addr_0, storage_key_0), Some(storage_value_0));
        assert_eq!(top.read_contract_storage(&addr_1, storage_key_0), Some(storage_value_0));
        assert_eq!(top.read_contract_storage(&addr_1, storage_key_1), Some(storage_value_1));
        assert_eq!(top.read_contract_storage(&addr_2, storage_key_0), None);
    }

    #[test]
    fn self_destruct_tests() {
        let addr_0 = Address::repeat_byte(0);
        let addr_1 = Address::repeat_byte(1);
        let zero = U256::zero();
        let code: [u8; 3] = [0, 1, 2];
        let nonce = U256::from_dec_str("103030303").unwrap();
        let balance_0 = U256::from_dec_str("3838209").unwrap();
        let balance_1 = U256::from_dec_str("11223344").unwrap();
        let storage_key_0 = [4u8; 32];
        let storage_key_1 = [5u8; 32];
        let storage_value_0 = [6u8; 32];
        let storage_value_1 = [7u8; 32];

        // Create the top-level store
        let mut top = StateStore::default();

        top.set_code(&addr_0, &code);
        top.set_nonce(&addr_0, nonce);
        top.set_balance(&addr_0, balance_0);
        top.set_contract_storage(&addr_0, storage_key_0, storage_value_0);

        top.set_code(&addr_1, &code);
        top.set_nonce(&addr_1, nonce);
        top.set_balance(&addr_1, balance_0);
        top.set_contract_storage(&addr_1, storage_key_1, storage_value_1);

        assert_eq!(top.code_at(&addr_0), Some(code.to_vec()));
        assert_eq!(top.code_at(&addr_1), Some(code.to_vec()));

        assert_eq!(top.nonce_of(&addr_0), nonce);
        assert_eq!(top.nonce_of(&addr_1), nonce);

        assert_eq!(top.balance_of(&addr_0), balance_0);
        assert_eq!(top.balance_of(&addr_1), balance_0);

        assert_eq!(top.read_contract_storage(&addr_0, storage_key_0), Some(storage_value_0));
        assert_eq!(top.read_contract_storage(&addr_1, storage_key_1), Some(storage_value_1));

        let next = {
            // Open a new store
            let mut next = StateStore::default();
            let mut sub_1 = SubState::new(&addr_0, &mut next, &mut top);

            assert_eq!(sub_1.code_at(&addr_1), Some(code.to_vec()));
            assert_eq!(sub_1.nonce_of(&addr_1), nonce);
            assert_eq!(sub_1.balance_of(&addr_1), balance_0);
            assert_eq!(sub_1.read_contract_storage(&addr_1, storage_key_1), Some(storage_value_1));

            sub_1.self_destruct(&addr_1);
            sub_1.set_balance(&addr_1, balance_1);

            assert_eq!(sub_1.code_at(&addr_1), None);
            assert_eq!(sub_1.nonce_of(&addr_1), zero);
            assert_eq!(sub_1.balance_of(&addr_1), balance_1);
            assert_eq!(sub_1.read_contract_storage(&addr_1, storage_key_1), None);

            next
        };

        top.commit_changes(&next);

        assert_eq!(top.code_at(&addr_0), Some(code.to_vec()));
        assert_eq!(top.code_at(&addr_1), None);

        assert_eq!(top.nonce_of(&addr_0), nonce);
        assert_eq!(top.nonce_of(&addr_1), zero);

        assert_eq!(top.balance_of(&addr_0), balance_0);
        assert_eq!(top.balance_of(&addr_1), balance_1);

        assert_eq!(top.read_contract_storage(&addr_0, storage_key_0), Some(storage_value_0));
        assert_eq!(top.read_contract_storage(&addr_1, storage_key_1), None);
    }
}
