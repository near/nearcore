use super::vm_logic_builder::TestVMLogic;
use crate::logic::Config;
use crate::logic::errors::VMLogicError;
use crate::logic::gas_counter::GasCounter;
use crate::logic::vmstate::Registers;

type Result<T> = std::result::Result<T, VMLogicError>;

#[allow(dead_code)]
impl TestVMLogic<'_> {
    pub(super) fn gas_counter(&mut self) -> &mut GasCounter {
        match self {
            Self::Legacy { logic, .. } => logic.gas_counter(),
            #[cfg(feature = "wasmtime_vm")]
            Self::Wasmtime(w) => w.gas_counter(),
        }
    }

    pub(super) fn config(&self) -> &Config {
        match self {
            Self::Legacy { logic, .. } => logic.config(),
            #[cfg(feature = "wasmtime_vm")]
            Self::Wasmtime(w) => w.config(),
        }
    }

    pub(super) fn registers(&mut self) -> &mut Registers {
        match self {
            Self::Legacy { logic, .. } => logic.registers(),
            #[cfg(feature = "wasmtime_vm")]
            Self::Wasmtime(w) => w.registers(),
        }
    }

    pub(super) fn logs(&self) -> &[String] {
        match self {
            Self::Legacy { logic, .. } => logic.logs(),
            #[cfg(feature = "wasmtime_vm")]
            Self::Wasmtime(w) => w.logs(),
        }
    }

    pub(super) fn wrapped_internal_write_register(
        &mut self,
        register_id: u64,
        data: &[u8],
    ) -> Result<()> {
        match self {
            Self::Legacy { logic, .. } => logic.wrapped_internal_write_register(register_id, data),
            #[cfg(feature = "wasmtime_vm")]
            Self::Wasmtime(w) => w.wrapped_internal_write_register(register_id, data),
        }
    }

    pub(super) fn gas_opcodes(&mut self, opcodes: u32) -> Result<()> {
        match self {
            Self::Legacy { logic, .. } => logic.gas_opcodes(opcodes),
            #[cfg(feature = "wasmtime_vm")]
            Self::Wasmtime(w) => w.gas_opcodes(opcodes),
        }
    }

    pub(super) fn read_register(&mut self, register_id: u64, ptr: u64) -> Result<()> {
        match self {
            Self::Legacy { logic, .. } => logic.read_register(register_id, ptr),
            #[cfg(feature = "wasmtime_vm")]
            Self::Wasmtime(w) => w.read_register(register_id, ptr),
        }
    }

    pub(super) fn register_len(&mut self, register_id: u64) -> Result<u64> {
        match self {
            Self::Legacy { logic, .. } => logic.register_len(register_id),
            #[cfg(feature = "wasmtime_vm")]
            Self::Wasmtime(w) => w.register_len(register_id),
        }
    }

    pub(super) fn write_register(
        &mut self,
        register_id: u64,
        data_len: u64,
        data_ptr: u64,
    ) -> Result<()> {
        match self {
            Self::Legacy { logic, .. } => logic.write_register(register_id, data_len, data_ptr),
            #[cfg(feature = "wasmtime_vm")]
            Self::Wasmtime(w) => w.write_register(register_id, data_len, data_ptr),
        }
    }

    pub(super) fn current_account_id(&mut self, register_id: u64) -> Result<()> {
        match self {
            Self::Legacy { logic, .. } => logic.current_account_id(register_id),
            #[cfg(feature = "wasmtime_vm")]
            Self::Wasmtime(w) => w.current_account_id(register_id),
        }
    }

    pub(super) fn signer_account_id(&mut self, register_id: u64) -> Result<()> {
        match self {
            Self::Legacy { logic, .. } => logic.signer_account_id(register_id),
            #[cfg(feature = "wasmtime_vm")]
            Self::Wasmtime(w) => w.signer_account_id(register_id),
        }
    }

    pub(super) fn signer_account_pk(&mut self, register_id: u64) -> Result<()> {
        match self {
            Self::Legacy { logic, .. } => logic.signer_account_pk(register_id),
            #[cfg(feature = "wasmtime_vm")]
            Self::Wasmtime(w) => w.signer_account_pk(register_id),
        }
    }

    pub(super) fn predecessor_account_id(&mut self, register_id: u64) -> Result<()> {
        match self {
            Self::Legacy { logic, .. } => logic.predecessor_account_id(register_id),
            #[cfg(feature = "wasmtime_vm")]
            Self::Wasmtime(w) => w.predecessor_account_id(register_id),
        }
    }

    pub(super) fn input(&mut self, register_id: u64) -> Result<()> {
        match self {
            Self::Legacy { logic, .. } => logic.input(register_id),
            #[cfg(feature = "wasmtime_vm")]
            Self::Wasmtime(w) => w.input(register_id),
        }
    }

    pub(super) fn block_index(&mut self) -> Result<u64> {
        match self {
            Self::Legacy { logic, .. } => logic.block_index(),
            #[cfg(feature = "wasmtime_vm")]
            Self::Wasmtime(w) => w.block_index(),
        }
    }

    pub(super) fn block_timestamp(&mut self) -> Result<u64> {
        match self {
            Self::Legacy { logic, .. } => logic.block_timestamp(),
            #[cfg(feature = "wasmtime_vm")]
            Self::Wasmtime(w) => w.block_timestamp(),
        }
    }

    pub(super) fn epoch_height(&mut self) -> Result<u64> {
        match self {
            Self::Legacy { logic, .. } => logic.epoch_height(),
            #[cfg(feature = "wasmtime_vm")]
            Self::Wasmtime(w) => w.epoch_height(),
        }
    }

    pub(super) fn storage_usage(&mut self) -> Result<u64> {
        match self {
            Self::Legacy { logic, .. } => logic.storage_usage(),
            #[cfg(feature = "wasmtime_vm")]
            Self::Wasmtime(w) => w.storage_usage(),
        }
    }

    pub(super) fn account_balance(&mut self, balance_ptr: u64) -> Result<()> {
        match self {
            Self::Legacy { logic, .. } => logic.account_balance(balance_ptr),
            #[cfg(feature = "wasmtime_vm")]
            Self::Wasmtime(w) => w.account_balance(balance_ptr),
        }
    }

    pub(super) fn account_locked_balance(&mut self, balance_ptr: u64) -> Result<()> {
        match self {
            Self::Legacy { logic, .. } => logic.account_locked_balance(balance_ptr),
            #[cfg(feature = "wasmtime_vm")]
            Self::Wasmtime(w) => w.account_locked_balance(balance_ptr),
        }
    }

    pub(super) fn attached_deposit(&mut self, balance_ptr: u64) -> Result<()> {
        match self {
            Self::Legacy { logic, .. } => logic.attached_deposit(balance_ptr),
            #[cfg(feature = "wasmtime_vm")]
            Self::Wasmtime(w) => w.attached_deposit(balance_ptr),
        }
    }

    pub(super) fn prepaid_gas(&mut self) -> Result<u64> {
        match self {
            Self::Legacy { logic, .. } => logic.prepaid_gas(),
            #[cfg(feature = "wasmtime_vm")]
            Self::Wasmtime(w) => w.prepaid_gas(),
        }
    }

    pub(super) fn used_gas(&mut self) -> Result<u64> {
        match self {
            Self::Legacy { logic, .. } => logic.used_gas(),
            #[cfg(feature = "wasmtime_vm")]
            Self::Wasmtime(w) => w.used_gas(),
        }
    }

    pub(super) fn random_seed(&mut self, register_id: u64) -> Result<()> {
        match self {
            Self::Legacy { logic, .. } => logic.random_seed(register_id),
            #[cfg(feature = "wasmtime_vm")]
            Self::Wasmtime(w) => w.random_seed(register_id),
        }
    }

    pub(super) fn sha256(
        &mut self,
        value_len: u64,
        value_ptr: u64,
        register_id: u64,
    ) -> Result<()> {
        match self {
            Self::Legacy { logic, .. } => logic.sha256(value_len, value_ptr, register_id),
            #[cfg(feature = "wasmtime_vm")]
            Self::Wasmtime(w) => w.sha256(value_len, value_ptr, register_id),
        }
    }

    pub(super) fn keccak256(
        &mut self,
        value_len: u64,
        value_ptr: u64,
        register_id: u64,
    ) -> Result<()> {
        match self {
            Self::Legacy { logic, .. } => logic.keccak256(value_len, value_ptr, register_id),
            #[cfg(feature = "wasmtime_vm")]
            Self::Wasmtime(w) => w.keccak256(value_len, value_ptr, register_id),
        }
    }

    pub(super) fn keccak512(
        &mut self,
        value_len: u64,
        value_ptr: u64,
        register_id: u64,
    ) -> Result<()> {
        match self {
            Self::Legacy { logic, .. } => logic.keccak512(value_len, value_ptr, register_id),
            #[cfg(feature = "wasmtime_vm")]
            Self::Wasmtime(w) => w.keccak512(value_len, value_ptr, register_id),
        }
    }

    pub(super) fn ripemd160(
        &mut self,
        value_len: u64,
        value_ptr: u64,
        register_id: u64,
    ) -> Result<()> {
        match self {
            Self::Legacy { logic, .. } => logic.ripemd160(value_len, value_ptr, register_id),
            #[cfg(feature = "wasmtime_vm")]
            Self::Wasmtime(w) => w.ripemd160(value_len, value_ptr, register_id),
        }
    }

    pub(super) fn ecrecover(
        &mut self,
        hash_len: u64,
        hash_ptr: u64,
        sign_len: u64,
        sig_ptr: u64,
        v: u64,
        malleability_flag: u64,
        register_id: u64,
    ) -> Result<u64> {
        match self {
            Self::Legacy { logic, .. } => logic.ecrecover(
                hash_len,
                hash_ptr,
                sign_len,
                sig_ptr,
                v,
                malleability_flag,
                register_id,
            ),
            #[cfg(feature = "wasmtime_vm")]
            Self::Wasmtime(w) => w.ecrecover(
                hash_len,
                hash_ptr,
                sign_len,
                sig_ptr,
                v,
                malleability_flag,
                register_id,
            ),
        }
    }

    pub(super) fn ed25519_verify(
        &mut self,
        sig_len: u64,
        sig_ptr: u64,
        msg_len: u64,
        msg_ptr: u64,
        pub_key_len: u64,
        pub_key_ptr: u64,
    ) -> Result<u64> {
        match self {
            Self::Legacy { logic, .. } => {
                logic.ed25519_verify(sig_len, sig_ptr, msg_len, msg_ptr, pub_key_len, pub_key_ptr)
            }
            #[cfg(feature = "wasmtime_vm")]
            Self::Wasmtime(w) => {
                w.ed25519_verify(sig_len, sig_ptr, msg_len, msg_ptr, pub_key_len, pub_key_ptr)
            }
        }
    }

    pub(super) fn value_return(&mut self, value_len: u64, value_ptr: u64) -> Result<()> {
        match self {
            Self::Legacy { logic, .. } => logic.value_return(value_len, value_ptr),
            #[cfg(feature = "wasmtime_vm")]
            Self::Wasmtime(w) => w.value_return(value_len, value_ptr),
        }
    }

    pub(super) fn log_utf8(&mut self, len: u64, ptr: u64) -> Result<()> {
        match self {
            Self::Legacy { logic, .. } => logic.log_utf8(len, ptr),
            #[cfg(feature = "wasmtime_vm")]
            Self::Wasmtime(w) => w.log_utf8(len, ptr),
        }
    }

    pub(super) fn log_utf16(&mut self, len: u64, ptr: u64) -> Result<()> {
        match self {
            Self::Legacy { logic, .. } => logic.log_utf16(len, ptr),
            #[cfg(feature = "wasmtime_vm")]
            Self::Wasmtime(w) => w.log_utf16(len, ptr),
        }
    }

    pub(super) fn promise_create(
        &mut self,
        account_id_len: u64,
        account_id_ptr: u64,
        method_name_len: u64,
        method_name_ptr: u64,
        arguments_len: u64,
        arguments_ptr: u64,
        amount_ptr: u64,
        gas: u64,
    ) -> Result<u64> {
        match self {
            Self::Legacy { logic, .. } => logic.promise_create(
                account_id_len,
                account_id_ptr,
                method_name_len,
                method_name_ptr,
                arguments_len,
                arguments_ptr,
                amount_ptr,
                gas,
            ),
            #[cfg(feature = "wasmtime_vm")]
            Self::Wasmtime(w) => w.promise_create(
                account_id_len,
                account_id_ptr,
                method_name_len,
                method_name_ptr,
                arguments_len,
                arguments_ptr,
                amount_ptr,
                gas,
            ),
        }
    }

    pub(super) fn promise_then(
        &mut self,
        promise_index: u64,
        account_id_len: u64,
        account_id_ptr: u64,
        method_name_len: u64,
        method_name_ptr: u64,
        arguments_len: u64,
        arguments_ptr: u64,
        amount_ptr: u64,
        gas: u64,
    ) -> Result<u64> {
        match self {
            Self::Legacy { logic, .. } => logic.promise_then(
                promise_index,
                account_id_len,
                account_id_ptr,
                method_name_len,
                method_name_ptr,
                arguments_len,
                arguments_ptr,
                amount_ptr,
                gas,
            ),
            #[cfg(feature = "wasmtime_vm")]
            Self::Wasmtime(w) => w.promise_then(
                promise_index,
                account_id_len,
                account_id_ptr,
                method_name_len,
                method_name_ptr,
                arguments_len,
                arguments_ptr,
                amount_ptr,
                gas,
            ),
        }
    }

    pub(super) fn promise_and(
        &mut self,
        promise_idx_ptr: u64,
        promise_idx_count: u64,
    ) -> Result<u64> {
        match self {
            Self::Legacy { logic, .. } => logic.promise_and(promise_idx_ptr, promise_idx_count),
            #[cfg(feature = "wasmtime_vm")]
            Self::Wasmtime(w) => w.promise_and(promise_idx_ptr, promise_idx_count),
        }
    }

    pub(super) fn promise_batch_create(
        &mut self,
        account_id_len: u64,
        account_id_ptr: u64,
    ) -> Result<u64> {
        match self {
            Self::Legacy { logic, .. } => {
                logic.promise_batch_create(account_id_len, account_id_ptr)
            }
            #[cfg(feature = "wasmtime_vm")]
            Self::Wasmtime(w) => w.promise_batch_create(account_id_len, account_id_ptr),
        }
    }

    pub(super) fn promise_batch_then(
        &mut self,
        promise_index: u64,
        account_id_len: u64,
        account_id_ptr: u64,
    ) -> Result<u64> {
        match self {
            Self::Legacy { logic, .. } => {
                logic.promise_batch_then(promise_index, account_id_len, account_id_ptr)
            }
            #[cfg(feature = "wasmtime_vm")]
            Self::Wasmtime(w) => {
                w.promise_batch_then(promise_index, account_id_len, account_id_ptr)
            }
        }
    }

    pub(super) fn promise_results_count(&mut self) -> Result<u64> {
        match self {
            Self::Legacy { logic, .. } => logic.promise_results_count(),
            #[cfg(feature = "wasmtime_vm")]
            Self::Wasmtime(w) => w.promise_results_count(),
        }
    }

    pub(super) fn promise_result(&mut self, result_idx: u64, register_id: u64) -> Result<u64> {
        match self {
            Self::Legacy { logic, .. } => logic.promise_result(result_idx, register_id),
            #[cfg(feature = "wasmtime_vm")]
            Self::Wasmtime(w) => w.promise_result(result_idx, register_id),
        }
    }

    pub(super) fn promise_return(&mut self, promise_idx: u64) -> Result<()> {
        match self {
            Self::Legacy { logic, .. } => logic.promise_return(promise_idx),
            #[cfg(feature = "wasmtime_vm")]
            Self::Wasmtime(w) => w.promise_return(promise_idx),
        }
    }

    pub(super) fn promise_batch_action_create_account(&mut self, promise_index: u64) -> Result<()> {
        match self {
            Self::Legacy { logic, .. } => logic.promise_batch_action_create_account(promise_index),
            #[cfg(feature = "wasmtime_vm")]
            Self::Wasmtime(w) => w.promise_batch_action_create_account(promise_index),
        }
    }

    pub(super) fn promise_batch_action_deploy_contract(
        &mut self,
        promise_index: u64,
        code_len: u64,
        code_ptr: u64,
    ) -> Result<()> {
        match self {
            Self::Legacy { logic, .. } => {
                logic.promise_batch_action_deploy_contract(promise_index, code_len, code_ptr)
            }
            #[cfg(feature = "wasmtime_vm")]
            Self::Wasmtime(w) => {
                w.promise_batch_action_deploy_contract(promise_index, code_len, code_ptr)
            }
        }
    }

    pub(super) fn promise_batch_action_deploy_global_contract(
        &mut self,
        promise_index: u64,
        code_len: u64,
        code_ptr: u64,
    ) -> Result<()> {
        match self {
            Self::Legacy { logic, .. } => {
                logic.promise_batch_action_deploy_global_contract(promise_index, code_len, code_ptr)
            }
            #[cfg(feature = "wasmtime_vm")]
            Self::Wasmtime(w) => {
                w.promise_batch_action_deploy_global_contract(promise_index, code_len, code_ptr)
            }
        }
    }

    pub(super) fn promise_batch_action_use_global_contract(
        &mut self,
        promise_index: u64,
        code_hash_len: u64,
        code_hash_ptr: u64,
    ) -> Result<()> {
        match self {
            Self::Legacy { logic, .. } => logic.promise_batch_action_use_global_contract(
                promise_index,
                code_hash_len,
                code_hash_ptr,
            ),
            #[cfg(feature = "wasmtime_vm")]
            Self::Wasmtime(w) => w.promise_batch_action_use_global_contract(
                promise_index,
                code_hash_len,
                code_hash_ptr,
            ),
        }
    }

    pub(super) fn promise_batch_action_use_global_contract_by_account_id(
        &mut self,
        promise_index: u64,
        account_id_len: u64,
        account_id_ptr: u64,
    ) -> Result<()> {
        match self {
            Self::Legacy { logic, .. } => logic
                .promise_batch_action_use_global_contract_by_account_id(
                    promise_index,
                    account_id_len,
                    account_id_ptr,
                ),
            #[cfg(feature = "wasmtime_vm")]
            Self::Wasmtime(w) => w.promise_batch_action_use_global_contract_by_account_id(
                promise_index,
                account_id_len,
                account_id_ptr,
            ),
        }
    }

    pub(super) fn promise_batch_action_function_call(
        &mut self,
        promise_index: u64,
        method_name_len: u64,
        method_name_ptr: u64,
        arguments_len: u64,
        arguments_ptr: u64,
        amount_ptr: u64,
        gas: u64,
    ) -> Result<()> {
        match self {
            Self::Legacy { logic, .. } => logic.promise_batch_action_function_call(
                promise_index,
                method_name_len,
                method_name_ptr,
                arguments_len,
                arguments_ptr,
                amount_ptr,
                gas,
            ),
            #[cfg(feature = "wasmtime_vm")]
            Self::Wasmtime(w) => w.promise_batch_action_function_call(
                promise_index,
                method_name_len,
                method_name_ptr,
                arguments_len,
                arguments_ptr,
                amount_ptr,
                gas,
            ),
        }
    }

    pub(super) fn promise_batch_action_function_call_weight(
        &mut self,
        promise_index: u64,
        method_name_len: u64,
        method_name_ptr: u64,
        arguments_len: u64,
        arguments_ptr: u64,
        amount_ptr: u64,
        gas: u64,
        gas_weight: u64,
    ) -> Result<()> {
        match self {
            Self::Legacy { logic, .. } => logic.promise_batch_action_function_call_weight(
                promise_index,
                method_name_len,
                method_name_ptr,
                arguments_len,
                arguments_ptr,
                amount_ptr,
                gas,
                gas_weight,
            ),
            #[cfg(feature = "wasmtime_vm")]
            Self::Wasmtime(w) => w.promise_batch_action_function_call_weight(
                promise_index,
                method_name_len,
                method_name_ptr,
                arguments_len,
                arguments_ptr,
                amount_ptr,
                gas,
                gas_weight,
            ),
        }
    }

    pub(super) fn promise_batch_action_transfer(
        &mut self,
        promise_index: u64,
        amount_ptr: u64,
    ) -> Result<()> {
        match self {
            Self::Legacy { logic, .. } => {
                logic.promise_batch_action_transfer(promise_index, amount_ptr)
            }
            #[cfg(feature = "wasmtime_vm")]
            Self::Wasmtime(w) => w.promise_batch_action_transfer(promise_index, amount_ptr),
        }
    }

    pub(super) fn promise_batch_action_stake(
        &mut self,
        promise_index: u64,
        amount_ptr: u64,
        public_key_len: u64,
        public_key_ptr: u64,
    ) -> Result<()> {
        match self {
            Self::Legacy { logic, .. } => logic.promise_batch_action_stake(
                promise_index,
                amount_ptr,
                public_key_len,
                public_key_ptr,
            ),
            #[cfg(feature = "wasmtime_vm")]
            Self::Wasmtime(w) => w.promise_batch_action_stake(
                promise_index,
                amount_ptr,
                public_key_len,
                public_key_ptr,
            ),
        }
    }

    pub(super) fn promise_batch_action_add_key_with_full_access(
        &mut self,
        promise_index: u64,
        public_key_len: u64,
        public_key_ptr: u64,
        nonce: u64,
    ) -> Result<()> {
        match self {
            Self::Legacy { logic, .. } => logic.promise_batch_action_add_key_with_full_access(
                promise_index,
                public_key_len,
                public_key_ptr,
                nonce,
            ),
            #[cfg(feature = "wasmtime_vm")]
            Self::Wasmtime(w) => w.promise_batch_action_add_key_with_full_access(
                promise_index,
                public_key_len,
                public_key_ptr,
                nonce,
            ),
        }
    }

    pub(super) fn promise_batch_action_add_key_with_function_call(
        &mut self,
        promise_index: u64,
        public_key_len: u64,
        public_key_ptr: u64,
        nonce: u64,
        allowance_ptr: u64,
        receiver_id_len: u64,
        receiver_id_ptr: u64,
        method_names_len: u64,
        method_names_ptr: u64,
    ) -> Result<()> {
        match self {
            Self::Legacy { logic, .. } => logic.promise_batch_action_add_key_with_function_call(
                promise_index,
                public_key_len,
                public_key_ptr,
                nonce,
                allowance_ptr,
                receiver_id_len,
                receiver_id_ptr,
                method_names_len,
                method_names_ptr,
            ),
            #[cfg(feature = "wasmtime_vm")]
            Self::Wasmtime(w) => w.promise_batch_action_add_key_with_function_call(
                promise_index,
                public_key_len,
                public_key_ptr,
                nonce,
                allowance_ptr,
                receiver_id_len,
                receiver_id_ptr,
                method_names_len,
                method_names_ptr,
            ),
        }
    }

    pub(super) fn promise_batch_action_delete_key(
        &mut self,
        promise_index: u64,
        public_key_len: u64,
        public_key_ptr: u64,
    ) -> Result<()> {
        match self {
            Self::Legacy { logic, .. } => {
                logic.promise_batch_action_delete_key(promise_index, public_key_len, public_key_ptr)
            }
            #[cfg(feature = "wasmtime_vm")]
            Self::Wasmtime(w) => {
                w.promise_batch_action_delete_key(promise_index, public_key_len, public_key_ptr)
            }
        }
    }

    pub(super) fn promise_batch_action_delete_account(
        &mut self,
        promise_index: u64,
        beneficiary_id_len: u64,
        beneficiary_id_ptr: u64,
    ) -> Result<()> {
        match self {
            Self::Legacy { logic, .. } => logic.promise_batch_action_delete_account(
                promise_index,
                beneficiary_id_len,
                beneficiary_id_ptr,
            ),
            #[cfg(feature = "wasmtime_vm")]
            Self::Wasmtime(w) => w.promise_batch_action_delete_account(
                promise_index,
                beneficiary_id_len,
                beneficiary_id_ptr,
            ),
        }
    }

    pub(super) fn promise_batch_action_add_gas_key_with_full_access(
        &mut self,
        promise_index: u64,
        public_key_len: u64,
        public_key_ptr: u64,
        num_nonces: u64,
    ) -> Result<()> {
        match self {
            Self::Legacy { logic, .. } => logic.promise_batch_action_add_gas_key_with_full_access(
                promise_index,
                public_key_len,
                public_key_ptr,
                num_nonces,
            ),
            #[cfg(feature = "wasmtime_vm")]
            Self::Wasmtime(w) => w.promise_batch_action_add_gas_key_with_full_access(
                promise_index,
                public_key_len,
                public_key_ptr,
                num_nonces,
            ),
        }
    }

    pub(super) fn promise_batch_action_add_gas_key_with_function_call(
        &mut self,
        promise_index: u64,
        public_key_len: u64,
        public_key_ptr: u64,
        num_nonces: u64,
        allowance_ptr: u64,
        receiver_id_len: u64,
        receiver_id_ptr: u64,
        method_names_len: u64,
        method_names_ptr: u64,
    ) -> Result<()> {
        match self {
            Self::Legacy { logic, .. } => logic
                .promise_batch_action_add_gas_key_with_function_call(
                    promise_index,
                    public_key_len,
                    public_key_ptr,
                    num_nonces,
                    allowance_ptr,
                    receiver_id_len,
                    receiver_id_ptr,
                    method_names_len,
                    method_names_ptr,
                ),
            #[cfg(feature = "wasmtime_vm")]
            Self::Wasmtime(w) => w.promise_batch_action_add_gas_key_with_function_call(
                promise_index,
                public_key_len,
                public_key_ptr,
                num_nonces,
                allowance_ptr,
                receiver_id_len,
                receiver_id_ptr,
                method_names_len,
                method_names_ptr,
            ),
        }
    }

    pub(super) fn promise_batch_action_transfer_to_gas_key(
        &mut self,
        promise_index: u64,
        public_key_len: u64,
        public_key_ptr: u64,
        amount_ptr: u64,
    ) -> Result<()> {
        match self {
            Self::Legacy { logic, .. } => logic.promise_batch_action_transfer_to_gas_key(
                promise_index,
                public_key_len,
                public_key_ptr,
                amount_ptr,
            ),
            #[cfg(feature = "wasmtime_vm")]
            Self::Wasmtime(w) => w.promise_batch_action_transfer_to_gas_key(
                promise_index,
                public_key_len,
                public_key_ptr,
                amount_ptr,
            ),
        }
    }

    pub(super) fn promise_batch_action_deploy_global_contract_by_account_id(
        &mut self,
        promise_index: u64,
        code_len: u64,
        code_ptr: u64,
    ) -> Result<()> {
        match self {
            Self::Legacy { logic, .. } => logic
                .promise_batch_action_deploy_global_contract_by_account_id(
                    promise_index,
                    code_len,
                    code_ptr,
                ),
            #[cfg(feature = "wasmtime_vm")]
            Self::Wasmtime(w) => w.promise_batch_action_deploy_global_contract_by_account_id(
                promise_index,
                code_len,
                code_ptr,
            ),
        }
    }

    pub(super) fn refund_to_account_id(&mut self, register_id: u64) -> Result<()> {
        match self {
            Self::Legacy { logic, .. } => logic.refund_to_account_id(register_id),
            #[cfg(feature = "wasmtime_vm")]
            Self::Wasmtime(w) => w.refund_to_account_id(register_id),
        }
    }

    pub(super) fn promise_batch_action_state_init(
        &mut self,
        promise_idx: u64,
        code_len: u64,
        code_ptr: u64,
        amount_ptr: u64,
    ) -> Result<u64> {
        match self {
            Self::Legacy { logic, .. } => {
                logic.promise_batch_action_state_init(promise_idx, code_len, code_ptr, amount_ptr)
            }
            #[cfg(feature = "wasmtime_vm")]
            Self::Wasmtime(w) => {
                w.promise_batch_action_state_init(promise_idx, code_len, code_ptr, amount_ptr)
            }
        }
    }

    pub(super) fn promise_batch_action_state_init_by_account_id(
        &mut self,
        promise_idx: u64,
        account_id_len: u64,
        code_hash_ptr: u64,
        amount_ptr: u64,
    ) -> Result<u64> {
        match self {
            Self::Legacy { logic, .. } => logic.promise_batch_action_state_init_by_account_id(
                promise_idx,
                account_id_len,
                code_hash_ptr,
                amount_ptr,
            ),
            #[cfg(feature = "wasmtime_vm")]
            Self::Wasmtime(w) => w.promise_batch_action_state_init_by_account_id(
                promise_idx,
                account_id_len,
                code_hash_ptr,
                amount_ptr,
            ),
        }
    }

    pub(super) fn set_state_init_data_entry(
        &mut self,
        promise_idx: u64,
        action_index: u64,
        key_len: u64,
        key_ptr: u64,
        value_len: u64,
        value_ptr: u64,
    ) -> Result<()> {
        match self {
            Self::Legacy { logic, .. } => logic.set_state_init_data_entry(
                promise_idx,
                action_index,
                key_len,
                key_ptr,
                value_len,
                value_ptr,
            ),
            #[cfg(feature = "wasmtime_vm")]
            Self::Wasmtime(w) => w.set_state_init_data_entry(
                promise_idx,
                action_index,
                key_len,
                key_ptr,
                value_len,
                value_ptr,
            ),
        }
    }

    pub(super) fn current_contract_code(&mut self, register_id: u64) -> Result<u64> {
        match self {
            Self::Legacy { logic, .. } => logic.current_contract_code(register_id),
            #[cfg(feature = "wasmtime_vm")]
            Self::Wasmtime(w) => w.current_contract_code(register_id),
        }
    }

    pub(super) fn storage_write(
        &mut self,
        key_len: u64,
        key_ptr: u64,
        value_len: u64,
        value_ptr: u64,
        register_id: u64,
    ) -> Result<u64> {
        match self {
            Self::Legacy { logic, .. } => {
                logic.storage_write(key_len, key_ptr, value_len, value_ptr, register_id)
            }
            #[cfg(feature = "wasmtime_vm")]
            Self::Wasmtime(w) => {
                w.storage_write(key_len, key_ptr, value_len, value_ptr, register_id)
            }
        }
    }

    pub(super) fn storage_read(
        &mut self,
        key_len: u64,
        key_ptr: u64,
        register_id: u64,
    ) -> Result<u64> {
        match self {
            Self::Legacy { logic, .. } => logic.storage_read(key_len, key_ptr, register_id),
            #[cfg(feature = "wasmtime_vm")]
            Self::Wasmtime(w) => w.storage_read(key_len, key_ptr, register_id),
        }
    }

    pub(super) fn storage_remove(
        &mut self,
        key_len: u64,
        key_ptr: u64,
        register_id: u64,
    ) -> Result<u64> {
        match self {
            Self::Legacy { logic, .. } => logic.storage_remove(key_len, key_ptr, register_id),
            #[cfg(feature = "wasmtime_vm")]
            Self::Wasmtime(w) => w.storage_remove(key_len, key_ptr, register_id),
        }
    }

    pub(super) fn storage_has_key(&mut self, key_len: u64, key_ptr: u64) -> Result<u64> {
        match self {
            Self::Legacy { logic, .. } => logic.storage_has_key(key_len, key_ptr),
            #[cfg(feature = "wasmtime_vm")]
            Self::Wasmtime(w) => w.storage_has_key(key_len, key_ptr),
        }
    }

    pub(super) fn storage_iter_prefix(&mut self, prefix_len: u64, prefix_ptr: u64) -> Result<u64> {
        match self {
            Self::Legacy { logic, .. } => logic.storage_iter_prefix(prefix_len, prefix_ptr),
            #[cfg(feature = "wasmtime_vm")]
            Self::Wasmtime(w) => w.storage_iter_prefix(prefix_len, prefix_ptr),
        }
    }

    pub(super) fn storage_iter_range(
        &mut self,
        start_len: u64,
        start_ptr: u64,
        end_len: u64,
        end_ptr: u64,
    ) -> Result<u64> {
        match self {
            Self::Legacy { logic, .. } => {
                logic.storage_iter_range(start_len, start_ptr, end_len, end_ptr)
            }
            #[cfg(feature = "wasmtime_vm")]
            Self::Wasmtime(w) => w.storage_iter_range(start_len, start_ptr, end_len, end_ptr),
        }
    }

    pub(super) fn storage_iter_next(
        &mut self,
        iterator_id: u64,
        key_register_id: u64,
        value_register_id: u64,
    ) -> Result<u64> {
        match self {
            Self::Legacy { logic, .. } => {
                logic.storage_iter_next(iterator_id, key_register_id, value_register_id)
            }
            #[cfg(feature = "wasmtime_vm")]
            Self::Wasmtime(w) => {
                w.storage_iter_next(iterator_id, key_register_id, value_register_id)
            }
        }
    }

    pub(super) fn alt_bn128_g1_multiexp(
        &mut self,
        value_len: u64,
        value_ptr: u64,
        register_id: u64,
    ) -> Result<()> {
        match self {
            Self::Legacy { logic, .. } => {
                logic.alt_bn128_g1_multiexp(value_len, value_ptr, register_id)
            }
            #[cfg(feature = "wasmtime_vm")]
            Self::Wasmtime(w) => w.alt_bn128_g1_multiexp(value_len, value_ptr, register_id),
        }
    }

    pub(super) fn alt_bn128_g1_sum(
        &mut self,
        value_len: u64,
        value_ptr: u64,
        register_id: u64,
    ) -> Result<()> {
        match self {
            Self::Legacy { logic, .. } => logic.alt_bn128_g1_sum(value_len, value_ptr, register_id),
            #[cfg(feature = "wasmtime_vm")]
            Self::Wasmtime(w) => w.alt_bn128_g1_sum(value_len, value_ptr, register_id),
        }
    }

    pub(super) fn alt_bn128_pairing_check(
        &mut self,
        value_len: u64,
        value_ptr: u64,
    ) -> Result<u64> {
        match self {
            Self::Legacy { logic, .. } => logic.alt_bn128_pairing_check(value_len, value_ptr),
            #[cfg(feature = "wasmtime_vm")]
            Self::Wasmtime(w) => w.alt_bn128_pairing_check(value_len, value_ptr),
        }
    }

    pub(super) fn bls12381_pairing_check(&mut self, value_len: u64, value_ptr: u64) -> Result<u64> {
        match self {
            Self::Legacy { logic, .. } => logic.bls12381_pairing_check(value_len, value_ptr),
            #[cfg(feature = "wasmtime_vm")]
            Self::Wasmtime(w) => w.bls12381_pairing_check(value_len, value_ptr),
        }
    }

    pub(super) fn bls12381_p1_sum(
        &mut self,
        value_len: u64,
        value_ptr: u64,
        register_id: u64,
    ) -> Result<u64> {
        match self {
            Self::Legacy { logic, .. } => logic.bls12381_p1_sum(value_len, value_ptr, register_id),
            #[cfg(feature = "wasmtime_vm")]
            Self::Wasmtime(w) => w.bls12381_p1_sum(value_len, value_ptr, register_id),
        }
    }

    pub(super) fn bls12381_p2_sum(
        &mut self,
        value_len: u64,
        value_ptr: u64,
        register_id: u64,
    ) -> Result<u64> {
        match self {
            Self::Legacy { logic, .. } => logic.bls12381_p2_sum(value_len, value_ptr, register_id),
            #[cfg(feature = "wasmtime_vm")]
            Self::Wasmtime(w) => w.bls12381_p2_sum(value_len, value_ptr, register_id),
        }
    }

    pub(super) fn bls12381_g1_multiexp(
        &mut self,
        value_len: u64,
        value_ptr: u64,
        register_id: u64,
    ) -> Result<u64> {
        match self {
            Self::Legacy { logic, .. } => {
                logic.bls12381_g1_multiexp(value_len, value_ptr, register_id)
            }
            #[cfg(feature = "wasmtime_vm")]
            Self::Wasmtime(w) => w.bls12381_g1_multiexp(value_len, value_ptr, register_id),
        }
    }

    pub(super) fn bls12381_g2_multiexp(
        &mut self,
        value_len: u64,
        value_ptr: u64,
        register_id: u64,
    ) -> Result<u64> {
        match self {
            Self::Legacy { logic, .. } => {
                logic.bls12381_g2_multiexp(value_len, value_ptr, register_id)
            }
            #[cfg(feature = "wasmtime_vm")]
            Self::Wasmtime(w) => w.bls12381_g2_multiexp(value_len, value_ptr, register_id),
        }
    }

    pub(super) fn bls12381_map_fp_to_g1(
        &mut self,
        value_len: u64,
        value_ptr: u64,
        register_id: u64,
    ) -> Result<u64> {
        match self {
            Self::Legacy { logic, .. } => {
                logic.bls12381_map_fp_to_g1(value_len, value_ptr, register_id)
            }
            #[cfg(feature = "wasmtime_vm")]
            Self::Wasmtime(w) => w.bls12381_map_fp_to_g1(value_len, value_ptr, register_id),
        }
    }

    pub(super) fn bls12381_map_fp2_to_g2(
        &mut self,
        value_len: u64,
        value_ptr: u64,
        register_id: u64,
    ) -> Result<u64> {
        match self {
            Self::Legacy { logic, .. } => {
                logic.bls12381_map_fp2_to_g2(value_len, value_ptr, register_id)
            }
            #[cfg(feature = "wasmtime_vm")]
            Self::Wasmtime(w) => w.bls12381_map_fp2_to_g2(value_len, value_ptr, register_id),
        }
    }

    pub(super) fn bls12381_p1_decompress(
        &mut self,
        value_len: u64,
        value_ptr: u64,
        register_id: u64,
    ) -> Result<u64> {
        match self {
            Self::Legacy { logic, .. } => {
                logic.bls12381_p1_decompress(value_len, value_ptr, register_id)
            }
            #[cfg(feature = "wasmtime_vm")]
            Self::Wasmtime(w) => w.bls12381_p1_decompress(value_len, value_ptr, register_id),
        }
    }

    pub(super) fn bls12381_p2_decompress(
        &mut self,
        value_len: u64,
        value_ptr: u64,
        register_id: u64,
    ) -> Result<u64> {
        match self {
            Self::Legacy { logic, .. } => {
                logic.bls12381_p2_decompress(value_len, value_ptr, register_id)
            }
            #[cfg(feature = "wasmtime_vm")]
            Self::Wasmtime(w) => w.bls12381_p2_decompress(value_len, value_ptr, register_id),
        }
    }
}
