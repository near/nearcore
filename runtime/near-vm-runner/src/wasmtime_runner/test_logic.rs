use super::Ctx;
use super::logic;
use crate::logic::errors::VMLogicError;
use crate::logic::gas_counter::GasCounter;
use crate::logic::mocks::mock_external::MockedExternal;
use crate::logic::vmstate::Registers;
use crate::logic::{Config, ExecutionResultState, MemSlice, VMContext, VMOutcome};
use near_parameters::RuntimeFeesConfig;
use std::sync::Arc;
use wasmtime::{Engine, Memory, Store};

const MEMORY_SIZE: u64 = 64 * 1024;

pub(crate) struct WasmtimeTestLogic {
    store: Store<Ctx>,
    memory: Memory,
    mem_write_offset: u64,
}

#[allow(dead_code)]
impl WasmtimeTestLogic {
    pub(crate) fn new(
        ext: &mut MockedExternal,
        context: &VMContext,
        fees_config: RuntimeFeesConfig,
        config: Config,
    ) -> Self {
        // SAFETY: same transmute pattern as production code. The references are
        // valid for the lifetime of the WasmtimeTestLogic because the builder
        // that owns them outlives it.
        let ext: &'static mut dyn crate::logic::External =
            unsafe { core::mem::transmute(ext as &mut dyn crate::logic::External) };
        let context: &'static VMContext = unsafe { core::mem::transmute(context) };

        let config = Arc::new(config);
        let engine = Engine::default();

        let result_state = ExecutionResultState::new(
            context,
            context.make_gas_counter(&config),
            Arc::clone(&config),
        );
        // Create a dummy ModuleExport for the memory field in Ctx. We'll
        // pre-resolve it immediately after creating the store, so the
        // Unresolved value is never actually used.
        let dummy_wasm = wat::parse_str("(module (memory (export \"memory\") 1))").unwrap();
        let dummy_memory_export = wasmtime::Module::new(&engine, &dummy_wasm)
            .unwrap()
            .get_export_index("memory")
            .unwrap();
        let ctx = Ctx::new(ext, context, Arc::new(fees_config), result_state, dummy_memory_export);

        let mut store = Store::new(&engine, ctx);
        store.limiter(|ctx| &mut ctx.limits);

        let memory = Memory::new(&mut store, wasmtime::MemoryType::new(1, Some(1))).unwrap();
        store.data_mut().memory = super::Export::Resolved(memory);

        Self { store, memory, mem_write_offset: 0 }
    }

    /// Returns `(&mut [u8], &mut Ctx)` — guest memory and store context.
    fn ctx_and_mem(&mut self) -> (&mut [u8], &mut Ctx) {
        self.memory.data_and_store_mut(&mut self.store)
    }

    // Registers API

    pub(crate) fn read_register(&mut self, register_id: u64, ptr: u64) -> Result<(), VMLogicError> {
        let (mem, ctx) = self.ctx_and_mem();
        logic::read_register(ctx, mem, register_id, ptr)
    }

    pub(crate) fn register_len(&mut self, register_id: u64) -> Result<u64, VMLogicError> {
        let (mem, ctx) = self.ctx_and_mem();
        logic::register_len(ctx, mem, register_id)
    }

    pub(crate) fn write_register(
        &mut self,
        register_id: u64,
        data_len: u64,
        data_ptr: u64,
    ) -> Result<(), VMLogicError> {
        let (mem, ctx) = self.ctx_and_mem();
        logic::write_register(ctx, mem, register_id, data_len, data_ptr)
    }

    // Context API

    pub(crate) fn current_account_id(&mut self, register_id: u64) -> Result<(), VMLogicError> {
        let (mem, ctx) = self.ctx_and_mem();
        logic::current_account_id(ctx, mem, register_id)
    }

    pub(crate) fn signer_account_id(&mut self, register_id: u64) -> Result<(), VMLogicError> {
        let (mem, ctx) = self.ctx_and_mem();
        logic::signer_account_id(ctx, mem, register_id)
    }

    pub(crate) fn signer_account_pk(&mut self, register_id: u64) -> Result<(), VMLogicError> {
        let (mem, ctx) = self.ctx_and_mem();
        logic::signer_account_pk(ctx, mem, register_id)
    }

    pub(crate) fn predecessor_account_id(&mut self, register_id: u64) -> Result<(), VMLogicError> {
        let (mem, ctx) = self.ctx_and_mem();
        logic::predecessor_account_id(ctx, mem, register_id)
    }

    pub(crate) fn input(&mut self, register_id: u64) -> Result<(), VMLogicError> {
        let (mem, ctx) = self.ctx_and_mem();
        logic::input(ctx, mem, register_id)
    }

    pub(crate) fn block_index(&mut self) -> Result<u64, VMLogicError> {
        let (mem, ctx) = self.ctx_and_mem();
        logic::block_index(ctx, mem)
    }

    pub(crate) fn block_timestamp(&mut self) -> Result<u64, VMLogicError> {
        let (mem, ctx) = self.ctx_and_mem();
        logic::block_timestamp(ctx, mem)
    }

    pub(crate) fn epoch_height(&mut self) -> Result<u64, VMLogicError> {
        let (mem, ctx) = self.ctx_and_mem();
        logic::epoch_height(ctx, mem)
    }

    pub(crate) fn storage_usage(&mut self) -> Result<u64, VMLogicError> {
        let (mem, ctx) = self.ctx_and_mem();
        logic::storage_usage(ctx, mem)
    }

    // Economics API

    pub(crate) fn account_balance(&mut self, balance_ptr: u64) -> Result<(), VMLogicError> {
        let (mem, ctx) = self.ctx_and_mem();
        logic::account_balance(ctx, mem, balance_ptr)
    }

    pub(crate) fn account_locked_balance(&mut self, balance_ptr: u64) -> Result<(), VMLogicError> {
        let (mem, ctx) = self.ctx_and_mem();
        logic::account_locked_balance(ctx, mem, balance_ptr)
    }

    pub(crate) fn attached_deposit(&mut self, balance_ptr: u64) -> Result<(), VMLogicError> {
        let (mem, ctx) = self.ctx_and_mem();
        logic::attached_deposit(ctx, mem, balance_ptr)
    }

    pub(crate) fn prepaid_gas(&mut self) -> Result<u64, VMLogicError> {
        let (mem, ctx) = self.ctx_and_mem();
        logic::prepaid_gas(ctx, mem)
    }

    pub(crate) fn used_gas(&mut self) -> Result<u64, VMLogicError> {
        let (mem, ctx) = self.ctx_and_mem();
        logic::used_gas(ctx, mem)
    }

    // Math API

    pub(crate) fn random_seed(&mut self, register_id: u64) -> Result<(), VMLogicError> {
        let (mem, ctx) = self.ctx_and_mem();
        logic::random_seed(ctx, mem, register_id)
    }

    pub(crate) fn sha256(
        &mut self,
        value_len: u64,
        value_ptr: u64,
        register_id: u64,
    ) -> Result<(), VMLogicError> {
        let (mem, ctx) = self.ctx_and_mem();
        logic::sha256(ctx, mem, value_len, value_ptr, register_id)
    }

    pub(crate) fn keccak256(
        &mut self,
        value_len: u64,
        value_ptr: u64,
        register_id: u64,
    ) -> Result<(), VMLogicError> {
        let (mem, ctx) = self.ctx_and_mem();
        logic::keccak256(ctx, mem, value_len, value_ptr, register_id)
    }

    pub(crate) fn keccak512(
        &mut self,
        value_len: u64,
        value_ptr: u64,
        register_id: u64,
    ) -> Result<(), VMLogicError> {
        let (mem, ctx) = self.ctx_and_mem();
        logic::keccak512(ctx, mem, value_len, value_ptr, register_id)
    }

    pub(crate) fn ripemd160(
        &mut self,
        value_len: u64,
        value_ptr: u64,
        register_id: u64,
    ) -> Result<(), VMLogicError> {
        let (mem, ctx) = self.ctx_and_mem();
        logic::ripemd160(ctx, mem, value_len, value_ptr, register_id)
    }

    pub(crate) fn ecrecover(
        &mut self,
        hash_len: u64,
        hash_ptr: u64,
        sign_len: u64,
        sig_ptr: u64,
        v: u64,
        malleability_flag: u64,
        register_id: u64,
    ) -> Result<u64, VMLogicError> {
        let (mem, ctx) = self.ctx_and_mem();
        logic::ecrecover(
            ctx,
            mem,
            hash_len,
            hash_ptr,
            sign_len,
            sig_ptr,
            v,
            malleability_flag,
            register_id,
        )
    }

    pub(crate) fn ed25519_verify(
        &mut self,
        sig_len: u64,
        sig_ptr: u64,
        msg_len: u64,
        msg_ptr: u64,
        pub_key_len: u64,
        pub_key_ptr: u64,
    ) -> Result<u64, VMLogicError> {
        let (mem, ctx) = self.ctx_and_mem();
        logic::ed25519_verify(
            ctx,
            mem,
            sig_len,
            sig_ptr,
            msg_len,
            msg_ptr,
            pub_key_len,
            pub_key_ptr,
        )
    }

    // Miscellaneous API

    pub(crate) fn value_return(
        &mut self,
        value_len: u64,
        value_ptr: u64,
    ) -> Result<(), VMLogicError> {
        let (mem, ctx) = self.ctx_and_mem();
        logic::value_return(ctx, mem, value_len, value_ptr)
    }

    pub(crate) fn panic(&mut self) -> Result<(), VMLogicError> {
        let (mem, ctx) = self.ctx_and_mem();
        logic::panic(ctx, mem)
    }

    pub(crate) fn panic_utf8(&mut self, len: u64, ptr: u64) -> Result<(), VMLogicError> {
        let (mem, ctx) = self.ctx_and_mem();
        logic::panic_utf8(ctx, mem, len, ptr)
    }

    pub(crate) fn log_utf8(&mut self, len: u64, ptr: u64) -> Result<(), VMLogicError> {
        let (mem, ctx) = self.ctx_and_mem();
        logic::log_utf8(ctx, mem, len, ptr)
    }

    pub(crate) fn log_utf16(&mut self, len: u64, ptr: u64) -> Result<(), VMLogicError> {
        let (mem, ctx) = self.ctx_and_mem();
        logic::log_utf16(ctx, mem, len, ptr)
    }

    pub(crate) fn abort(
        &mut self,
        msg_ptr: u32,
        filename_ptr: u32,
        line: u32,
        col: u32,
    ) -> Result<(), VMLogicError> {
        let (mem, ctx) = self.ctx_and_mem();
        logic::abort(ctx, mem, msg_ptr, filename_ptr, line, col)
    }

    // Promises API

    pub(crate) fn promise_create(
        &mut self,
        account_id_len: u64,
        account_id_ptr: u64,
        method_name_len: u64,
        method_name_ptr: u64,
        arguments_len: u64,
        arguments_ptr: u64,
        amount_ptr: u64,
        gas: u64,
    ) -> Result<u64, VMLogicError> {
        let (mem, ctx) = self.ctx_and_mem();
        logic::promise_create(
            ctx,
            mem,
            account_id_len,
            account_id_ptr,
            method_name_len,
            method_name_ptr,
            arguments_len,
            arguments_ptr,
            amount_ptr,
            gas,
        )
    }

    pub(crate) fn promise_then(
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
    ) -> Result<u64, VMLogicError> {
        let (mem, ctx) = self.ctx_and_mem();
        logic::promise_then(
            ctx,
            mem,
            promise_index,
            account_id_len,
            account_id_ptr,
            method_name_len,
            method_name_ptr,
            arguments_len,
            arguments_ptr,
            amount_ptr,
            gas,
        )
    }

    pub(crate) fn promise_and(
        &mut self,
        promise_idx_ptr: u64,
        promise_idx_count: u64,
    ) -> Result<u64, VMLogicError> {
        let (mem, ctx) = self.ctx_and_mem();
        logic::promise_and(ctx, mem, promise_idx_ptr, promise_idx_count)
    }

    pub(crate) fn promise_batch_create(
        &mut self,
        account_id_len: u64,
        account_id_ptr: u64,
    ) -> Result<u64, VMLogicError> {
        let (mem, ctx) = self.ctx_and_mem();
        logic::promise_batch_create(ctx, mem, account_id_len, account_id_ptr)
    }

    pub(crate) fn promise_batch_then(
        &mut self,
        promise_index: u64,
        account_id_len: u64,
        account_id_ptr: u64,
    ) -> Result<u64, VMLogicError> {
        let (mem, ctx) = self.ctx_and_mem();
        logic::promise_batch_then(ctx, mem, promise_index, account_id_len, account_id_ptr)
    }

    pub(crate) fn promise_set_refund_to(
        &mut self,
        promise_index: u64,
        account_id_len: u64,
        account_id_ptr: u64,
    ) -> Result<(), VMLogicError> {
        let (mem, ctx) = self.ctx_and_mem();
        logic::promise_set_refund_to(ctx, mem, promise_index, account_id_len, account_id_ptr)
    }

    pub(crate) fn promise_batch_action_state_init(
        &mut self,
        promise_idx: u64,
        code_len: u64,
        code_ptr: u64,
        amount_ptr: u64,
    ) -> Result<u64, VMLogicError> {
        let (mem, ctx) = self.ctx_and_mem();
        logic::promise_batch_action_state_init(
            ctx,
            mem,
            promise_idx,
            code_len,
            code_ptr,
            amount_ptr,
        )
    }

    pub(crate) fn promise_batch_action_state_init_by_account_id(
        &mut self,
        promise_idx: u64,
        account_id_len: u64,
        code_hash_ptr: u64,
        amount_ptr: u64,
    ) -> Result<u64, VMLogicError> {
        let (mem, ctx) = self.ctx_and_mem();
        logic::promise_batch_action_state_init_by_account_id(
            ctx,
            mem,
            promise_idx,
            account_id_len,
            code_hash_ptr,
            amount_ptr,
        )
    }

    pub(crate) fn set_state_init_data_entry(
        &mut self,
        promise_idx: u64,
        action_index: u64,
        key_len: u64,
        key_ptr: u64,
        value_len: u64,
        value_ptr: u64,
    ) -> Result<(), VMLogicError> {
        let (mem, ctx) = self.ctx_and_mem();
        logic::set_state_init_data_entry(
            ctx,
            mem,
            promise_idx,
            action_index,
            key_len,
            key_ptr,
            value_len,
            value_ptr,
        )
    }

    pub(crate) fn current_contract_code(&mut self, register_id: u64) -> Result<u64, VMLogicError> {
        let (mem, ctx) = self.ctx_and_mem();
        logic::current_contract_code(ctx, mem, register_id)
    }

    pub(crate) fn refund_to_account_id(&mut self, register_id: u64) -> Result<(), VMLogicError> {
        let (mem, ctx) = self.ctx_and_mem();
        logic::refund_to_account_id(ctx, mem, register_id)
    }

    // Promise batch actions

    pub(crate) fn promise_batch_action_create_account(
        &mut self,
        promise_index: u64,
    ) -> Result<(), VMLogicError> {
        let (mem, ctx) = self.ctx_and_mem();
        logic::promise_batch_action_create_account(ctx, mem, promise_index)
    }

    pub(crate) fn promise_batch_action_deploy_contract(
        &mut self,
        promise_index: u64,
        code_len: u64,
        code_ptr: u64,
    ) -> Result<(), VMLogicError> {
        let (mem, ctx) = self.ctx_and_mem();
        logic::promise_batch_action_deploy_contract(ctx, mem, promise_index, code_len, code_ptr)
    }

    pub(crate) fn promise_batch_action_deploy_global_contract(
        &mut self,
        promise_index: u64,
        code_len: u64,
        code_ptr: u64,
    ) -> Result<(), VMLogicError> {
        let (mem, ctx) = self.ctx_and_mem();
        logic::promise_batch_action_deploy_global_contract(
            ctx,
            mem,
            promise_index,
            code_len,
            code_ptr,
        )
    }

    pub(crate) fn promise_batch_action_deploy_global_contract_by_account_id(
        &mut self,
        promise_index: u64,
        code_len: u64,
        code_ptr: u64,
    ) -> Result<(), VMLogicError> {
        let (mem, ctx) = self.ctx_and_mem();
        logic::promise_batch_action_deploy_global_contract_by_account_id(
            ctx,
            mem,
            promise_index,
            code_len,
            code_ptr,
        )
    }

    pub(crate) fn promise_batch_action_use_global_contract(
        &mut self,
        promise_index: u64,
        code_hash_len: u64,
        code_hash_ptr: u64,
    ) -> Result<(), VMLogicError> {
        let (mem, ctx) = self.ctx_and_mem();
        logic::promise_batch_action_use_global_contract(
            ctx,
            mem,
            promise_index,
            code_hash_len,
            code_hash_ptr,
        )
    }

    pub(crate) fn promise_batch_action_use_global_contract_by_account_id(
        &mut self,
        promise_index: u64,
        account_id_len: u64,
        account_id_ptr: u64,
    ) -> Result<(), VMLogicError> {
        let (mem, ctx) = self.ctx_and_mem();
        logic::promise_batch_action_use_global_contract_by_account_id(
            ctx,
            mem,
            promise_index,
            account_id_len,
            account_id_ptr,
        )
    }

    pub(crate) fn promise_batch_action_function_call(
        &mut self,
        promise_index: u64,
        method_name_len: u64,
        method_name_ptr: u64,
        arguments_len: u64,
        arguments_ptr: u64,
        amount_ptr: u64,
        gas: u64,
    ) -> Result<(), VMLogicError> {
        let (mem, ctx) = self.ctx_and_mem();
        logic::promise_batch_action_function_call(
            ctx,
            mem,
            promise_index,
            method_name_len,
            method_name_ptr,
            arguments_len,
            arguments_ptr,
            amount_ptr,
            gas,
        )
    }

    pub(crate) fn promise_batch_action_function_call_weight(
        &mut self,
        promise_index: u64,
        method_name_len: u64,
        method_name_ptr: u64,
        arguments_len: u64,
        arguments_ptr: u64,
        amount_ptr: u64,
        gas: u64,
        gas_weight: u64,
    ) -> Result<(), VMLogicError> {
        let (mem, ctx) = self.ctx_and_mem();
        logic::promise_batch_action_function_call_weight(
            ctx,
            mem,
            promise_index,
            method_name_len,
            method_name_ptr,
            arguments_len,
            arguments_ptr,
            amount_ptr,
            gas,
            gas_weight,
        )
    }

    pub(crate) fn promise_batch_action_transfer(
        &mut self,
        promise_index: u64,
        amount_ptr: u64,
    ) -> Result<(), VMLogicError> {
        let (mem, ctx) = self.ctx_and_mem();
        logic::promise_batch_action_transfer(ctx, mem, promise_index, amount_ptr)
    }

    pub(crate) fn promise_batch_action_stake(
        &mut self,
        promise_index: u64,
        amount_ptr: u64,
        public_key_len: u64,
        public_key_ptr: u64,
    ) -> Result<(), VMLogicError> {
        let (mem, ctx) = self.ctx_and_mem();
        logic::promise_batch_action_stake(
            ctx,
            mem,
            promise_index,
            amount_ptr,
            public_key_len,
            public_key_ptr,
        )
    }

    pub(crate) fn promise_batch_action_add_key_with_full_access(
        &mut self,
        promise_index: u64,
        public_key_len: u64,
        public_key_ptr: u64,
        nonce: u64,
    ) -> Result<(), VMLogicError> {
        let (mem, ctx) = self.ctx_and_mem();
        logic::promise_batch_action_add_key_with_full_access(
            ctx,
            mem,
            promise_index,
            public_key_len,
            public_key_ptr,
            nonce,
        )
    }

    pub(crate) fn promise_batch_action_add_key_with_function_call(
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
    ) -> Result<(), VMLogicError> {
        let (mem, ctx) = self.ctx_and_mem();
        logic::promise_batch_action_add_key_with_function_call(
            ctx,
            mem,
            promise_index,
            public_key_len,
            public_key_ptr,
            nonce,
            allowance_ptr,
            receiver_id_len,
            receiver_id_ptr,
            method_names_len,
            method_names_ptr,
        )
    }

    pub(crate) fn promise_batch_action_delete_key(
        &mut self,
        promise_index: u64,
        public_key_len: u64,
        public_key_ptr: u64,
    ) -> Result<(), VMLogicError> {
        let (mem, ctx) = self.ctx_and_mem();
        logic::promise_batch_action_delete_key(
            ctx,
            mem,
            promise_index,
            public_key_len,
            public_key_ptr,
        )
    }

    pub(crate) fn promise_batch_action_delete_account(
        &mut self,
        promise_index: u64,
        beneficiary_id_len: u64,
        beneficiary_id_ptr: u64,
    ) -> Result<(), VMLogicError> {
        let (mem, ctx) = self.ctx_and_mem();
        logic::promise_batch_action_delete_account(
            ctx,
            mem,
            promise_index,
            beneficiary_id_len,
            beneficiary_id_ptr,
        )
    }

    pub(crate) fn promise_batch_action_transfer_to_gas_key(
        &mut self,
        promise_index: u64,
        public_key_len: u64,
        public_key_ptr: u64,
        amount_ptr: u64,
    ) -> Result<(), VMLogicError> {
        let (mem, ctx) = self.ctx_and_mem();
        logic::promise_batch_action_transfer_to_gas_key(
            ctx,
            mem,
            promise_index,
            public_key_len,
            public_key_ptr,
            amount_ptr,
        )
    }

    pub(crate) fn promise_batch_action_add_gas_key_with_full_access(
        &mut self,
        promise_index: u64,
        public_key_len: u64,
        public_key_ptr: u64,
        num_nonces: u64,
    ) -> Result<(), VMLogicError> {
        let (mem, ctx) = self.ctx_and_mem();
        logic::promise_batch_action_add_gas_key_with_full_access(
            ctx,
            mem,
            promise_index,
            public_key_len,
            public_key_ptr,
            num_nonces,
        )
    }

    pub(crate) fn promise_batch_action_add_gas_key_with_function_call(
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
    ) -> Result<(), VMLogicError> {
        let (mem, ctx) = self.ctx_and_mem();
        logic::promise_batch_action_add_gas_key_with_function_call(
            ctx,
            mem,
            promise_index,
            public_key_len,
            public_key_ptr,
            num_nonces,
            allowance_ptr,
            receiver_id_len,
            receiver_id_ptr,
            method_names_len,
            method_names_ptr,
        )
    }

    // Promise yield/resume

    pub(crate) fn promise_yield_create(
        &mut self,
        method_name_len: u64,
        method_name_ptr: u64,
        arguments_len: u64,
        arguments_ptr: u64,
        gas: u64,
        gas_weight: u64,
        register_id: u64,
    ) -> Result<u64, VMLogicError> {
        let (mem, ctx) = self.ctx_and_mem();
        logic::promise_yield_create(
            ctx,
            mem,
            method_name_len,
            method_name_ptr,
            arguments_len,
            arguments_ptr,
            gas,
            gas_weight,
            register_id,
        )
    }

    pub(crate) fn promise_yield_resume(
        &mut self,
        data_id_len: u64,
        data_id_ptr: u64,
        payload_len: u64,
        payload_ptr: u64,
    ) -> Result<u32, VMLogicError> {
        let (mem, ctx) = self.ctx_and_mem();
        logic::promise_yield_resume(ctx, mem, data_id_len, data_id_ptr, payload_len, payload_ptr)
    }

    // Promise results

    pub(crate) fn promise_results_count(&mut self) -> Result<u64, VMLogicError> {
        let (mem, ctx) = self.ctx_and_mem();
        logic::promise_results_count(ctx, mem)
    }

    pub(crate) fn promise_result(
        &mut self,
        result_idx: u64,
        register_id: u64,
    ) -> Result<u64, VMLogicError> {
        let (mem, ctx) = self.ctx_and_mem();
        logic::promise_result(ctx, mem, result_idx, register_id)
    }

    pub(crate) fn promise_return(&mut self, promise_idx: u64) -> Result<(), VMLogicError> {
        let (mem, ctx) = self.ctx_and_mem();
        logic::promise_return(ctx, mem, promise_idx)
    }

    // Storage API

    pub(crate) fn storage_write(
        &mut self,
        key_len: u64,
        key_ptr: u64,
        value_len: u64,
        value_ptr: u64,
        register_id: u64,
    ) -> Result<u64, VMLogicError> {
        let (mem, ctx) = self.ctx_and_mem();
        logic::storage_write(ctx, mem, key_len, key_ptr, value_len, value_ptr, register_id)
    }

    pub(crate) fn storage_read(
        &mut self,
        key_len: u64,
        key_ptr: u64,
        register_id: u64,
    ) -> Result<u64, VMLogicError> {
        let (mem, ctx) = self.ctx_and_mem();
        logic::storage_read(ctx, mem, key_len, key_ptr, register_id)
    }

    pub(crate) fn storage_remove(
        &mut self,
        key_len: u64,
        key_ptr: u64,
        register_id: u64,
    ) -> Result<u64, VMLogicError> {
        let (mem, ctx) = self.ctx_and_mem();
        logic::storage_remove(ctx, mem, key_len, key_ptr, register_id)
    }

    pub(crate) fn storage_has_key(
        &mut self,
        key_len: u64,
        key_ptr: u64,
    ) -> Result<u64, VMLogicError> {
        let (mem, ctx) = self.ctx_and_mem();
        logic::storage_has_key(ctx, mem, key_len, key_ptr)
    }

    pub(crate) fn storage_iter_prefix(
        &mut self,
        prefix_len: u64,
        prefix_ptr: u64,
    ) -> Result<u64, VMLogicError> {
        let (mem, ctx) = self.ctx_and_mem();
        logic::storage_iter_prefix(ctx, mem, prefix_len, prefix_ptr)
    }

    pub(crate) fn storage_iter_range(
        &mut self,
        start_len: u64,
        start_ptr: u64,
        end_len: u64,
        end_ptr: u64,
    ) -> Result<u64, VMLogicError> {
        let (mem, ctx) = self.ctx_and_mem();
        logic::storage_iter_range(ctx, mem, start_len, start_ptr, end_len, end_ptr)
    }

    pub(crate) fn storage_iter_next(
        &mut self,
        iterator_id: u64,
        key_register_id: u64,
        value_register_id: u64,
    ) -> Result<u64, VMLogicError> {
        let (mem, ctx) = self.ctx_and_mem();
        logic::storage_iter_next(ctx, mem, iterator_id, key_register_id, value_register_id)
    }

    // Gas

    pub(crate) fn gas_seen_from_wasm(&mut self, opcodes: u32) -> Result<(), VMLogicError> {
        let (mem, ctx) = self.ctx_and_mem();
        logic::gas_seen_from_wasm(ctx, mem, opcodes)
    }

    pub(crate) fn gas_opcodes(&mut self, opcodes: u32) -> Result<(), VMLogicError> {
        logic::gas_opcodes(&mut self.store.data_mut().result_state, opcodes)
    }

    // Validator API

    pub(crate) fn validator_stake(
        &mut self,
        account_id_len: u64,
        account_id_ptr: u64,
        stake_ptr: u64,
    ) -> Result<(), VMLogicError> {
        let (mem, ctx) = self.ctx_and_mem();
        logic::validator_stake(ctx, mem, account_id_len, account_id_ptr, stake_ptr)
    }

    pub(crate) fn validator_total_stake(&mut self, stake_ptr: u64) -> Result<(), VMLogicError> {
        let (mem, ctx) = self.ctx_and_mem();
        logic::validator_total_stake(ctx, mem, stake_ptr)
    }

    // Alt BN128

    pub(crate) fn alt_bn128_g1_multiexp(
        &mut self,
        value_len: u64,
        value_ptr: u64,
        register_id: u64,
    ) -> Result<(), VMLogicError> {
        let (mem, ctx) = self.ctx_and_mem();
        logic::alt_bn128_g1_multiexp(ctx, mem, value_len, value_ptr, register_id)
    }

    pub(crate) fn alt_bn128_g1_sum(
        &mut self,
        value_len: u64,
        value_ptr: u64,
        register_id: u64,
    ) -> Result<(), VMLogicError> {
        let (mem, ctx) = self.ctx_and_mem();
        logic::alt_bn128_g1_sum(ctx, mem, value_len, value_ptr, register_id)
    }

    pub(crate) fn alt_bn128_pairing_check(
        &mut self,
        value_len: u64,
        value_ptr: u64,
    ) -> Result<u64, VMLogicError> {
        let (mem, ctx) = self.ctx_and_mem();
        logic::alt_bn128_pairing_check(ctx, mem, value_len, value_ptr)
    }

    // BLS12-381

    pub(crate) fn bls12381_p1_sum(
        &mut self,
        value_len: u64,
        value_ptr: u64,
        register_id: u64,
    ) -> Result<u64, VMLogicError> {
        let (mem, ctx) = self.ctx_and_mem();
        logic::bls12381_p1_sum(ctx, mem, value_len, value_ptr, register_id)
    }

    pub(crate) fn bls12381_p2_sum(
        &mut self,
        value_len: u64,
        value_ptr: u64,
        register_id: u64,
    ) -> Result<u64, VMLogicError> {
        let (mem, ctx) = self.ctx_and_mem();
        logic::bls12381_p2_sum(ctx, mem, value_len, value_ptr, register_id)
    }

    pub(crate) fn bls12381_g1_multiexp(
        &mut self,
        value_len: u64,
        value_ptr: u64,
        register_id: u64,
    ) -> Result<u64, VMLogicError> {
        let (mem, ctx) = self.ctx_and_mem();
        logic::bls12381_g1_multiexp(ctx, mem, value_len, value_ptr, register_id)
    }

    pub(crate) fn bls12381_g2_multiexp(
        &mut self,
        value_len: u64,
        value_ptr: u64,
        register_id: u64,
    ) -> Result<u64, VMLogicError> {
        let (mem, ctx) = self.ctx_and_mem();
        logic::bls12381_g2_multiexp(ctx, mem, value_len, value_ptr, register_id)
    }

    pub(crate) fn bls12381_map_fp_to_g1(
        &mut self,
        value_len: u64,
        value_ptr: u64,
        register_id: u64,
    ) -> Result<u64, VMLogicError> {
        let (mem, ctx) = self.ctx_and_mem();
        logic::bls12381_map_fp_to_g1(ctx, mem, value_len, value_ptr, register_id)
    }

    pub(crate) fn bls12381_map_fp2_to_g2(
        &mut self,
        value_len: u64,
        value_ptr: u64,
        register_id: u64,
    ) -> Result<u64, VMLogicError> {
        let (mem, ctx) = self.ctx_and_mem();
        logic::bls12381_map_fp2_to_g2(ctx, mem, value_len, value_ptr, register_id)
    }

    pub(crate) fn bls12381_pairing_check(
        &mut self,
        value_len: u64,
        value_ptr: u64,
    ) -> Result<u64, VMLogicError> {
        let (mem, ctx) = self.ctx_and_mem();
        logic::bls12381_pairing_check(ctx, mem, value_len, value_ptr)
    }

    pub(crate) fn bls12381_p1_decompress(
        &mut self,
        value_len: u64,
        value_ptr: u64,
        register_id: u64,
    ) -> Result<u64, VMLogicError> {
        let (mem, ctx) = self.ctx_and_mem();
        logic::bls12381_p1_decompress(ctx, mem, value_len, value_ptr, register_id)
    }

    pub(crate) fn bls12381_p2_decompress(
        &mut self,
        value_len: u64,
        value_ptr: u64,
        register_id: u64,
    ) -> Result<u64, VMLogicError> {
        let (mem, ctx) = self.ctx_and_mem();
        logic::bls12381_p2_decompress(ctx, mem, value_len, value_ptr, register_id)
    }

    // Test helpers

    pub(crate) fn gas_counter(&mut self) -> &mut GasCounter {
        &mut self.store.data_mut().result_state.gas_counter
    }

    pub(crate) fn config(&self) -> &Config {
        &self.store.data().config
    }

    pub(crate) fn registers(&mut self) -> &mut Registers {
        &mut self.store.data_mut().registers
    }

    pub(crate) fn logs(&self) -> &[String] {
        &self.store.data().result_state.logs
    }

    pub(crate) fn internal_mem_write(&mut self, data: &[u8]) -> MemSlice {
        let ptr = self.mem_write_offset;
        self.memory.write(&mut self.store, ptr as usize, data).unwrap();
        self.mem_write_offset += data.len() as u64;
        MemSlice { ptr, len: data.len() as u64 }
    }

    pub(crate) fn internal_mem_write_at(&mut self, ptr: u64, data: &[u8]) -> MemSlice {
        self.memory.write(&mut self.store, ptr as usize, data).unwrap();
        MemSlice { ptr, len: data.len() as u64 }
    }

    pub(crate) fn internal_mem_read(&self, ptr: u64, len: u64) -> Vec<u8> {
        let mut buf = vec![0u8; len as usize];
        self.memory.read(&self.store, ptr as usize, &mut buf).unwrap();
        buf
    }

    #[track_caller]
    pub(crate) fn assert_read_register(&mut self, want: &[u8], register_id: u64) {
        let len = self.registers().get_len(register_id).unwrap();
        let ptr = MEMORY_SIZE - len;
        self.read_register(register_id, ptr).unwrap();
        let got = self.internal_mem_read(ptr, len);
        assert_eq!(want, &got[..]);
    }

    pub(crate) fn wrapped_internal_write_register(
        &mut self,
        register_id: u64,
        data: &[u8],
    ) -> Result<(), VMLogicError> {
        let ctx = self.store.data_mut();
        ctx.registers.set(
            &mut ctx.result_state.gas_counter,
            &ctx.config.limit_config,
            register_id,
            data,
        )
    }

    pub(crate) fn compute_outcome(self) -> VMOutcome {
        self.store.into_data().result_state.compute_outcome()
    }
}
