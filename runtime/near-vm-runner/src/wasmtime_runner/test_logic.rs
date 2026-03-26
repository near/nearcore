use super::{Ctx, ErrorContainer, Export, link};
use crate::imports;
use crate::logic::errors::VMLogicError;
use crate::logic::gas_counter::GasCounter;
use crate::logic::mocks::mock_external::MockedExternal;
use crate::logic::vmstate::Registers;
use crate::logic::{Config, ExecutionResultState, MemSlice, VMContext, VMOutcome};
use crate::tests::test_vm_config;
use near_parameters::RuntimeFeesConfig;
use std::sync::Arc;
use wasmtime::{Engine, Instance, Linker, Memory, Module, Store};

/// Size of the guest memory in the test WAT shim (1 page = 64KB).
/// Matches `MockedMemory::MEMORY_SIZE`.
const MEMORY_PAGES: u32 = 1;
const MEMORY_SIZE: u64 = MEMORY_PAGES as u64 * 64 * 1024;

fn extract_vm_logic_error(err: anyhow::Error) -> VMLogicError {
    let cause = err.root_cause();
    if let Some(container) = cause.downcast_ref::<ErrorContainer>() {
        container.take().expect("error already taken")
    } else {
        panic!("unexpected wasmtime error: {err:?}");
    }
}

/// Build a WAT module that imports all host functions and re-exports them, plus
/// exports a 1-page linear memory. This lets us call host functions through the
/// normal wasmtime path (which creates `Caller` internally).
#[allow(clippy::large_stack_frames)]
fn build_wat_shim(config: &Config) -> String {
    let mut imports = String::new();
    let mut exports = String::new();

    fn wat_type(t: &str) -> &str {
        match t {
            "u64" => "i64",
            "u32" => "i32",
            other => panic!("unexpected wasm type: {other}"),
        }
    }

    macro_rules! add_wat {
        ($mod:ident / $name:ident : $func:ident < [ $( $arg_name:ident : $arg_type:ident ),* ] -> [ $( $returns:ident ),* ] >) => {{
            let params: &[&str] = &[$(wat_type(stringify!($arg_type))),*];
            let results: &[&str] = &[$(wat_type(stringify!($returns))),*];
            let param_str = if params.is_empty() {
                String::new()
            } else {
                format!(" (param{})", params.iter().map(|t| format!(" {t}")).collect::<String>())
            };
            let result_str = if results.is_empty() {
                String::new()
            } else {
                format!(" (result{})", results.iter().map(|t| format!(" {t}")).collect::<String>())
            };
            imports.push_str(&format!(
                "  (import \"{}\" \"{}\" (func ${}{}{}))\n",
                stringify!($mod), stringify!($name), stringify!($name), param_str, result_str,
            ));
            exports.push_str(&format!(
                "  (export \"{}\" (func ${}))\n",
                stringify!($name), stringify!($name),
            ));
        }};
    }

    imports::for_each_available_import!(config, add_wat);

    format!("(module\n{imports}{exports}  (memory (export \"memory\") {MEMORY_PAGES})\n)")
}

pub(crate) struct WasmtimeTestLogic {
    store: Store<Ctx>,
    instance: Instance,
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
        let wat = build_wat_shim(&config);
        let wat_bytes = wat::parse_str(&wat).expect("failed to parse WAT shim");

        let engine = Engine::default();
        let module = Module::new(&engine, &wat_bytes).expect("failed to compile WAT shim");

        let mut linker = Linker::new(&engine);
        link(&mut linker, &config);

        let result_state = ExecutionResultState::new(
            context,
            context.make_gas_counter(&config),
            Arc::clone(&config),
        );
        let memory_export = module.get_export_index("memory").expect("WAT shim must export memory");
        let ctx = Ctx::new(ext, context, Arc::new(fees_config), result_state, memory_export);

        let mut store = Store::new(&engine, ctx);
        store.limiter(|ctx| &mut ctx.limits);

        let pre = linker.instantiate_pre(&module).expect("failed to pre-instantiate WAT shim");
        let instance = pre.instantiate(&mut store).expect("failed to instantiate WAT shim");
        let memory =
            instance.get_memory(&mut store, "memory").expect("WAT shim must export memory");

        // Pre-resolve the memory in Ctx so that host functions don't need to
        // call `caller.get_module_export` (which fails with on-demand
        // allocation since those create "Dummy" instances).
        store.data_mut().memory = Export::Resolved(memory);

        Self { store, instance, memory, mem_write_offset: 0 }
    }

    // ---------------------------------------------------------------
    // Generic call helpers
    //
    // Uses `get_export` + `into_func` + `typed` instead of
    // `get_typed_func` because wasmtime's on-demand allocator creates
    // "Dummy" instances that don't support `get_typed_func`.
    // ---------------------------------------------------------------

    fn get_func(&mut self, name: &str) -> wasmtime::Func {
        let wasmtime::Extern::Func(func) = self
            .instance
            .get_export(&mut self.store, name)
            .unwrap_or_else(|| panic!("export {name:?} not found"))
        else {
            panic!("export {name:?} is not a function");
        };
        func
    }

    fn call_0(&mut self, name: &str) -> Result<(), VMLogicError> {
        let func = self.get_func(name);
        let func = func.typed::<(), ()>(&self.store).unwrap();
        func.call(&mut self.store, ()).map_err(extract_vm_logic_error)
    }

    fn call_0_ret(&mut self, name: &str) -> Result<u64, VMLogicError> {
        let func = self.get_func(name);
        let func = func.typed::<(), u64>(&self.store).unwrap();
        func.call(&mut self.store, ()).map_err(extract_vm_logic_error)
    }

    fn call_1(&mut self, name: &str, a: u64) -> Result<(), VMLogicError> {
        let func = self.get_func(name);
        let func = func.typed::<(u64,), ()>(&self.store).unwrap();
        func.call(&mut self.store, (a,)).map_err(extract_vm_logic_error)
    }

    fn call_1_ret(&mut self, name: &str, a: u64) -> Result<u64, VMLogicError> {
        let func = self.get_func(name);
        let func = func.typed::<(u64,), u64>(&self.store).unwrap();
        func.call(&mut self.store, (a,)).map_err(extract_vm_logic_error)
    }

    fn call_2(&mut self, name: &str, a: u64, b: u64) -> Result<(), VMLogicError> {
        let func = self.get_func(name);
        let func = func.typed::<(u64, u64), ()>(&self.store).unwrap();
        func.call(&mut self.store, (a, b)).map_err(extract_vm_logic_error)
    }

    fn call_2_ret(&mut self, name: &str, a: u64, b: u64) -> Result<u64, VMLogicError> {
        let func = self.get_func(name);
        let func = func.typed::<(u64, u64), u64>(&self.store).unwrap();
        func.call(&mut self.store, (a, b)).map_err(extract_vm_logic_error)
    }

    fn call_3(&mut self, name: &str, a: u64, b: u64, c: u64) -> Result<(), VMLogicError> {
        let func = self.get_func(name);
        let func = func.typed::<(u64, u64, u64), ()>(&self.store).unwrap();
        func.call(&mut self.store, (a, b, c)).map_err(extract_vm_logic_error)
    }

    fn call_3_ret(&mut self, name: &str, a: u64, b: u64, c: u64) -> Result<u64, VMLogicError> {
        let func = self.get_func(name);
        let func = func.typed::<(u64, u64, u64), u64>(&self.store).unwrap();
        func.call(&mut self.store, (a, b, c)).map_err(extract_vm_logic_error)
    }

    fn call_4(&mut self, name: &str, a: u64, b: u64, c: u64, d: u64) -> Result<(), VMLogicError> {
        let func = self.get_func(name);
        let func = func.typed::<(u64, u64, u64, u64), ()>(&self.store).unwrap();
        func.call(&mut self.store, (a, b, c, d)).map_err(extract_vm_logic_error)
    }

    fn call_4_ret(
        &mut self,
        name: &str,
        a: u64,
        b: u64,
        c: u64,
        d: u64,
    ) -> Result<u64, VMLogicError> {
        let func = self.get_func(name);
        let func = func.typed::<(u64, u64, u64, u64), u64>(&self.store).unwrap();
        func.call(&mut self.store, (a, b, c, d)).map_err(extract_vm_logic_error)
    }

    fn call_5(
        &mut self,
        name: &str,
        a: u64,
        b: u64,
        c: u64,
        d: u64,
        e: u64,
    ) -> Result<(), VMLogicError> {
        let func = self.get_func(name);
        let func = func.typed::<(u64, u64, u64, u64, u64), ()>(&self.store).unwrap();
        func.call(&mut self.store, (a, b, c, d, e)).map_err(extract_vm_logic_error)
    }

    fn call_5_ret(
        &mut self,
        name: &str,
        a: u64,
        b: u64,
        c: u64,
        d: u64,
        e: u64,
    ) -> Result<u64, VMLogicError> {
        let func = self.get_func(name);
        let func = func.typed::<(u64, u64, u64, u64, u64), u64>(&self.store).unwrap();
        func.call(&mut self.store, (a, b, c, d, e)).map_err(extract_vm_logic_error)
    }

    fn call_6(
        &mut self,
        name: &str,
        a: u64,
        b: u64,
        c: u64,
        d: u64,
        e: u64,
        f: u64,
    ) -> Result<(), VMLogicError> {
        let func = self.get_func(name);
        let func = func.typed::<(u64, u64, u64, u64, u64, u64), ()>(&self.store).unwrap();
        func.call(&mut self.store, (a, b, c, d, e, f)).map_err(extract_vm_logic_error)
    }

    fn call_7(
        &mut self,
        name: &str,
        a: u64,
        b: u64,
        c: u64,
        d: u64,
        e: u64,
        f: u64,
        g: u64,
    ) -> Result<(), VMLogicError> {
        let func = self.get_func(name);
        let func = func.typed::<(u64, u64, u64, u64, u64, u64, u64), ()>(&self.store).unwrap();
        func.call(&mut self.store, (a, b, c, d, e, f, g)).map_err(extract_vm_logic_error)
    }

    fn call_7_ret(
        &mut self,
        name: &str,
        a: u64,
        b: u64,
        c: u64,
        d: u64,
        e: u64,
        f: u64,
        g: u64,
    ) -> Result<u64, VMLogicError> {
        let func = self.get_func(name);
        let func = func.typed::<(u64, u64, u64, u64, u64, u64, u64), u64>(&self.store).unwrap();
        func.call(&mut self.store, (a, b, c, d, e, f, g)).map_err(extract_vm_logic_error)
    }

    fn call_8(
        &mut self,
        name: &str,
        a: u64,
        b: u64,
        c: u64,
        d: u64,
        e: u64,
        f: u64,
        g: u64,
        h: u64,
    ) -> Result<(), VMLogicError> {
        let func = self.get_func(name);
        let func = func.typed::<(u64, u64, u64, u64, u64, u64, u64, u64), ()>(&self.store).unwrap();
        func.call(&mut self.store, (a, b, c, d, e, f, g, h)).map_err(extract_vm_logic_error)
    }

    fn call_8_ret(
        &mut self,
        name: &str,
        a: u64,
        b: u64,
        c: u64,
        d: u64,
        e: u64,
        f: u64,
        g: u64,
        h: u64,
    ) -> Result<u64, VMLogicError> {
        let func = self.get_func(name);
        let func =
            func.typed::<(u64, u64, u64, u64, u64, u64, u64, u64), u64>(&self.store).unwrap();
        func.call(&mut self.store, (a, b, c, d, e, f, g, h)).map_err(extract_vm_logic_error)
    }

    fn call_9(
        &mut self,
        name: &str,
        a: u64,
        b: u64,
        c: u64,
        d: u64,
        e: u64,
        f: u64,
        g: u64,
        h: u64,
        i: u64,
    ) -> Result<(), VMLogicError> {
        let func = self.get_func(name);
        let func =
            func.typed::<(u64, u64, u64, u64, u64, u64, u64, u64, u64), ()>(&self.store).unwrap();
        func.call(&mut self.store, (a, b, c, d, e, f, g, h, i)).map_err(extract_vm_logic_error)
    }

    fn call_9_ret(
        &mut self,
        name: &str,
        a: u64,
        b: u64,
        c: u64,
        d: u64,
        e: u64,
        f: u64,
        g: u64,
        h: u64,
        i: u64,
    ) -> Result<u64, VMLogicError> {
        let func = self.get_func(name);
        let func =
            func.typed::<(u64, u64, u64, u64, u64, u64, u64, u64, u64), u64>(&self.store).unwrap();
        func.call(&mut self.store, (a, b, c, d, e, f, g, h, i)).map_err(extract_vm_logic_error)
    }

    fn call_abort(
        &mut self,
        msg_ptr: u32,
        filename_ptr: u32,
        line: u32,
        col: u32,
    ) -> Result<(), VMLogicError> {
        let func = self.get_func("abort");
        let func = func.typed::<(u32, u32, u32, u32), ()>(&self.store).unwrap();
        func.call(&mut self.store, (msg_ptr, filename_ptr, line, col))
            .map_err(extract_vm_logic_error)
    }

    // ---------------------------------------------------------------
    // Registers API
    // ---------------------------------------------------------------

    pub(crate) fn read_register(&mut self, register_id: u64, ptr: u64) -> Result<(), VMLogicError> {
        self.call_2("read_register", register_id, ptr)
    }

    pub(crate) fn register_len(&mut self, register_id: u64) -> Result<u64, VMLogicError> {
        self.call_1_ret("register_len", register_id)
    }

    pub(crate) fn write_register(
        &mut self,
        register_id: u64,
        data_len: u64,
        data_ptr: u64,
    ) -> Result<(), VMLogicError> {
        self.call_3("write_register", register_id, data_len, data_ptr)
    }

    // ---------------------------------------------------------------
    // Context API
    // ---------------------------------------------------------------

    pub(crate) fn current_account_id(&mut self, register_id: u64) -> Result<(), VMLogicError> {
        self.call_1("current_account_id", register_id)
    }

    pub(crate) fn signer_account_id(&mut self, register_id: u64) -> Result<(), VMLogicError> {
        self.call_1("signer_account_id", register_id)
    }

    pub(crate) fn signer_account_pk(&mut self, register_id: u64) -> Result<(), VMLogicError> {
        self.call_1("signer_account_pk", register_id)
    }

    pub(crate) fn predecessor_account_id(&mut self, register_id: u64) -> Result<(), VMLogicError> {
        self.call_1("predecessor_account_id", register_id)
    }

    pub(crate) fn input(&mut self, register_id: u64) -> Result<(), VMLogicError> {
        self.call_1("input", register_id)
    }

    pub(crate) fn block_index(&mut self) -> Result<u64, VMLogicError> {
        self.call_0_ret("block_index")
    }

    pub(crate) fn block_timestamp(&mut self) -> Result<u64, VMLogicError> {
        self.call_0_ret("block_timestamp")
    }

    pub(crate) fn epoch_height(&mut self) -> Result<u64, VMLogicError> {
        self.call_0_ret("epoch_height")
    }

    pub(crate) fn storage_usage(&mut self) -> Result<u64, VMLogicError> {
        self.call_0_ret("storage_usage")
    }

    // ---------------------------------------------------------------
    // Economics API
    // ---------------------------------------------------------------

    pub(crate) fn account_balance(&mut self, balance_ptr: u64) -> Result<(), VMLogicError> {
        self.call_1("account_balance", balance_ptr)
    }

    pub(crate) fn account_locked_balance(&mut self, balance_ptr: u64) -> Result<(), VMLogicError> {
        self.call_1("account_locked_balance", balance_ptr)
    }

    pub(crate) fn attached_deposit(&mut self, balance_ptr: u64) -> Result<(), VMLogicError> {
        self.call_1("attached_deposit", balance_ptr)
    }

    pub(crate) fn prepaid_gas(&mut self) -> Result<u64, VMLogicError> {
        self.call_0_ret("prepaid_gas")
    }

    pub(crate) fn used_gas(&mut self) -> Result<u64, VMLogicError> {
        self.call_0_ret("used_gas")
    }

    // ---------------------------------------------------------------
    // Math API
    // ---------------------------------------------------------------

    pub(crate) fn random_seed(&mut self, register_id: u64) -> Result<(), VMLogicError> {
        self.call_1("random_seed", register_id)
    }

    pub(crate) fn sha256(
        &mut self,
        value_len: u64,
        value_ptr: u64,
        register_id: u64,
    ) -> Result<(), VMLogicError> {
        self.call_3("sha256", value_len, value_ptr, register_id)
    }

    pub(crate) fn keccak256(
        &mut self,
        value_len: u64,
        value_ptr: u64,
        register_id: u64,
    ) -> Result<(), VMLogicError> {
        self.call_3("keccak256", value_len, value_ptr, register_id)
    }

    pub(crate) fn keccak512(
        &mut self,
        value_len: u64,
        value_ptr: u64,
        register_id: u64,
    ) -> Result<(), VMLogicError> {
        self.call_3("keccak512", value_len, value_ptr, register_id)
    }

    pub(crate) fn ripemd160(
        &mut self,
        value_len: u64,
        value_ptr: u64,
        register_id: u64,
    ) -> Result<(), VMLogicError> {
        self.call_3("ripemd160", value_len, value_ptr, register_id)
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
        self.call_7_ret(
            "ecrecover",
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
        let func = self.get_func("ed25519_verify");
        let func = func.typed::<(u64, u64, u64, u64, u64, u64), u64>(&self.store).unwrap();
        func.call(&mut self.store, (sig_len, sig_ptr, msg_len, msg_ptr, pub_key_len, pub_key_ptr))
            .map_err(extract_vm_logic_error)
    }

    // ---------------------------------------------------------------
    // Miscellaneous API
    // ---------------------------------------------------------------

    pub(crate) fn value_return(
        &mut self,
        value_len: u64,
        value_ptr: u64,
    ) -> Result<(), VMLogicError> {
        self.call_2("value_return", value_len, value_ptr)
    }

    pub(crate) fn panic(&mut self) -> Result<(), VMLogicError> {
        self.call_0("panic")
    }

    pub(crate) fn panic_utf8(&mut self, len: u64, ptr: u64) -> Result<(), VMLogicError> {
        self.call_2("panic_utf8", len, ptr)
    }

    pub(crate) fn log_utf8(&mut self, len: u64, ptr: u64) -> Result<(), VMLogicError> {
        self.call_2("log_utf8", len, ptr)
    }

    pub(crate) fn log_utf16(&mut self, len: u64, ptr: u64) -> Result<(), VMLogicError> {
        self.call_2("log_utf16", len, ptr)
    }

    pub(crate) fn abort(
        &mut self,
        msg_ptr: u32,
        filename_ptr: u32,
        line: u32,
        col: u32,
    ) -> Result<(), VMLogicError> {
        self.call_abort(msg_ptr, filename_ptr, line, col)
    }

    // ---------------------------------------------------------------
    // Promises API
    // ---------------------------------------------------------------

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
        self.call_8_ret(
            "promise_create",
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
        self.call_9_ret(
            "promise_then",
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
        self.call_2_ret("promise_and", promise_idx_ptr, promise_idx_count)
    }

    pub(crate) fn promise_batch_create(
        &mut self,
        account_id_len: u64,
        account_id_ptr: u64,
    ) -> Result<u64, VMLogicError> {
        self.call_2_ret("promise_batch_create", account_id_len, account_id_ptr)
    }

    pub(crate) fn promise_batch_then(
        &mut self,
        promise_index: u64,
        account_id_len: u64,
        account_id_ptr: u64,
    ) -> Result<u64, VMLogicError> {
        self.call_3_ret("promise_batch_then", promise_index, account_id_len, account_id_ptr)
    }

    pub(crate) fn promise_set_refund_to(
        &mut self,
        promise_index: u64,
        account_id_len: u64,
        account_id_ptr: u64,
    ) -> Result<(), VMLogicError> {
        self.call_3("promise_set_refund_to", promise_index, account_id_len, account_id_ptr)
    }

    pub(crate) fn promise_batch_action_state_init(
        &mut self,
        promise_idx: u64,
        code_len: u64,
        code_ptr: u64,
        amount_ptr: u64,
    ) -> Result<u64, VMLogicError> {
        self.call_4_ret(
            "promise_batch_action_state_init",
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
        self.call_4_ret(
            "promise_batch_action_state_init_by_account_id",
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
        self.call_6(
            "set_state_init_data_entry",
            promise_idx,
            action_index,
            key_len,
            key_ptr,
            value_len,
            value_ptr,
        )
    }

    pub(crate) fn current_contract_code(&mut self, register_id: u64) -> Result<u64, VMLogicError> {
        self.call_1_ret("current_contract_code", register_id)
    }

    pub(crate) fn refund_to_account_id(&mut self, register_id: u64) -> Result<(), VMLogicError> {
        self.call_1("refund_to_account_id", register_id)
    }

    // ---------------------------------------------------------------
    // Promise API actions
    // ---------------------------------------------------------------

    pub(crate) fn promise_batch_action_create_account(
        &mut self,
        promise_index: u64,
    ) -> Result<(), VMLogicError> {
        self.call_1("promise_batch_action_create_account", promise_index)
    }

    pub(crate) fn promise_batch_action_deploy_contract(
        &mut self,
        promise_index: u64,
        code_len: u64,
        code_ptr: u64,
    ) -> Result<(), VMLogicError> {
        self.call_3("promise_batch_action_deploy_contract", promise_index, code_len, code_ptr)
    }

    pub(crate) fn promise_batch_action_deploy_global_contract(
        &mut self,
        promise_index: u64,
        code_len: u64,
        code_ptr: u64,
    ) -> Result<(), VMLogicError> {
        self.call_3(
            "promise_batch_action_deploy_global_contract",
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
        self.call_3(
            "promise_batch_action_deploy_global_contract_by_account_id",
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
        self.call_3(
            "promise_batch_action_use_global_contract",
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
        self.call_3(
            "promise_batch_action_use_global_contract_by_account_id",
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
        self.call_7(
            "promise_batch_action_function_call",
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
        self.call_8(
            "promise_batch_action_function_call_weight",
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
        self.call_2("promise_batch_action_transfer", promise_index, amount_ptr)
    }

    pub(crate) fn promise_batch_action_stake(
        &mut self,
        promise_index: u64,
        amount_ptr: u64,
        public_key_len: u64,
        public_key_ptr: u64,
    ) -> Result<(), VMLogicError> {
        self.call_4(
            "promise_batch_action_stake",
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
        self.call_4(
            "promise_batch_action_add_key_with_full_access",
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
        self.call_9(
            "promise_batch_action_add_key_with_function_call",
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
        self.call_3(
            "promise_batch_action_delete_key",
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
        self.call_3(
            "promise_batch_action_delete_account",
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
        self.call_4(
            "promise_batch_action_transfer_to_gas_key",
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
        self.call_4(
            "promise_batch_action_add_gas_key_with_full_access",
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
        self.call_9(
            "promise_batch_action_add_gas_key_with_function_call",
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

    // ---------------------------------------------------------------
    // Promise API yield/resume
    // ---------------------------------------------------------------

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
        self.call_7_ret(
            "promise_yield_create",
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
        let func = self.get_func("promise_yield_resume");
        let func = func.typed::<(u64, u64, u64, u64), u32>(&self.store).unwrap();
        func.call(&mut self.store, (data_id_len, data_id_ptr, payload_len, payload_ptr))
            .map_err(extract_vm_logic_error)
    }

    // ---------------------------------------------------------------
    // Promise API results
    // ---------------------------------------------------------------

    pub(crate) fn promise_results_count(&mut self) -> Result<u64, VMLogicError> {
        self.call_0_ret("promise_results_count")
    }

    pub(crate) fn promise_result(
        &mut self,
        result_idx: u64,
        register_id: u64,
    ) -> Result<u64, VMLogicError> {
        self.call_2_ret("promise_result", result_idx, register_id)
    }

    pub(crate) fn promise_return(&mut self, promise_idx: u64) -> Result<(), VMLogicError> {
        self.call_1("promise_return", promise_idx)
    }

    // ---------------------------------------------------------------
    // Storage API
    // ---------------------------------------------------------------

    pub(crate) fn storage_write(
        &mut self,
        key_len: u64,
        key_ptr: u64,
        value_len: u64,
        value_ptr: u64,
        register_id: u64,
    ) -> Result<u64, VMLogicError> {
        self.call_5_ret("storage_write", key_len, key_ptr, value_len, value_ptr, register_id)
    }

    pub(crate) fn storage_read(
        &mut self,
        key_len: u64,
        key_ptr: u64,
        register_id: u64,
    ) -> Result<u64, VMLogicError> {
        self.call_3_ret("storage_read", key_len, key_ptr, register_id)
    }

    pub(crate) fn storage_remove(
        &mut self,
        key_len: u64,
        key_ptr: u64,
        register_id: u64,
    ) -> Result<u64, VMLogicError> {
        self.call_3_ret("storage_remove", key_len, key_ptr, register_id)
    }

    pub(crate) fn storage_has_key(
        &mut self,
        key_len: u64,
        key_ptr: u64,
    ) -> Result<u64, VMLogicError> {
        self.call_2_ret("storage_has_key", key_len, key_ptr)
    }

    pub(crate) fn storage_iter_prefix(
        &mut self,
        prefix_len: u64,
        prefix_ptr: u64,
    ) -> Result<u64, VMLogicError> {
        self.call_2_ret("storage_iter_prefix", prefix_len, prefix_ptr)
    }

    pub(crate) fn storage_iter_range(
        &mut self,
        start_len: u64,
        start_ptr: u64,
        end_len: u64,
        end_ptr: u64,
    ) -> Result<u64, VMLogicError> {
        self.call_4_ret("storage_iter_range", start_len, start_ptr, end_len, end_ptr)
    }

    pub(crate) fn storage_iter_next(
        &mut self,
        iterator_id: u64,
        key_register_id: u64,
        value_register_id: u64,
    ) -> Result<u64, VMLogicError> {
        self.call_3_ret("storage_iter_next", iterator_id, key_register_id, value_register_id)
    }

    // ---------------------------------------------------------------
    // Gas
    // ---------------------------------------------------------------

    pub(crate) fn gas_seen_from_wasm(&mut self, opcodes: u32) -> Result<(), VMLogicError> {
        let func = self.get_func("gas");
        let func = func.typed::<(u32,), ()>(&self.store).unwrap();
        func.call(&mut self.store, (opcodes,)).map_err(extract_vm_logic_error)
    }

    pub(crate) fn gas_opcodes(&mut self, opcodes: u32) -> Result<(), VMLogicError> {
        let config = Arc::clone(&self.store.data().config);
        let gas = opcodes as u64 * config.regular_op_cost as u64;
        let ctx = self.store.data_mut();
        ctx.result_state.gas_counter.burn_gas(near_primitives_core::gas::Gas::from_gas(gas))
    }

    // ---------------------------------------------------------------
    // Validator API
    // ---------------------------------------------------------------

    pub(crate) fn validator_stake(
        &mut self,
        account_id_len: u64,
        account_id_ptr: u64,
        stake_ptr: u64,
    ) -> Result<(), VMLogicError> {
        self.call_3("validator_stake", account_id_len, account_id_ptr, stake_ptr)
    }

    pub(crate) fn validator_total_stake(&mut self, stake_ptr: u64) -> Result<(), VMLogicError> {
        self.call_1("validator_total_stake", stake_ptr)
    }

    // ---------------------------------------------------------------
    // Alt BN128
    // ---------------------------------------------------------------

    pub(crate) fn alt_bn128_g1_multiexp(
        &mut self,
        value_len: u64,
        value_ptr: u64,
        register_id: u64,
    ) -> Result<(), VMLogicError> {
        self.call_3("alt_bn128_g1_multiexp", value_len, value_ptr, register_id)
    }

    pub(crate) fn alt_bn128_g1_sum(
        &mut self,
        value_len: u64,
        value_ptr: u64,
        register_id: u64,
    ) -> Result<(), VMLogicError> {
        self.call_3("alt_bn128_g1_sum", value_len, value_ptr, register_id)
    }

    pub(crate) fn alt_bn128_pairing_check(
        &mut self,
        value_len: u64,
        value_ptr: u64,
    ) -> Result<u64, VMLogicError> {
        self.call_2_ret("alt_bn128_pairing_check", value_len, value_ptr)
    }

    // ---------------------------------------------------------------
    // BLS12-381
    // ---------------------------------------------------------------

    pub(crate) fn bls12381_p1_sum(
        &mut self,
        value_len: u64,
        value_ptr: u64,
        register_id: u64,
    ) -> Result<u64, VMLogicError> {
        self.call_3_ret("bls12381_p1_sum", value_len, value_ptr, register_id)
    }

    pub(crate) fn bls12381_p2_sum(
        &mut self,
        value_len: u64,
        value_ptr: u64,
        register_id: u64,
    ) -> Result<u64, VMLogicError> {
        self.call_3_ret("bls12381_p2_sum", value_len, value_ptr, register_id)
    }

    pub(crate) fn bls12381_g1_multiexp(
        &mut self,
        value_len: u64,
        value_ptr: u64,
        register_id: u64,
    ) -> Result<u64, VMLogicError> {
        self.call_3_ret("bls12381_g1_multiexp", value_len, value_ptr, register_id)
    }

    pub(crate) fn bls12381_g2_multiexp(
        &mut self,
        value_len: u64,
        value_ptr: u64,
        register_id: u64,
    ) -> Result<u64, VMLogicError> {
        self.call_3_ret("bls12381_g2_multiexp", value_len, value_ptr, register_id)
    }

    pub(crate) fn bls12381_map_fp_to_g1(
        &mut self,
        value_len: u64,
        value_ptr: u64,
        register_id: u64,
    ) -> Result<u64, VMLogicError> {
        self.call_3_ret("bls12381_map_fp_to_g1", value_len, value_ptr, register_id)
    }

    pub(crate) fn bls12381_map_fp2_to_g2(
        &mut self,
        value_len: u64,
        value_ptr: u64,
        register_id: u64,
    ) -> Result<u64, VMLogicError> {
        self.call_3_ret("bls12381_map_fp2_to_g2", value_len, value_ptr, register_id)
    }

    pub(crate) fn bls12381_pairing_check(
        &mut self,
        value_len: u64,
        value_ptr: u64,
    ) -> Result<u64, VMLogicError> {
        self.call_2_ret("bls12381_pairing_check", value_len, value_ptr)
    }

    pub(crate) fn bls12381_p1_decompress(
        &mut self,
        value_len: u64,
        value_ptr: u64,
        register_id: u64,
    ) -> Result<u64, VMLogicError> {
        self.call_3_ret("bls12381_p1_decompress", value_len, value_ptr, register_id)
    }

    pub(crate) fn bls12381_p2_decompress(
        &mut self,
        value_len: u64,
        value_ptr: u64,
        register_id: u64,
    ) -> Result<u64, VMLogicError> {
        self.call_3_ret("bls12381_p2_decompress", value_len, value_ptr, register_id)
    }

    // ---------------------------------------------------------------
    // Test helpers — access Ctx directly (no wasm call needed)
    // ---------------------------------------------------------------

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wat_shim_covers_all_host_functions() {
        let config = test_vm_config(None);
        let wat = build_wat_shim(&config);
        let wat_bytes = wat::parse_str(&wat).expect("WAT shim should parse");
        let engine = Engine::default();
        let module = Module::new(&engine, &wat_bytes).expect("WAT shim should compile");
        let func_export_count = module.exports().filter(|e| e.ty().func().is_some()).count();
        // If you add a host function to imports.rs, update this count and add
        // a delegate method to WasmtimeTestLogic.
        let expected = if cfg!(feature = "nightly") { 98 } else { 95 };
        assert_eq!(
            func_export_count, expected,
            "WAT shim export count changed — did you add/remove a host function?"
        );
    }
}
