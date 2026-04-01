use super::Ctx;
use super::logic;
use crate::logic::errors::VMLogicError;
use crate::logic::gas_counter::GasCounter;
use crate::logic::mocks::mock_external::MockedExternal;
use crate::logic::mocks::mock_memory::MockedMemory;
use crate::logic::vmstate::Registers;
use crate::logic::{Config, ExecutionResultState, MemSlice, VMContext, VMOutcome};
use near_parameters::RuntimeFeesConfig;
use std::marker::PhantomData;
use std::sync::{Arc, LazyLock};
use wasmtime::{Engine, Memory, Module, Store};

static CACHED_ENGINE_MODULE: LazyLock<(Engine, Module)> = LazyLock::new(|| {
    let engine = Engine::default();
    let wasm = wat::parse_str("(module (memory (export \"memory\") 1))").unwrap();
    let module = Module::new(&engine, &wasm).unwrap();
    (engine, module)
});

/// Wasmtime-backed test logic that calls host function implementations
/// directly. The lifetime `'a` ties it to the `VMLogicBuilder` that created
/// it, preventing the transmuted `'static` references from escaping.
pub(crate) struct WasmtimeTestLogic<'a> {
    store: Store<Ctx>,
    memory: Memory,
    mem_write_offset: u64,
    _lifetime: PhantomData<&'a mut ()>,
}

/// Generates a delegate method that forwards to `logic::$func(ctx, mem, ...)`.
/// Internal (finite-wasm instrumentation) imports are skipped.
macro_rules! delegate_import {
    (@in internal : $func:ident < [ $( $arg_name:ident : $arg_type:ident ),* ] -> [ $( $returns:ident ),* ] >) => {};
    ($( @as $name:ident : )? $func:ident < [ $( $arg_name:ident : $arg_type:ident ),* ] -> [ $( $returns:ident ),* ] >) => {
        #[allow(dead_code, unused_parens)]
        pub(crate) fn $func(&mut self, $( $arg_name: $arg_type ),*) -> Result<($($returns),*), VMLogicError> {
            let (mem, ctx) = self.ctx_and_mem();
            logic::$func(ctx, mem, $( $arg_name ),*)
        }
    };
}

impl WasmtimeTestLogic<'_> {
    pub(crate) fn new<'a>(
        ext: &'a mut MockedExternal,
        context: &'a VMContext,
        fees_config: RuntimeFeesConfig,
        config: Config,
    ) -> WasmtimeTestLogic<'a> {
        // SAFETY: the 'static transmute is required by wasmtime's Store<Ctx>,
        // but the returned WasmtimeTestLogic<'a> borrows the builder via
        // PhantomData, so the compiler prevents it from outliving ext/context.
        let ext: &'static mut dyn crate::logic::External =
            unsafe { core::mem::transmute(ext as &mut dyn crate::logic::External) };
        let context: &'static VMContext = unsafe { core::mem::transmute(context) };

        let config = Arc::new(config);
        let (engine, module) = &*CACHED_ENGINE_MODULE;

        let result_state = ExecutionResultState::new(
            context,
            context.make_gas_counter(&config),
            Arc::clone(&config),
        );
        let dummy_memory_export = module.get_export_index("memory").unwrap();
        let ctx = Ctx::new(ext, context, Arc::new(fees_config), result_state, dummy_memory_export);

        let mut store = Store::new(engine, ctx);
        store.limiter(|ctx| &mut ctx.limits);

        let memory = Memory::new(&mut store, wasmtime::MemoryType::new(1, Some(1))).unwrap();
        store.data_mut().memory = super::Export::Resolved(memory);

        WasmtimeTestLogic { store, memory, mem_write_offset: 0, _lifetime: PhantomData }
    }

    fn ctx_and_mem(&mut self) -> (&mut [u8], &mut Ctx) {
        self.memory.data_and_store_mut(&mut self.store)
    }

    // Expands to pub(crate) fn $name(&mut self, ...) delegates for every
    // host function import, forwarding to logic::$func(ctx, mem, ...).
    crate::imports::for_each_import_item!(delegate_import);

    pub(crate) fn gas_opcodes(&mut self, opcodes: u32) -> Result<(), VMLogicError> {
        logic::gas_opcodes(&mut self.store.data_mut().result_state, opcodes)
    }

    pub(crate) fn result_state(&self) -> &ExecutionResultState {
        &self.store.data().result_state
    }

    pub(crate) fn gas_counter(&mut self) -> &mut GasCounter {
        &mut self.store.data_mut().result_state.gas_counter
    }

    pub(crate) fn config(&self) -> &Config {
        &self.store.data().config
    }

    pub(crate) fn registers(&mut self) -> &mut Registers {
        &mut self.store.data_mut().registers
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
        let ptr = MockedMemory::MEMORY_SIZE - len;
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
