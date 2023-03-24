use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use wasmer::*;
use wasmer_engine_universal::UniversalExecutableRef;

pub struct LargeContract {
    pub functions: u32,
    pub locals_per_function: u32,
    pub panic_imports: u32, // How many times to import `env.panic`
}

impl Default for LargeContract {
    fn default() -> Self {
        Self {
            functions: 1,
            locals_per_function: 0,
            panic_imports: 0,
        }
    }
}

impl LargeContract {
    /// Construct a contract with many entitites.
    ///
    /// Currently supports constructing contracts that contain a specified number of functions with the
    /// specified number of locals each.
    ///
    /// Exports a function called `main` that does nothing.
    pub fn make(&self) -> Vec<u8> {
        use wasm_encoder::{
            CodeSection, EntityType, Export, ExportSection, Function, FunctionSection,
            ImportSection, Instruction, Module, TypeSection, ValType,
        };

        // Won't generate a valid WASM without functions.
        assert!(
            self.functions >= 1,
            "must specify at least 1 function to be generated"
        );
        let mut module = Module::new();
        let mut type_section = TypeSection::new();
        type_section.function([], []);
        module.section(&type_section);

        if self.panic_imports != 0 {
            let mut import_section = ImportSection::new();
            for _ in 0..self.panic_imports {
                import_section.import("env", "panic", EntityType::Function(0));
            }
            module.section(&import_section);
        }

        let mut functions_section = FunctionSection::new();
        for _ in 0..self.functions {
            functions_section.function(0);
        }
        module.section(&functions_section);

        let mut exports_section = ExportSection::new();
        exports_section.export("main", Export::Function(0));
        module.section(&exports_section);

        let mut code_section = CodeSection::new();
        for _ in 0..self.functions {
            let mut f = Function::new([(self.locals_per_function, ValType::I64)]);
            f.instruction(&Instruction::End);
            code_section.function(&f);
        }
        module.section(&code_section);

        module.finish()
    }
}

fn many_functions(c: &mut Criterion) {
    let mut group = c.benchmark_group("many_functions");
    for functions in [1, 10, 100, 1000, 10000] {
        let wasm = LargeContract {
            functions,
            ..Default::default()
        }
        .make();
        let store = Store::new(&Universal::new(Singlepass::new()).engine());
        group.bench_function(BenchmarkId::new("compile+instantiate", functions), |b| {
            b.iter(|| {
                let module = Module::new(&store, &wasm).unwrap();
                let imports = imports! {};
                let _ = Instance::new(&module, &imports).unwrap();
            })
        });

        let module = Module::new(&store, &wasm).unwrap();
        let imports = imports! {};
        let instance = Instance::new(&module, &imports).unwrap();
        group.bench_function(BenchmarkId::new("lookup_main", functions), |b| {
            b.iter(|| {
                let _: Function = instance.lookup_function("main").unwrap();
            })
        });

        let main: Function = instance.lookup_function("main").unwrap();
        group.bench_function(BenchmarkId::new("call_main", functions), |b| {
            b.iter(|| {
                black_box(main.call(&[]).unwrap());
            })
        });

        let wasm = wat::parse_bytes(wasm.as_ref()).unwrap();
        let executable = store.engine().compile(&wasm, store.tunables()).unwrap();
        group.bench_function(BenchmarkId::new("serialize", functions), |b| {
            b.iter(|| {
                black_box(executable.serialize().unwrap());
            })
        });

        let serialized = executable.serialize().unwrap();
        group.bench_function(BenchmarkId::new("load", functions), |b| {
            b.iter(|| unsafe {
                let deserialized = UniversalExecutableRef::deserialize(&serialized).unwrap();
                black_box(store.engine().load(&deserialized).unwrap());
            })
        });
    }
}

fn many_locals(c: &mut Criterion) {
    let mut group = c.benchmark_group("many_locals");
    for (functions, locals_per_function) in [(10, 100), (100, 1000), (1000, 10000)] {
        let wasm = LargeContract {
            functions,
            locals_per_function,
            ..Default::default()
        }
        .make();
        let size = functions * locals_per_function;
        let store = Store::new(&Universal::new(Singlepass::new()).engine());
        group.bench_function(BenchmarkId::new("compile+instantiate", size), |b| {
            b.iter(|| {
                let module = Module::new(&store, &wasm).unwrap();
                let imports = imports! {};
                let _ = Instance::new(&module, &imports).unwrap();
            })
        });

        let wasm = wat::parse_bytes(wasm.as_ref()).unwrap();
        let executable = store.engine().compile(&wasm, store.tunables()).unwrap();
        group.bench_function(BenchmarkId::new("serialize", size), |b| {
            b.iter(|| {
                black_box(executable.serialize().unwrap());
            })
        });

        let serialized = executable.serialize().unwrap();
        group.bench_function(BenchmarkId::new("load", size), |b| {
            b.iter(|| unsafe {
                let deserialized = UniversalExecutableRef::deserialize(&serialized).unwrap();
                black_box(store.engine().load(&deserialized).unwrap());
            })
        });
    }
}

criterion_group! {
    name = functions;
    config = Criterion::default();
    targets = many_functions
}
criterion_group! {
    name = locals;
    config = Criterion::default();
    targets = many_locals
}

criterion_main!(functions, locals);
