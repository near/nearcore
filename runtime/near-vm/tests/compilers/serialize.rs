use anyhow::Result;
use near_vm_test_api::*;

#[compiler_test(serialize)]
fn test_serialize(config: crate::Config) -> Result<()> {
    let store = config.store();
    let wasm = wat2wasm(
        r#"
        (module
        (func $hello (import "" "hello"))
        (func (export "run") (call $hello))
        )
    "#
        .as_bytes(),
    )
    .unwrap();
    let engine = store.engine();
    let tunables = BaseTunables::for_target(engine.target());
    let executable = engine.compile_universal(&wasm, &tunables).unwrap();
    let serialized = executable.serialize().unwrap();
    assert!(!serialized.is_empty());
    Ok(())
}

// #[compiler_test(serialize)]
// fn test_deserialize(config: crate::Config) -> Result<()> {
//     let store = config.store();
//     let wasm = wat2wasm(r#"
//         (module $name
//             (import "host" "sum_part" (func (param i32 i64 i32 f32 f64) (result i64)))
//             (func (export "test_call") (result i64)
//                 i32.const 100
//                 i64.const 200
//                 i32.const 300
//                 f32.const 400
//                 f64.const 500
//                 call 0
//             )
//         )
//     "#.as_bytes()).unwrap();
//
//     let engine = store.engine();
//     let tunables = BaseTunables::for_target(engine.target());
//     let executable = engine.compile(&wasm, &tunables).unwrap();
//     let writer = std::io::Cursor::new(vec![]);
//     executable.serialize(&mut writer).unwrap();
//     let serialized_bytes = writer.into_inner();
//
//     let headless_store = config.headless_store();
//
//
//     let deserialized_module = unsafe { Module::deserialize(&headless_store, &serialized_bytes)? };
//     assert_eq!(deserialized_module.name(), Some("name"));
//     assert_eq!(
//         deserialized_module.artifact().module_ref().exports().collect::<Vec<_>>(),
//         module.exports().collect::<Vec<_>>()
//     );
//     assert_eq!(
//         deserialized_module.artifact().module_ref().imports().collect::<Vec<_>>(),
//         module.imports().collect::<Vec<_>>()
//     );
//
//     let func_type = FunctionType::new(
//         vec![Type::I32, Type::I64, Type::I32, Type::F32, Type::F64],
//         vec![Type::I64],
//     );
//     let instance = Instance::new_with_config(
//         &module,
//         InstanceConfig::with_stack_limit(1000000),
//         &imports! {
//             "host" => {
//                 "sum_part" => Function::new(&store, &func_type, |params| {
//                     let param_0: i64 = params[0].unwrap_i32() as i64;
//                     let param_1: i64 = params[1].unwrap_i64() as i64;
//                     let param_2: i64 = params[2].unwrap_i32() as i64;
//                     let param_3: i64 = params[3].unwrap_f32() as i64;
//                     let param_4: i64 = params[4].unwrap_f64() as i64;
//                     Ok(vec![Value::I64(param_0 + param_1 + param_2 + param_3 + param_4)])
//                 })
//             }
//         },
//     )?;
//
//     let test_call = instance.exports.get_function("test_call")?;
//     let result = test_call.call(&[])?;
//     assert_eq!(result.to_vec(), vec![Value::I64(1500)]);
//     Ok(())
// }
