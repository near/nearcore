wasmtime::component::bindgen!({
    imports: {
        default: trappable,
    },
    with: {
        "near:nearcore/runtime@0.1.0/account-id": near_primitives_core::types::AccountId,
        "near:nearcore/runtime@0.1.0/promise": crate::logic::types::PromiseIndex,
        "near:nearcore/runtime@0.1.0/promise-action": crate::logic::types::ActionIndex,
    },
});
