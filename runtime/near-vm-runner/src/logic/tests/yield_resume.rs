use crate::logic::HostError;
use crate::logic::mocks::mock_external::MockAction;
use crate::logic::tests::vm_logic_builder::VMLogicBuilder;
use near_primitives_core::hash::CryptoHash;
use near_primitives_core::version::{PROTOCOL_VERSION, ProtocolFeature};

#[test]
fn test_promise_yield_create() {
    let mut logic_builder = VMLogicBuilder::free();
    let mut logic = logic_builder.build();

    let method_name = logic.internal_mem_write(b"callback");
    let args = logic.internal_mem_write(b"arg_data");
    let register_id = 0u64;

    let promise_idx = logic
        .promise_yield_create(
            method_name.len,
            method_name.ptr,
            args.len,
            args.ptr,
            0,
            1,
            register_id,
        )
        .expect("yield_create should succeed");

    assert_eq!(promise_idx, 0);

    // Verify data_id was written to the register (32 bytes)
    let data_id_len = logic.registers().get_len(register_id).unwrap();
    assert_eq!(data_id_len, CryptoHash::LENGTH as u64);

    drop(logic);

    // Verify a YieldCreate action was logged
    assert!(
        logic_builder.ext.action_log.iter().any(|a| matches!(a, MockAction::YieldCreate { .. })),
        "expected YieldCreate in action log"
    );
}

#[test]
fn test_promise_yield_create_empty_method_name() {
    let mut logic_builder = VMLogicBuilder::free();
    let mut logic = logic_builder.build();

    let method_name = logic.internal_mem_write(b"");
    let args = logic.internal_mem_write(b"args");

    let result =
        logic.promise_yield_create(method_name.len, method_name.ptr, args.len, args.ptr, 0, 1, 0);

    assert!(
        matches!(result, Err(crate::logic::VMLogicError::HostError(HostError::EmptyMethodName))),
        "expected EmptyMethodName error, got {result:?}"
    );
}

#[test]
fn test_promise_yield_resume() {
    let mut logic_builder = VMLogicBuilder::free();
    let mut logic = logic_builder.build();

    // First create a yield
    let method_name = logic.internal_mem_write(b"callback");
    let args = logic.internal_mem_write(b"args");
    logic
        .promise_yield_create(method_name.len, method_name.ptr, args.len, args.ptr, 0, 1, 0)
        .expect("yield_create should succeed");

    // Read the data_id from register 0
    let data_id_len = logic.registers().get_len(0).unwrap();
    let ptr = 1024u64;
    logic.read_register(0, ptr).unwrap();
    let data_id_bytes = logic.internal_mem_read(ptr, data_id_len);

    // Now resume with a payload
    let data_id_mem = logic.internal_mem_write(&data_id_bytes);
    let payload = logic.internal_mem_write(b"payload_data");

    let result = logic
        .promise_yield_resume(data_id_mem.len, data_id_mem.ptr, payload.len, payload.ptr)
        .expect("yield_resume should succeed");

    // MockExternal returns true (1) if a matching YieldCreate exists
    assert_eq!(result, 1u32);
}

#[test]
fn test_promise_yield_resume_unknown_data_id() {
    let mut logic_builder = VMLogicBuilder::free();
    let mut logic = logic_builder.build();

    // Try to resume with a data_id that was never created
    let fake_data_id = [0u8; 32];
    let data_id_mem = logic.internal_mem_write(&fake_data_id);
    let payload = logic.internal_mem_write(b"payload");

    let result = logic
        .promise_yield_resume(data_id_mem.len, data_id_mem.ptr, payload.len, payload.ptr)
        .expect("yield_resume should succeed (returning false)");

    assert_eq!(result, 0u32, "resume with unknown data_id should return 0");
}

#[test]
fn test_promise_yield_resume_malformed_data_id() {
    let mut logic_builder = VMLogicBuilder::free();
    let mut logic = logic_builder.build();

    // Pass a data_id that's not 32 bytes
    let bad_data_id = [0u8; 16];
    let data_id_mem = logic.internal_mem_write(&bad_data_id);
    let payload = logic.internal_mem_write(b"payload");

    let result =
        logic.promise_yield_resume(data_id_mem.len, data_id_mem.ptr, payload.len, payload.ptr);

    assert!(
        matches!(result, Err(crate::logic::VMLogicError::HostError(HostError::DataIdMalformed))),
        "expected DataIdMalformed error, got {result:?}"
    );
}

#[test]
fn test_promise_yield_create_with_id() {
    if !ProtocolFeature::YieldWithId.enabled(PROTOCOL_VERSION) {
        return;
    }

    let mut logic_builder = VMLogicBuilder::free();
    let mut logic = logic_builder.build();

    let method_name = logic.internal_mem_write(b"callback");
    let args = logic.internal_mem_write(b"arg_data");
    let yield_id = [42u8; 32];
    let yield_id_mem = logic.internal_mem_write(&yield_id);

    let promise_idx = logic
        .promise_yield_create_with_id(
            method_name.len,
            method_name.ptr,
            args.len,
            args.ptr,
            0,
            1,
            yield_id_mem.len,
            yield_id_mem.ptr,
        )
        .expect("yield_create_with_id should succeed");

    assert_eq!(promise_idx, 0);

    drop(logic);

    // Verify a YieldCreate action was logged
    assert!(
        logic_builder.ext.action_log.iter().any(|a| matches!(a, MockAction::YieldCreate { .. })),
        "expected YieldCreate in action log"
    );
}

#[test]
fn test_promise_yield_create_with_id_invalid_yield_id_length() {
    if !ProtocolFeature::YieldWithId.enabled(PROTOCOL_VERSION) {
        return;
    }

    let mut logic_builder = VMLogicBuilder::free();
    let mut logic = logic_builder.build();

    let method_name = logic.internal_mem_write(b"callback");
    let args = logic.internal_mem_write(b"args");
    // Only 16 bytes instead of 32
    let bad_yield_id = [0u8; 16];
    let yield_id_mem = logic.internal_mem_write(&bad_yield_id);

    let result = logic.promise_yield_create_with_id(
        method_name.len,
        method_name.ptr,
        args.len,
        args.ptr,
        0,
        1,
        yield_id_mem.len,
        yield_id_mem.ptr,
    );

    assert!(
        matches!(result, Err(crate::logic::VMLogicError::HostError(HostError::DataIdMalformed))),
        "expected DataIdMalformed for short yield_id, got {result:?}"
    );
}

#[test]
fn test_promise_yield_create_with_id_empty_method_name() {
    if !ProtocolFeature::YieldWithId.enabled(PROTOCOL_VERSION) {
        return;
    }

    let mut logic_builder = VMLogicBuilder::free();
    let mut logic = logic_builder.build();

    let method_name = logic.internal_mem_write(b"");
    let args = logic.internal_mem_write(b"args");
    let yield_id = [1u8; 32];
    let yield_id_mem = logic.internal_mem_write(&yield_id);

    let result = logic.promise_yield_create_with_id(
        method_name.len,
        method_name.ptr,
        args.len,
        args.ptr,
        0,
        1,
        yield_id_mem.len,
        yield_id_mem.ptr,
    );

    assert!(
        matches!(result, Err(crate::logic::VMLogicError::HostError(HostError::EmptyMethodName))),
        "expected EmptyMethodName error, got {result:?}"
    );
}

#[test]
fn test_promise_yield_create_with_id_view_prohibited() {
    if !ProtocolFeature::YieldWithId.enabled(PROTOCOL_VERSION) {
        return;
    }

    let mut logic_builder = VMLogicBuilder::view();
    logic_builder.config.make_free();
    let mut logic = logic_builder.build();

    let method_name = logic.internal_mem_write(b"callback");
    let args = logic.internal_mem_write(b"args");
    let yield_id = [1u8; 32];
    let yield_id_mem = logic.internal_mem_write(&yield_id);

    let result = logic.promise_yield_create_with_id(
        method_name.len,
        method_name.ptr,
        args.len,
        args.ptr,
        0,
        1,
        yield_id_mem.len,
        yield_id_mem.ptr,
    );

    assert!(
        matches!(
            result,
            Err(crate::logic::VMLogicError::HostError(HostError::ProhibitedInView { .. }))
        ),
        "expected ProhibitedInView error, got {result:?}"
    );
}

#[test]
fn test_promise_yield_resume_with_yield_id_after_create_with_id() {
    if !ProtocolFeature::YieldWithId.enabled(PROTOCOL_VERSION) {
        return;
    }

    let mut logic_builder = VMLogicBuilder::free();
    let mut logic = logic_builder.build();

    let method_name = logic.internal_mem_write(b"callback");
    let args = logic.internal_mem_write(b"args");
    let yield_id = [9u8; 32];
    let yield_id_mem = logic.internal_mem_write(&yield_id);

    logic
        .promise_yield_create_with_id(
            method_name.len,
            method_name.ptr,
            args.len,
            args.ptr,
            0,
            1,
            yield_id_mem.len,
            yield_id_mem.ptr,
        )
        .expect("yield_create_with_id should succeed");

    // Resume using the yield_id (not data_id)
    let yield_id_for_resume = logic.internal_mem_write(&yield_id);
    let payload = logic.internal_mem_write(b"resumed_via_yield_id");

    let result = logic
        .promise_yield_resume_with_yield_id(
            yield_id_for_resume.len,
            yield_id_for_resume.ptr,
            payload.len,
            payload.ptr,
        )
        .expect("yield_resume_with_id should succeed");

    assert_eq!(result, 1u32, "resume_with_id should succeed with valid yield_id");
}

#[test]
fn test_promise_yield_resume_with_yield_id_unknown_yield_id() {
    if !ProtocolFeature::YieldWithId.enabled(PROTOCOL_VERSION) {
        return;
    }

    let mut logic_builder = VMLogicBuilder::free();
    let mut logic = logic_builder.build();

    // Try to resume with a yield_id that was never created
    let unknown_yield_id = [3u8; 32];
    let yield_id_mem = logic.internal_mem_write(&unknown_yield_id);
    let payload = logic.internal_mem_write(b"payload");

    let result = logic
        .promise_yield_resume_with_yield_id(
            yield_id_mem.len,
            yield_id_mem.ptr,
            payload.len,
            payload.ptr,
        )
        .expect("yield_resume_with_id should succeed (returning false)");

    assert_eq!(result, 0u32, "resume_with_id with unknown yield_id should return 0");
}

#[test]
fn test_promise_yield_resume_with_yield_id_malformed_yield_id() {
    if !ProtocolFeature::YieldWithId.enabled(PROTOCOL_VERSION) {
        return;
    }

    let mut logic_builder = VMLogicBuilder::free();
    let mut logic = logic_builder.build();

    // Pass a yield_id that's not 32 bytes
    let bad_yield_id = [0u8; 16];
    let yield_id_mem = logic.internal_mem_write(&bad_yield_id);
    let payload = logic.internal_mem_write(b"payload");

    let result = logic.promise_yield_resume_with_yield_id(
        yield_id_mem.len,
        yield_id_mem.ptr,
        payload.len,
        payload.ptr,
    );

    assert!(
        matches!(result, Err(crate::logic::VMLogicError::HostError(HostError::DataIdMalformed))),
        "expected DataIdMalformed error, got {result:?}"
    );
}

#[test]
fn test_promise_yield_create_with_id_then_resume_with_yield_id_fails() {
    if !ProtocolFeature::YieldWithId.enabled(PROTOCOL_VERSION) {
        return;
    }

    let mut logic_builder = VMLogicBuilder::free();
    let mut logic = logic_builder.build();

    // Create a yield with create_with_id
    let method_name = logic.internal_mem_write(b"callback");
    let args = logic.internal_mem_write(b"args");
    let yield_id = [42u8; 32];
    let yield_id_mem = logic.internal_mem_write(&yield_id);

    logic
        .promise_yield_create_with_id(
            method_name.len,
            method_name.ptr,
            args.len,
            args.ptr,
            0,
            1,
            yield_id_mem.len,
            yield_id_mem.ptr,
        )
        .expect("yield_create_with_id should succeed");

    // Try to resume with the yield_id passed as data_id — should fail (return 0)
    // because the yield_id is not the same as the runtime-generated data_id.
    let yield_id_as_data_id = logic.internal_mem_write(&yield_id);
    let payload = logic.internal_mem_write(b"payload");

    let result = logic
        .promise_yield_resume(
            yield_id_as_data_id.len,
            yield_id_as_data_id.ptr,
            payload.len,
            payload.ptr,
        )
        .expect("yield_resume should succeed (returning false)");

    assert_eq!(
        result, 0u32,
        "resume with yield_id passed as data_id should return 0, got {result}"
    );
}

#[test]
fn test_promise_yield_create_then_resume_with_yield_id_fails() {
    if !ProtocolFeature::YieldWithId.enabled(PROTOCOL_VERSION) {
        return;
    }

    let mut logic_builder = VMLogicBuilder::free();
    let mut logic = logic_builder.build();

    // Create a yield with the original yield_create (no yield_id mapping)
    let method_name = logic.internal_mem_write(b"callback");
    let args = logic.internal_mem_write(b"args");
    logic
        .promise_yield_create(method_name.len, method_name.ptr, args.len, args.ptr, 0, 1, 0)
        .expect("yield_create should succeed");

    // Read the data_id from register 0
    let data_id_len = logic.registers().get_len(0).unwrap();
    let ptr = 1024u64;
    logic.read_register(0, ptr).unwrap();
    let data_id_bytes = logic.internal_mem_read(ptr, data_id_len);

    // Try to resume_with_id using the data_id as a yield_id — should fail since no
    // yield_id mapping was created.
    let data_id_as_yield_id = logic.internal_mem_write(&data_id_bytes);
    let payload = logic.internal_mem_write(b"payload");

    let result = logic
        .promise_yield_resume_with_yield_id(
            data_id_as_yield_id.len,
            data_id_as_yield_id.ptr,
            payload.len,
            payload.ptr,
        )
        .expect("yield_resume_with_id should succeed (returning false)");

    assert_eq!(
        result, 0u32,
        "resume_with_id with data_id passed as yield_id should return 0, got {result}"
    );
}

#[test]
fn test_promise_yield_create_with_id_duplicate_in_same_call() {
    if !ProtocolFeature::YieldWithId.enabled(PROTOCOL_VERSION) {
        return;
    }

    let mut logic_builder = VMLogicBuilder::free();
    let mut logic = logic_builder.build();

    let method_name = logic.internal_mem_write(b"callback");
    let args = logic.internal_mem_write(b"args");
    let yield_id = [5u8; 32];
    let yield_id_mem = logic.internal_mem_write(&yield_id);

    // First call should succeed
    let first_idx = logic
        .promise_yield_create_with_id(
            method_name.len,
            method_name.ptr,
            args.len,
            args.ptr,
            0,
            1,
            yield_id_mem.len,
            yield_id_mem.ptr,
        )
        .expect("first yield_create_with_id should succeed");
    assert_ne!(first_idx, u64::MAX, "first call should return a real promise index");

    // Second call with the same yield_id should return u64::MAX without aborting.
    let yield_id_mem2 = logic.internal_mem_write(&yield_id);
    let result = logic
        .promise_yield_create_with_id(
            method_name.len,
            method_name.ptr,
            args.len,
            args.ptr,
            0,
            1,
            yield_id_mem2.len,
            yield_id_mem2.ptr,
        )
        .expect("second yield_create_with_id should succeed (returning sentinel)");
    assert_eq!(result, u64::MAX, "duplicate yield_id should return u64::MAX");
}

#[test]
fn test_promise_yield_resume_with_yield_id_view_prohibited() {
    if !ProtocolFeature::YieldWithId.enabled(PROTOCOL_VERSION) {
        return;
    }

    let mut logic_builder = VMLogicBuilder::view();
    logic_builder.config.make_free();
    let mut logic = logic_builder.build();

    let yield_id = [1u8; 32];
    let yield_id_mem = logic.internal_mem_write(&yield_id);
    let payload = logic.internal_mem_write(b"payload");

    let result = logic.promise_yield_resume_with_yield_id(
        yield_id_mem.len,
        yield_id_mem.ptr,
        payload.len,
        payload.ptr,
    );

    assert!(
        matches!(
            result,
            Err(crate::logic::VMLogicError::HostError(HostError::ProhibitedInView { .. }))
        ),
        "expected ProhibitedInView error, got {result:?}"
    );
}
