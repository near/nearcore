use crate::logic::HostError;
use crate::logic::tests::vm_logic_builder::VMLogicBuilder;
use near_primitives_core::types::Balance;

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

// Behavioral end-to-end tests for `promise_yield_create_with_id` and
// `promise_yield_resume_with_yield_id` live in the integration tests at
// integration-tests/src/tests/runtime/test_yield_resume.rs (which exercises a
// RuntimeNode against the real trie + runtime). The tests below only cover
// argument validation that fails before any external interaction.

#[test]
fn test_promise_yield_create_with_id_invalid_yield_id_length() {
    let mut logic_builder = VMLogicBuilder::free();
    let mut logic = logic_builder.build();

    let method_name = logic.internal_mem_write(b"callback");
    let args = logic.internal_mem_write(b"args");
    // Only 16 bytes instead of 32
    let bad_yield_id = [0u8; 16];
    let yield_id_mem = logic.internal_mem_write(&bad_yield_id);

    let amount = logic.internal_mem_write(&[0u8; 16]);
    let result = logic.promise_yield_create_with_id(
        method_name.len,
        method_name.ptr,
        args.len,
        args.ptr,
        amount.ptr,
        0,
        1,
        yield_id_mem.len,
        yield_id_mem.ptr,
    );

    assert!(
        matches!(result, Err(crate::logic::VMLogicError::HostError(HostError::YieldIdMalformed))),
        "expected YieldIdMalformed for short yield_id, got {result:?}"
    );
}

#[test]
fn test_promise_yield_create_with_id_empty_method_name() {
    let mut logic_builder = VMLogicBuilder::free();
    let mut logic = logic_builder.build();

    let method_name = logic.internal_mem_write(b"");
    let args = logic.internal_mem_write(b"args");
    let yield_id = [1u8; 32];
    let yield_id_mem = logic.internal_mem_write(&yield_id);

    let amount = logic.internal_mem_write(&[0u8; 16]);
    let result = logic.promise_yield_create_with_id(
        method_name.len,
        method_name.ptr,
        args.len,
        args.ptr,
        amount.ptr,
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
    let mut logic_builder = VMLogicBuilder::view();
    logic_builder.config.make_free();
    let mut logic = logic_builder.build();

    let method_name = logic.internal_mem_write(b"callback");
    let args = logic.internal_mem_write(b"args");
    let yield_id = [1u8; 32];
    let yield_id_mem = logic.internal_mem_write(&yield_id);

    let amount = logic.internal_mem_write(&[0u8; 16]);
    let result = logic.promise_yield_create_with_id(
        method_name.len,
        method_name.ptr,
        args.len,
        args.ptr,
        amount.ptr,
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
fn test_promise_yield_resume_with_yield_id_malformed_yield_id() {
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
        matches!(result, Err(crate::logic::VMLogicError::HostError(HostError::YieldIdMalformed))),
        "expected YieldIdMalformed error, got {result:?}"
    );
}

#[test]
fn test_promise_yield_resume_with_yield_id_view_prohibited() {
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

// ── Balance accounting tests ─────────────────────────────────────────────────
//
// promise_yield_create_with_id allows attaching a balance to the callback's
// function-call action. The amount must be subtracted from the contract's
// current balance; if balance is insufficient the call should fail with
// BalanceExceeded. As a special case, exactly 1 yoctoNEAR is permitted on a
// zero-balance contract when `one_yocto_on_promise` is enabled (parity with
// promise_batch_action_function_call_weight).

fn yield_create_with_id_attaching(
    logic: &mut crate::logic::tests::vm_logic_builder::TestVMLogic<'_>,
    yield_id: [u8; 32],
    amount: u128,
) -> crate::logic::logic::Result<u64> {
    let method_name = logic.internal_mem_write(b"callback");
    let args = logic.internal_mem_write(b"args");
    let yield_id_mem = logic.internal_mem_write(&yield_id);
    let amount_mem = logic.internal_mem_write(&amount.to_le_bytes());
    logic.promise_yield_create_with_id(
        method_name.len,
        method_name.ptr,
        args.len,
        args.ptr,
        amount_mem.ptr,
        0,
        1,
        yield_id_mem.len,
        yield_id_mem.ptr,
    )
}

#[test]
fn test_promise_yield_create_with_id_deducts_balance() {
    let mut logic_builder = VMLogicBuilder::free();
    logic_builder.context.account_balance = Balance::from_yoctonear(100);
    logic_builder.context.attached_deposit = Balance::ZERO;
    let mut logic = logic_builder.build();

    yield_create_with_id_attaching(&mut logic, [1u8; 32], 25)
        .expect("yield_create_with_id should succeed with sufficient balance");
    assert_eq!(
        logic.result_state().current_account_balance,
        Balance::from_yoctonear(75),
        "balance should be reduced by attached amount"
    );
    assert_eq!(
        logic.result_state().subsidized_amount,
        Balance::ZERO,
        "no subsidy when balance covers the deposit"
    );
}

#[test]
fn test_promise_yield_create_with_id_insufficient_balance() {
    let mut logic_builder = VMLogicBuilder::free();
    logic_builder.context.account_balance = Balance::from_yoctonear(10);
    logic_builder.context.attached_deposit = Balance::ZERO;
    let mut logic = logic_builder.build();

    let err = yield_create_with_id_attaching(&mut logic, [1u8; 32], 100)
        .expect_err("attaching more than the balance must fail");
    assert!(
        matches!(err, crate::logic::VMLogicError::HostError(HostError::BalanceExceeded)),
        "expected BalanceExceeded, got {err:?}"
    );
    // Balance must not be touched on a failed deduction.
    assert_eq!(
        logic.result_state().current_account_balance,
        Balance::from_yoctonear(10),
        "balance must not change on failed deduction"
    );
}

#[test]
fn test_promise_yield_create_with_id_zero_amount_succeeds_on_zero_balance() {
    let mut logic_builder = VMLogicBuilder::free();
    logic_builder.context.account_balance = Balance::ZERO;
    logic_builder.context.attached_deposit = Balance::ZERO;
    let mut logic = logic_builder.build();

    // Attaching 0 is always allowed (no deduction).
    yield_create_with_id_attaching(&mut logic, [1u8; 32], 0)
        .expect("attaching 0 amount should succeed on zero balance");
    assert_eq!(logic.result_state().subsidized_amount, Balance::ZERO);
}

#[test]
fn test_promise_yield_create_with_id_one_yocto_exemption_enabled() {
    let mut logic_builder = VMLogicBuilder::free();
    logic_builder.config.one_yocto_on_promise = true;
    logic_builder.context.account_balance = Balance::ZERO;
    logic_builder.context.attached_deposit = Balance::ZERO;
    let mut logic = logic_builder.build();

    // 1 yoctoNEAR works on zero balance via the subsidy path.
    yield_create_with_id_attaching(&mut logic, [1u8; 32], 1)
        .expect("1 yoctoNEAR should succeed with one_yocto_on_promise enabled");
    assert_eq!(
        logic.result_state().current_account_balance,
        Balance::ZERO,
        "balance must remain zero, exemption bumps subsidized_amount instead"
    );
    assert_eq!(
        logic.result_state().subsidized_amount,
        Balance::from_yoctonear(1),
        "subsidized_amount tracks the skipped deduction"
    );

    // 2 yoctoNEAR is NOT covered by the exemption.
    let err = yield_create_with_id_attaching(&mut logic, [2u8; 32], 2)
        .expect_err("attaching 2 yoctoNEAR on zero balance should fail");
    assert!(
        matches!(err, crate::logic::VMLogicError::HostError(HostError::BalanceExceeded)),
        "expected BalanceExceeded, got {err:?}"
    );
}

#[test]
fn test_promise_yield_create_with_id_one_yocto_exemption_disabled() {
    let mut logic_builder = VMLogicBuilder::free();
    logic_builder.config.one_yocto_on_promise = false;
    logic_builder.context.account_balance = Balance::ZERO;
    logic_builder.context.attached_deposit = Balance::ZERO;
    let mut logic = logic_builder.build();

    // Without the exemption, 1 yoctoNEAR on zero balance must fail.
    let err = yield_create_with_id_attaching(&mut logic, [1u8; 32], 1)
        .expect_err("1 yoctoNEAR without exemption should fail on zero balance");
    assert!(
        matches!(err, crate::logic::VMLogicError::HostError(HostError::BalanceExceeded)),
        "expected BalanceExceeded, got {err:?}"
    );
}

#[test]
fn test_promise_yield_create_with_id_one_yocto_deducts_with_nonzero_balance() {
    let mut logic_builder = VMLogicBuilder::free();
    logic_builder.config.one_yocto_on_promise = true;
    logic_builder.context.account_balance = Balance::from_yoctonear(1);
    logic_builder.context.attached_deposit = Balance::ZERO;
    let mut logic = logic_builder.build();

    // With balance >= 1, the exemption is NOT used — 1 yocto is deducted normally.
    yield_create_with_id_attaching(&mut logic, [1u8; 32], 1)
        .expect("1 yoctoNEAR should succeed and be deducted from non-zero balance");
    assert!(
        logic.result_state().current_account_balance.is_zero(),
        "balance should be zero after 1 yoctoNEAR deduction"
    );
    assert_eq!(
        logic.result_state().subsidized_amount,
        Balance::ZERO,
        "non-zero-balance call is not subsidized"
    );
}
