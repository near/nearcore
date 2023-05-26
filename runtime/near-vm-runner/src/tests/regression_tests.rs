use crate::tests::test_builder::test_builder;
use expect_test::expect;

#[test]
fn memory_size_alignment_issue() {
    test_builder()
        .wat(
            r#"
              (module
                (type (;0;) (func))
                (func (;0;) (type 0)
                  memory.size
                  drop
                )
                (memory (;0;) 1 1024)
                (export "foo" (func 0))
              )
            "#,
        )
        .method("foo")
        .protocol_features(&[
            #[cfg(feature = "nightly")]
            near_primitives::version::ProtocolFeature::PreparationV2,
        ])
        .expects(&[
            expect![[r#"
                VMOutcome: balance 4 storage_usage 12 return data None burnt gas 46411725 used gas 46411725
            "#]],
            #[cfg(feature = "nightly")]
            expect![[r#"
                VMOutcome: balance 4 storage_usage 12 return data None burnt gas 52993773 used gas 52993773
            "#]],
        ]);
}

#[test]
fn finite_wasm_gas_was_being_traced_and_thus_slow() {
    test_builder()
        .wat(
            r#"(module
              (type (func))
              (func (type 0)
                loop (result i64)
                  loop
                    br 0
                  end
                  i64.const 0
                end
                drop)
              (export "foo" (func 0)))
            "#,
        )
        .method("foo")
        .expects(&[
            expect![[r#"
              VMOutcome: balance 4 storage_usage 12 return data None burnt gas 100000000000000 used gas 100000000000000
              Err: Exceeded the prepaid gas.
            "#]],
        ]);
}
