#[test]
fn test_run_pass_conversions_extern() {
    run_pass("isle_examples/pass/conversions_extern.isle");
}
#[test]
fn test_run_pass_bound_var() {
    run_pass("isle_examples/pass/bound_var.isle");
}
#[test]
fn test_run_pass_nodebug() {
    run_pass("isle_examples/pass/nodebug.isle");
}
#[test]
fn test_run_pass_construct_and_extract() {
    run_pass("isle_examples/pass/construct_and_extract.isle");
}
#[test]
fn test_run_pass_conversions() {
    run_pass("isle_examples/pass/conversions.isle");
}
#[test]
fn test_run_pass_tutorial() {
    run_pass("isle_examples/pass/tutorial.isle");
}
#[test]
fn test_run_pass_test3() {
    run_pass("isle_examples/pass/test3.isle");
}
#[test]
fn test_run_pass_test2() {
    run_pass("isle_examples/pass/test2.isle");
}
#[test]
fn test_run_pass_prio_trie_bug() {
    run_pass("isle_examples/pass/prio_trie_bug.isle");
}
#[test]
fn test_run_pass_let() {
    run_pass("isle_examples/pass/let.isle");
}
#[test]
fn test_run_pass_test4() {
    run_pass("isle_examples/pass/test4.isle");
}
#[test]
fn test_run_fail_impure_rhs() {
    run_fail("isle_examples/fail/impure_rhs.isle");
}
#[test]
fn test_run_fail_bound_var_type_mismatch() {
    run_fail("isle_examples/fail/bound_var_type_mismatch.isle");
}
#[test]
fn test_run_fail_error1() {
    run_fail("isle_examples/fail/error1.isle");
}
#[test]
fn test_run_fail_multi_internal_etor() {
    run_fail("isle_examples/fail/multi_internal_etor.isle");
}
#[test]
fn test_run_fail_bad_converters() {
    run_fail("isle_examples/fail/bad_converters.isle");
}
#[test]
fn test_run_fail_converter_extractor_constructor() {
    run_fail("isle_examples/fail/converter_extractor_constructor.isle");
}
#[test]
fn test_run_fail_impure_expression() {
    run_fail("isle_examples/fail/impure_expression.isle");
}
#[test]
fn test_run_fail_extra_parens() {
    run_fail("isle_examples/fail/extra_parens.isle");
}
#[test]
fn test_run_link_test() {
    run_link("isle_examples/link/test.isle");
}
#[test]
fn test_run_link_iflets() {
    run_link("isle_examples/link/iflets.isle");
}
#[test]
fn test_run_link_multi_constructor() {
    run_link("isle_examples/link/multi_constructor.isle");
}
#[test]
fn test_run_link_borrows() {
    run_link("isle_examples/link/borrows.isle");
}
#[test]
fn test_run_link_multi_extractor() {
    run_link("isle_examples/link/multi_extractor.isle");
}
#[test]
fn test_run_run_iconst() {
    run_run("isle_examples/run/iconst.isle");
}
#[test]
fn test_run_run_let_shadowing() {
    run_run("isle_examples/run/let_shadowing.isle");
}
