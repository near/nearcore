#![no_std]

use digest::dev::{digest_test, variable_test};
use digest::new_test;

new_test!(blake2b_fixed, "blake2b/fixed", near_blake2::Blake2b, digest_test);
new_test!(blake2b_variable, "blake2b/variable", near_blake2::VarBlake2b, variable_test);
new_test!(blake2s_variable, "blake2s/variable", near_blake2::VarBlake2s, variable_test);
