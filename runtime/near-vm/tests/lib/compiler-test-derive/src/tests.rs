use super::*;
use pretty_assertions::assert_eq;

macro_rules! gen_tests {(
    $(
        $test_name:ident:
        stringify! {
            #[$function:ident $(($($attrs:tt)*))?]
            $($input:tt)*
        } == $output:expr;
    )*
) => (
    $(
        #[test]
        fn $test_name()
        {
            let input: TokenStream =
                stringify!($($input)*)
                    .parse()
                    .expect("Syntax error in test");
            let output: TokenStream =
                $output
                    .parse()
                    .expect("Syntax error in test");
            let attrs: TokenStream =
                stringify!($($($attrs)*)?)
                    .parse()
                    .expect("Syntax error in test");
            let ret = $function(attrs, input).to_string();
            eprintln!("{}", ret);
            assert_eq!(ret, output.to_string());
        }
    )*
)}

gen_tests! {
    identity_for_no_unsafe:
    stringify! {
        #[compiler_test(derive_test)]
        #[cold]
        fn foo(config: crate::Config) {
            // Do tests
        }
    } == stringify! {
        #[cfg(test)]
        mod foo {
            use super::*;

            #[allow(unused)]
            fn foo(config: crate::Config) {
                // Do tests
            }

            #[cfg(feature = "singlepass")]
            mod singlepass {
                use super::*;
                #[test_log::test]
                #[cold]
                #[cfg(feature = "universal")]
                fn universal() {
                    foo(crate::Config::new(
                        crate::Engine::Universal,
                        crate::Compiler::Singlepass
                    ))
                }
                #[test_log::test]
                #[cold]
                #[cfg(feature = "dylib")]
                fn dylib() {
                    foo(crate::Config::new(
                        crate::Engine::Dylib,
                        crate::Compiler::Singlepass
                    ))
                }
            }
        }
    };
}
