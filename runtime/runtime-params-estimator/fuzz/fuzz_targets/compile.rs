#![no_main]
use libfuzzer_sys::fuzz_target;
use runtime_params_estimator::{compile_module, VMKind};


fuzz_target!(|data: &[u8]| {
  let data = Vec::from(data);
  compile_module(VMKind::default(), &data);
    // println!("{:?}", VMKind::default());
  //   panic!("oob");
  // }
  // match VMKind::{}
  
});
