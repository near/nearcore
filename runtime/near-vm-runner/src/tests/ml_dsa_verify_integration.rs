//! End-to-end integration tests for the `ml_dsa_verify` host function.
//!
//! These build a tiny WASM contract that imports `ml_dsa_verify`, lays the
//! inputs out in linear memory, invokes the host function, and returns its
//! result. They exercise the full pipeline through the real WASM VM (Wasmtime)
//! — imports wiring, memory marshalling, gas accounting, and the feature gate —
//! in a way the logic-only unit tests cannot.
//!
//! ML-DSA signing is hedged (randomized), so the known-answer vector is a fixed
//! valid `(public_key, message, signature)` triple rather than something
//! re-signed at runtime; verification is deterministic.

use crate::ContractCode;
use crate::logic::Config;
use crate::logic::errors::{FunctionCallError, HostError};
use crate::logic::mocks::mock_external::MockedExternal;
use crate::logic::types::ReturnData;
use crate::runner::VMKindExt;
use crate::tests::{create_context, test_vm_config, with_vm_variants};
use near_parameters::RuntimeFeesConfig;
use near_parameters::vm::VMKind;
use std::cell::RefCell;
use std::sync::Arc;

const PUBKEY_HEX: &str = "f619d9a454aa2afc8eef799c8e3f38d93b30d0fc9daed9aa8ed2647c17e219f2c116683ff8e8829087da200ecddfb690ae2d8f31ae28889c3b02bc4117a7f9ad266d3b2f86609b5c9f6d2440a42d49e813535b454cedca06f9fdd3058b58f07475abf571c9d04d446a962733e0eabf3323748a90fded4f79aa7dcfd1f1a651771b6dc9009976ea971e0ebcdc5017b1a085387b94d950f40804bd81f6ad17af060eb7c249f188ef437aa4e7ab6de326bb95099c3279000fb48dd13769a09aeb9bcaf9ce8068956aadb3d99439145c7b9e5aebe3c072e4478fb4ccee15b54802a688cebb48f409bb76d35907c339bdb679876493c45f5fa16bcc541362ed59a25214f1148a79642c77ce2dbf9d33e05b8d1caff2fe3638460661ed36e0a4996e153ef818e5d51f097d6a40b21ecbd5735b7ed6f05661925ecbec6f7c8991e6fd19cf30a1d3119e12ce5644fd47eac594bafb602411e4342ef4dd78a886d937fbe40350e7120c2718fb7fbe2b1c9fb729aa35bf27b415a93100a942a08abc07e241d3788ec36f02020fa01e9bf49517562833836390ee2d66ddb4a4834b542c3b712c15e894ff0fc46181c5aaa5dac662cc382a2b07011eea3c770e51653607bc526dc53604cfde79374a98d664f78b8d510de30d5fcb4bf6efc776ae9e519fc220a9c837ea87e01857eec438b1f3a984f7387774f473ec33146f51a143f6605cc3876eeb8b40342981a9aab011a3f534e80d30c331fc3d5a85a9820a13334b300ee21affbd28556d47c9bae7b0b189ac3afb5b0acf0b3cb541c040f0a65aeb146b411c286171cdeb1f88382325824c6646649d4ff4963bdb926231ecd642126be0e5a0fe4dc75ed4f503c4056c282558a74ecb3148ed44c5b5aac39daa8fabece6b52de68ae1cf19643bda9056e130b87f91a00de78bd17929d4ab1c01cbc2f2db445247b9ab3734a4bbda97087f34dcf8dec5af691cb4e3dd284262ed13c716d4615c388dbd4469b9ce4068982ca88608187ac9a035a21764366ae2f9f06fb805a02225b8c110be59d8ae436204a0e951c001ec60005c87770264a03ba14895ffcde9f0bb86c06b7a03ae55327933856e0866229f84f6ed480e513443e5ab9ccbe3c19f68ec129571c74a28a53ed41e9924801ca6f7dd09abc29bc83ebda99f636f84d0fc61f55b62d4a2da9a2f1b32999c19b4e7977f885fcd36002e66392d6defbfc8dade616740d0ac5901a4550f68e75be17b553111512e5a9a36d0b9713d3c1b3bc313b6da4f900dd947c80a1c2cd5b8dcdb1f458146ad059d2da9144291d0959994c84753be696ec4c5f632edfd27dafc0472d0e6c5cb7c5f4fff2ae522a28f3878f269f5da6909fa466da31252bfcefc634131c1b03111f6b952a7c9b6ea7b3d2fd7eb21bc12691c5e7dbb6ae2ab669c7a72ab0f9b3edbfe1dc6b3347b854c325730e5d42338b72630eea9d43c8d0558d86be1ff1ec4902bd45027e5e5a041a288420ce501cd5773ca1b66c65b471fc3a554fe0b45b1c4399d3e80d535560e01ff6b893508de9e42b53bdf798b8de95f05699f1dcd82960a5beac4914fdd27e5b64df5eaf7801e649e8b0e0e17d20177c35396113e1b63bffd47bac0388e41117127baaa30655e663ccb2643f64fd3bad8725cd20000cde8926fadbff5ea1ba359ce2a852f10cb06d490fa6da85961389c6f54fb685d2ea57e0bc483fae1e398ef9915513834d2dce247af3060e453de1f04504d73d7cd96ee916aeaa0ff44c83a1d941da5970d84b3788c1411e70152014ebde0e79276e78aee6a429be54bb0af10fd0f9ca70e3c130f26de4cc74479712423b5ab2daecc0ef08ce9ed526d31e65adb4fad1bc8f1380505f6587f9e12bc4acc62c4ca11a9ec14dedb3abafc75209f9e64090d0c94e704e6f7271be960122725cbe925f6b417e3634c1e3c5cc596fe5dd6b88ed486b31e1bef2f1077a21399898f874a0f8136c6eff808df1cc379caf3de8000721c94bd9e02c0154a71e25378c3b28a1a7ebb63ca8143ad75e110b33c3c61c5851ea94741ba8353c9003eb32c148cf999164abdcd14e485602ca4c0f938bb69b72e431c8e642fc1d3c91c70f537c10b2d54518e2f5a95dd226e0e7d2df6ce3ef5be6f309a6b67d310f71f861e970a59bd7318745939bd6f9c17993b5e0506250e8688fc40ec0d31bfe8f6b924a33a2ba7167b887f32d7ee9b15154fc02e47c4e59abe69d9b7d0b35393fd815b009d75fd69373c7fdd7a3e8ae6717682dcd9c9ff980f56d57ed32c07b705fa98875cddc4f634218f5c163db15f1da424758a7da60d9532b133775c328de1f030d5972e4bb28a64c5a59f72c77e05f49d035a9219ba120fb123f7ae25a4ac1dfdc01b5c6e76db4f1ac53d8bab35144ecba0e8a925989f103b9c461fe3905a7b8d8f64d2de1834f39086a5de74854e97b5a2b8a07bf76fb4e7547fa799ab6420b68f5767cc4fc0a3868e53befd0d063afa2b49d06944dfa24c1fd9931feefc3d94d3fd65c324d048997146942f9545368ad6b56f3bebd8152610c42c27967dee9441ef0a3e63add60121f4002189ee233ac458a3d90bfa87b7bf7ec55ab855bb04b33d363a655537c7abb94994a6849733949405bf9ca02fdd6aad8a0d901b6fab166b18e8f3d10e8c997e3a0638cc261fa2bac1e663bc1e6d66a8998d1419b589f25de6c51f8305b8de918140897a7f06e686ea1a72464db6056e6a404b7e651b1de7d3a55a598844f65b";
const SIG_HEX: &str = "91956895828553984ff1435b356ea219f56b203f6a3e39bc44a07c06edc85a2868c1b641da4310d5dd6cf29f41aeda916f59578eadddcc86a41fe2d0ccff2cc51d073171033414cc9335c830d237f0574e7ea3f02fa2d3339fd6d4986db917fb41c636ed1d174d0ee4092cf9c839cf0804035a12dbd17d30d2c764ed12db9a6bcf0cc8566d5908e4bccc184656df615ad12d1333618d740a3965eafc1598c4c33db076c52a2819c214d5f0b09191d261d2fca874aff022b0ada1d4aaa6049f20d14165c596e3230769b50b1ad8c0be1e78412add9b07cde8c2381bcb6f5992a30c0ce8948705400796e75d5688dee3478524bf54f5f241f9636e2979a9791fab229d5062c6be707aa8be60f508c0a13255bff11dedf0f9c38f874d96f3f86d027fa313c46b3fadb14ad78147d877287239732a3786aa80d85c0dc18f5a04af5b3d4d51f2a563492d11c39d665faff4a8b57190dc45e966690774b792b8c1c8529d8ba5fd24895a5196d08de36e5e1756732c2d9ed06a77efe57bb2df00971e2949d32f80f2027bf82e4ec3d925b0c1af742274bde92115682d276b252f699b71283a108f9c86bd4f38b2a2ee820aba9cc896bdf2d3c6e3129f1c12a1d13223a70c2a5976b81285d3624e1d40f2bb77baf0cf4ae9c6d1319b98869c30feddca60d6ec6a7432703d2983048c861da2406a95b7dbc5ba893c95eae9a371e938889009cde5495a9319193925db11dc589e28f22414830ad33c306b35e822e564c0f6b233ff4adc8529c45d20b08c1f0fbc2601392347faba898acfdb896bfd7e2799891af7f1ae5a8df8ce8126d20392f3ff2dc1587dbe2472a3dd24eb03535be8048c45485dd14835e532d4c01795bb1adf05700acf21a0e98d0113050541a20b941ce5196ae403237051486bac23b802d2e66aab39008982ce7f569c24ecb6a153ddcb9b2ec2068e1f0d3af62572f70732c222d324d744251f640154710e9de01c3acc1c3ff8a558a3ef50f3c8f687cf50b82b3ba58c0d35c136cc24eb750cce13f092f56d517f142355fe7aad673b8b1c53aa983ba08bc71f879f906b5b72214c7ec21d776aaaaf8c6c091e3afd1e668a25d216c39cc8753f0a858d445b402701e0d74b41af272780b993457aa13a7314e55e011bff0177293d6d3beae0cf485cd2e75fd3b6c6aa5162c2ed523199981bd65e3f6f733a7ab4c205d45155be69ff982f793d78101d4ea27500a132a1ab643f7aab0819c899ca3d3c72c1ca4ee58322c84fb93a3a8003b283c02252ae0b75b0d52543d9fbeb2b93118dcb31dc4a42b55bf69ff9c3c5c14d886fbed4b2d19b5538a99d0e84fdc756aaddb83ec926af1d0030523399a7844716dd83cf8dc6eea73e14734f021f593ba70c2dd15901bf7485f7c3dd8c966d6cbfab7de2b6109f61238d54f5e9b7e60cd928e9c7c326d41d132622375656339255629e84656d3cd1ad553032d831fb61343bc722f0bf2d84060ebfc542e261e4a274125fc6278778d89afc69f662fb8f9410ccc60e61e6ee93c65e73458f08f8f88bf7350a4f968a5da10937c11a4179211216901f69f885f46c23a7683fa5efc3226ade42a3fc10fb8b2b48aa67d2048e1c7cf9d946e2cc3d04e4d99ac29e480619dacb25a4e5ae29bde0febcd1f2d5532496c1e828d842f1a91ea2e0b39cd2f826cdf207738f3b02aa6b213a1ea7b2482298d499ea85ef6cdbb474dc00b78f0fad50926e433a316ed5a9e18db7e52c463f34ae0cfcb66a86813c15b0aa864e3edf9b5cbaaf8af9d32dd768c95de4608197c5282268ce4156139348b3a4a99d25ef39097fe7bb3d1e434c25a0c18672b151903672a907e43670174442e1f86fd5ebfeb6354d4ea10a7711b999a81f52afefc56e99a1c4d344391454f88a47a3218423eb98bf9b1873c31abfeb234f5b3ab727ff5a9800b52693ef503f8642873cd9b17ffdb36f6d906a0c476cc327ca2362a94bb38fded9c953775779c5ad5572d7239ea0256c682b2813b6986e330e7e5d9c167cbe879792c0074c977b9268bb553fc466e0fe22e4d57a23e12c09a180d9f05f73cc7842d3348a75fdc02d674f4432741dfc762169b278beb4d78b0c14f8647ecce22143d40053166fee5307623fe51196852f71174605a192ce8d67d5c1e45af3b56d8aae86fd5c9db81985e3065927b2b32838ae9cec71720bff471b9f4e39e9e4b925e260e67ec459c4d023a975c3f91b9c2d9f9719bd3e22f72e781cae101edd5b48d2e587a77b41ee6463d241059abc5bea7b261ca547640328e1be508f096d693581fa544b9f98c6859ffc05876d32be94f58eee2590ed659f8863ec588e67b4511e20f482fd579ee1077cbbecad3252bca694417dcf426392ddf565a9292c4a6db55da73a9fb69b64a6f0fe2f8f18faf7bbe3cb7713e74c65353e8bb973f03571ff767bb712538130613d77bd6f746f7e13e50b8a7c9a42fea4f5b73e3522a60c11d42d3906c2b750f0768be3aa82d5f1e4783b36e0d795f71d16be826c6478e46af86aa9fdb3726325d2e9ff7ebfdc9fe0b84590263887f91b21dabb3de20713aad5b149f9d90fef95c665a39a8f6d91b2729a62856766612554b133f426e127ca7f561499730942edfdf6d8b4ba784ca7790fbc460556db7ecd93073410b508811fd5db1997540a108717d0a6096d1cc049493051b0f5c0fa8057120605ea1c247230877bad8525b829f01fa9d87533371a850065fb0d016647646ea9ba8cf2c1eae253d50340371b7cba1e8fd13e70e5cfc1fab272cb067e7e72757134e06ab8b3a108636ffb489c5fbfb9d556eae36568dd2bc426fbe4f2a4741bef8ce97f5d5c0f5ce3c0bce2490f6dfa01149b7bf396a5acc9f483130ac77f85296bdf2aa11ddf9baab7b6ad7d54365330c75278b13d208f0639580b8061275d4df809f083a749ff46815fc56ed0a7be818503e3a17a3f61ffc0b27c1c16eb42421fee111c1b969c3240331342a7216075c1f2f565f9678ab93925472f0bb78c556bc9af5791f786077aa1d7dbb1e79c930e1b0b986f5094d91e05ad9c6075c145876120f859e02aa4f4502dd29b2584c9f187092f7c6ac71736307577cf6b5663f96feb0e5604c14d5372f77e2dd679ffb1f7af52f6a118d59b9a643bce8b94c6597672dba3b672d30ab165d8c052d1a48a9773a57394b98a46e14390e71617e8aec5cdf0213d19b7eaf9c9c89d15c198a32c491b132bddedc8dc258a68e4d84c7201755649853f15165850e6cadc04e57591a4166e5c48e3bed4d945f255b643297cd1114c074312976813c8965b833fda2d6c8a63e67c07292fea4cc63089cf463f29cb223189045d09848000bd49ca09754cdf6ccb0fe2c64b7bdd1eeb121c2acc4c698cc4399598b77afba475e714aee52e83cc07f65ac4ff15e36ee4aacd0dab1c3c5244433c328406b426919d5b1ecfb136b22c506c2a2bfdc015a2ab7596d9b9a30e19a5bd1dd4f5d59e550aebd2ac2f78d51666d66bcbaeeacad1edee9237376c4e2531a6c9ca14408523b3fcb195ac8d5d7975ee49a64faccfd4f1d16f6bf4bdfc9107e4dbe833aea808b8117080d65239ca47212bc7c9d2c3b66d2ae6cba0a1cce0ec4ecaadbb93e39fbfb90a13ca4a34c283a0a0b7cc211a0a6ac48897fee0e3888253c40b9ba322a198cd95f4ba8281f559b601b3d4d8e13c603737aa3f68c9afdaecbc898de410b72af3f1f2fb6c5a72f0562f24fa7284fb30320c64f16c822aa557f722ad3720f75a82ae140cb86bea097a10c6f420cec5ee19223dbba52a349c5c231748ec4db8770d3f82a81b4a6222de495b996d91229deaac146b0ade99a76b97ef1018cd823c0b8431787b5771c5e8eaad0a709b82846051b6246e1e3b59097fb4e37b30d61f68d3110dc09e9671526665922ddc071da310ecd6b1c2b92ae0c53df525d4f8f9d22deeb27ae5c5f9a9f9896aee4f8730862e66ad5447df2fdb171cb27f1d23869ac82ee7f291ea430cf1278dae1dbe7b0030a7ddffb4267ba772889c8edfab05497471503618b5a5b362335e65723fc499f0d089ca28e5f20cca2a2471133fafe063b0bccf47f57b178b5d266e6f52334b2b593834006bfccb125373aab8aedc780d68e28cb33dab6c059ef2452d0d91a79deec0c886746c18264c5770265a20afb8afe1d5b2c7379a983faff181322946a0e3dbb63ed647390a0f34a815b117ec9cbe61e5bc3aa57d7eb8e211199c298f4be359fb5fdde29efb6dd12829455d6cc3031011c09e0ca58bd5563516f6145510a19366240d46d6e4d6d85dc8b6226f4a367a66706694eb9d12c766251304aacebeab440aa84603977cdabf4025165b2f5d723345ff11c9a8ae92bbe17ab02b3f524ccfcd879d603d9311596125fda50d011a67aa1d4c86560a3ad9973bc9bf7ed78560b5e256fcc6f90a96a250fcc555fc40d3e393588eebb4d818407e134fb7e73fc55e08b9637132e918a33b45428e86aa2b278a891f767b7618e3a6528e3a6e8103fbccaa7afa906da5cf230b720864a193b77eab36ec3223e894b8b0855257679c6efab88e071307cdd34c762d1ba80a424a80c2f116172c92a9cdf00a1b38cd030f136f82a8d9e8eb0c181e5f7a95c31d2e435b8698b3b7ec00000000000000000000000000060d111a212a";
const MSG_HEX: &str = "0707070707070707070707070707070707070707070707070707070707070707";

fn kat() -> (Vec<u8>, Vec<u8>, Vec<u8>) {
    let signature = hex::decode(SIG_HEX).expect("bad sig hex");
    let message = hex::decode(MSG_HEX).expect("bad msg hex");
    let public_key = hex::decode(PUBKEY_HEX).expect("bad pk hex");
    (signature, message, public_key)
}

/// Build a WAT module that invokes `ml_dsa_verify` with three inputs laid out
/// in linear memory:
///   signature:  offset 0,    `sig_len` bytes
///   message:    offset 4096, `message.len()` bytes
///   public_key: offset 8192, `pk_len` bytes
///
/// `sig_len_arg` / `pk_len_arg` control the lengths passed to the host function
/// so we can drive both the valid and the length-mismatch paths. The `u64`
/// result is returned via `value_return`.
fn verify_wat(
    signature: &[u8],
    message: &[u8],
    public_key: &[u8],
    sig_len_arg: u64,
    pk_len_arg: u64,
) -> String {
    fn bytes_to_data(bytes: &[u8]) -> String {
        let mut s = String::new();
        for byte in bytes {
            s.push_str(&format!("\\{:02x}", byte));
        }
        s
    }

    let sig_data = bytes_to_data(signature);
    let msg_data = bytes_to_data(message);
    let pk_data = bytes_to_data(public_key);
    let msg_len = message.len();

    format!(
        r#"(module
  (import "env" "ml_dsa_verify"
    (func $ml_dsa_verify (param i64 i64 i64 i64 i64 i64) (result i64)))
  (import "env" "value_return" (func $value_return (param i64 i64)))
  (memory (export "memory") 1)

  (data (i32.const 0) "{sig_data}")
  (data (i32.const 4096) "{msg_data}")
  (data (i32.const 8192) "{pk_data}")

  (func (export "main")
    (local $result i64)
    (local.set $result
      (call $ml_dsa_verify
        (i64.const {sig_len_arg})
        (i64.const 0)
        (i64.const {msg_len})
        (i64.const 4096)
        (i64.const {pk_len_arg})
        (i64.const 8192))
    )
    ;; Store the u64 result at offset 12288 and return its 8 bytes.
    (i64.store (i32.const 12288) (local.get $result))
    (call $value_return (i64.const 8) (i64.const 12288))
  )
)"#,
    )
}

enum ExpectedOutcome<'a> {
    /// Execution completes without abort and returns the given u64.
    Value(u64),
    /// Execution aborts with `HostError::MlDsaVerifyInvalidInput` whose Display
    /// contains this substring.
    HostErrorContains(&'a str),
    /// The contract fails to link because the host function is not exported
    /// (feature gate off).
    LinkError,
}

/// Compile and run a WAT contract under every available VM with the ML-DSA
/// feature flag set to `enabled`, asserting the outcome matches `expected`.
fn run_wat(wat: &str, enabled: bool, expected: ExpectedOutcome) {
    let ran = RefCell::new(false);
    with_vm_variants(|vm_kind: VMKind| {
        let mut config = test_vm_config(Some(vm_kind));
        config.ml_dsa_verify_host_fn = enabled;
        let config = Arc::new(config);
        let fees = Arc::new(RuntimeFeesConfig::test());
        let wasm = wat::parse_str(wat).expect("failed to parse wat");
        let code = ContractCode::new(wasm, None);
        let mut fake_external = MockedExternal::with_code(code);
        let context = create_context(vec![]);
        let gas_counter = context.make_gas_counter(&config);
        let runtime = vm_kind.runtime(Arc::<Config>::clone(&config)).expect("no runtime");
        let outcome = runtime
            .prepare(&fake_external, None, gas_counter, "main")
            .run(&mut fake_external, &context, Arc::clone(&fees))
            .expect("execution failed");

        match &expected {
            ExpectedOutcome::Value(want) => {
                assert!(
                    outcome.aborted.is_none(),
                    "contract aborted under {:?}: {:?}",
                    vm_kind,
                    outcome.aborted
                );
                let value = match &outcome.return_data {
                    ReturnData::Value(v) => v.clone(),
                    other => panic!("unexpected return data for {:?}: {:?}", vm_kind, other),
                };
                assert_eq!(value.len(), 8, "expected 8-byte u64 return from {:?}", vm_kind);
                let mut arr = [0u8; 8];
                arr.copy_from_slice(&value);
                assert_eq!(
                    u64::from_le_bytes(arr),
                    *want,
                    "unexpected return value from {:?}",
                    vm_kind
                );
            }
            ExpectedOutcome::HostErrorContains(substr) => {
                let aborted = outcome
                    .aborted
                    .as_ref()
                    .unwrap_or_else(|| panic!("expected abort from {:?}, got none", vm_kind));
                match aborted {
                    FunctionCallError::HostError(HostError::MlDsaVerifyInvalidInput { msg }) => {
                        assert!(
                            msg.contains(substr),
                            "expected MlDsaVerifyInvalidInput msg to contain {:?}, got {:?}",
                            substr,
                            msg,
                        );
                    }
                    other => panic!(
                        "expected MlDsaVerifyInvalidInput error under {:?}, got {:?}",
                        vm_kind, other
                    ),
                }
            }
            ExpectedOutcome::LinkError => {
                let aborted = outcome
                    .aborted
                    .as_ref()
                    .unwrap_or_else(|| panic!("expected link error from {:?}, got none", vm_kind));
                assert!(
                    matches!(aborted, FunctionCallError::LinkError { .. }),
                    "expected LinkError under {:?}, got {:?}",
                    vm_kind,
                    aborted
                );
            }
        }

        *ran.borrow_mut() = true;
    });
    assert!(*ran.borrow(), "no VM variants executed this test");
}

#[test]
fn test_ml_dsa_verify_integration_valid_signature() {
    let (signature, message, public_key) = kat();
    let wat = verify_wat(&signature, &message, &public_key, 3309, 1952);
    run_wat(&wat, true, ExpectedOutcome::Value(1));
}

#[test]
fn test_ml_dsa_verify_integration_tampered_message() {
    let (signature, message, public_key) = kat();
    let mut tampered = message;
    tampered[0] ^= 0x01;
    let wat = verify_wat(&signature, &tampered, &public_key, 3309, 1952);
    run_wat(&wat, true, ExpectedOutcome::Value(0));
}

#[test]
fn test_ml_dsa_verify_integration_wrong_public_key() {
    // A well-formed but unrelated public key: flipping a ρ byte yields a
    // different valid key, so verification returns 0.
    let (signature, message, mut public_key) = kat();
    public_key[0] ^= 0x01;
    let wat = verify_wat(&signature, &message, &public_key, 3309, 1952);
    run_wat(&wat, true, ExpectedOutcome::Value(0));
}

#[test]
fn test_ml_dsa_verify_integration_invalid_signature_length_aborts() {
    // sig_len != 3309 must raise MlDsaVerifyInvalidInput, aborting the VM.
    let (signature, message, public_key) = kat();
    let wat = verify_wat(&signature, &message, &public_key, 3308, 1952);
    run_wat(&wat, true, ExpectedOutcome::HostErrorContains("invalid signature length"));
}

#[test]
fn test_ml_dsa_verify_integration_invalid_public_key_length_aborts() {
    let (signature, message, public_key) = kat();
    let wat = verify_wat(&signature, &message, &public_key, 3309, 1951);
    run_wat(&wat, true, ExpectedOutcome::HostErrorContains("invalid public key length"));
}

#[test]
fn test_ml_dsa_verify_integration_feature_gate_off_fails_to_link() {
    // With the feature disabled, `env.ml_dsa_verify` is not exported and the
    // contract fails to link.
    let (signature, message, public_key) = kat();
    let wat = verify_wat(&signature, &message, &public_key, 3309, 1952);
    run_wat(&wat, false, ExpectedOutcome::LinkError);
}
