mod alt_bn128;
mod context;
#[cfg(feature = "protocol_feature_ed25519_verify")]
mod ed25519_verify;
mod fixtures;
mod gas_counter;
pub(crate) mod helpers;
mod iterators;
mod logs;
mod miscs;
mod promises;
mod registers;
mod storage_read_write;
mod storage_usage;
mod view_method;
mod vm_logic_builder;

use vm_logic_builder::TestVMLogic;
