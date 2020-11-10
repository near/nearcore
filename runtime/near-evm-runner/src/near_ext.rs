use std::convert::TryInto;
use std::sync::Arc;

use ethereum_types::{Address, H256, U256};
use evm::ActionParams;
use keccak_hash::keccak;
use near_runtime_fees::EvmCostConfig;
use parity_bytes::Bytes;
use vm::{
    CallType, ContractCreateResult, CreateContractAddress, EnvInfo, Error as VmError,
    MessageCallResult, Result as EvmResult, ReturnData, Schedule, TrapKind,
};

use crate::evm_state::{EvmState, SubState};
use crate::interpreter;

use crate::utils::format_log;

// https://github.com/openethereum/openethereum/blob/77643c13e80ca09d9a6b10631034f5a1568ba6d3/ethcore/machine/src/externalities.rs
pub struct NearExt<'a> {
    pub info: EnvInfo,
    pub origin: Address,
    pub schedule: Schedule,
    pub context_addr: Address,
    pub selfdestruct_address: Option<Address>,
    pub sub_state: &'a mut SubState<'a>,
    pub static_flag: bool,
    pub depth: usize,
    pub evm_gas_config: &'a EvmCostConfig,
    pub chain_id: u128,
}

impl std::fmt::Debug for NearExt<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "\nNearExt {{")?;
        write!(f, "\n\tinfo: {:?}", self.info)?;
        write!(f, "\n\torigin: {:?}", self.origin)?;
        write!(f, "\n\tcontext_addr: {:?}", self.context_addr)?;
        write!(f, "\n\tstatic_flag: {:?}", self.static_flag)?;
        write!(f, "\n\tdepth: {:?}", self.depth)?;
        write!(f, "\n\tchain_id: {:?}", self.chain_id)?;
        write!(f, "\n}}")
    }
}

impl<'a> NearExt<'a> {
    pub fn new(
        context_addr: Address,
        origin: Address,
        sub_state: &'a mut SubState<'a>,
        depth: usize,
        static_flag: bool,
        evm_gas_config: &'a EvmCostConfig,
        chain_id: u128,
    ) -> Self {
        Self {
            info: Default::default(),
            origin,
            schedule: Default::default(),
            context_addr,
            selfdestruct_address: Default::default(),
            sub_state,
            static_flag,
            depth,
            evm_gas_config,
            chain_id,
        }
    }
}

impl<'a> vm::Ext for NearExt<'a> {
    /// EIP-1344: Returns the current chain's EIP-155 unique identifier.
    /// See https://github.com/ethereum/EIPs/blob/master/EIPS/eip-1344.md
    fn chain_id(&self) -> u64 {
      self.chain_id.try_into().unwrap()
    }

    /// Returns the storage value for a given key if reversion happens on the current transaction.
    fn initial_storage_at(&self, key: &H256) -> EvmResult<H256> {
        let raw_val = self
            .sub_state
            .parent // Read from the unmodified parent state
            .read_contract_storage(&self.context_addr, key.0)
            .unwrap_or(None)
            .unwrap_or([0u8; 32]); // default to an empty value
        Ok(H256(raw_val))
    }

    /// Returns a value for given key.
    fn storage_at(&self, key: &H256) -> EvmResult<H256> {
        let raw_val = self
            .sub_state
            .read_contract_storage(&self.context_addr, key.0)
            .unwrap_or(None)
            .unwrap_or([0u8; 32]); // default to an empty value
        Ok(H256(raw_val))
    }

    /// Stores a value for given key.
    fn set_storage(&mut self, key: H256, value: H256) -> EvmResult<()> {
        if self.is_static() {
            return Err(VmError::MutableCallInStaticContext);
        }
        self.sub_state
            .set_contract_storage(&self.context_addr, key.0, value.0)
            .map_err(|err| vm::Error::Internal(err.to_string()))
    }

    // TODO: research why these are different
    fn exists(&self, address: &Address) -> EvmResult<bool> {
        Ok(self.sub_state.balance_of(address).unwrap_or_else(|_| U256::zero()) > U256::zero()
            || self.sub_state.code_at(address).unwrap_or(None).is_some())
    }

    fn exists_and_not_null(&self, address: &Address) -> EvmResult<bool> {
        Ok(self.sub_state.balance_of(address).unwrap_or_else(|_| U256::zero()) > 0.into()
            || self.sub_state.code_at(address).unwrap_or(None).is_some())
    }

    fn origin_balance(&self) -> EvmResult<U256> {
        self.balance(&self.origin)
    }

    fn balance(&self, address: &Address) -> EvmResult<U256> {
        Ok(self
            .sub_state
            .get_account(address)
            .unwrap_or(None)
            .map(|account| account.balance.into())
            .unwrap_or(U256::zero()))
    }

    fn blockhash(&mut self, number: &U256) -> H256 {
        // TODO(3456): Return actual block hashes.
        let mut buf = [0u8; 32];
        number.to_big_endian(&mut buf);
        keccak(&buf[..])
    }

    fn create(
        &mut self,
        gas: &U256,
        value: &U256,
        code: &[u8],
        address_type: CreateContractAddress,
        _trap: bool,
    ) -> Result<ContractCreateResult, TrapKind> {
        if self.is_static() {
            return Err(TrapKind::Call(ActionParams::default()));
        }

        // TODO: better error propagation.
        interpreter::deploy_code(
            self.sub_state,
            &self.origin,
            &self.context_addr,
            *value,
            self.depth,
            address_type,
            true,
            &code.to_vec(),
            gas,
            &self.evm_gas_config,
            self.chain_id,
        )
        .map_err(|_| TrapKind::Call(ActionParams::default()))
    }

    /// Message call.
    ///
    /// Returns Err, if we run out of gas.
    /// Otherwise returns call_result which contains gas left
    /// and true if subcall was successful.
    fn call(
        &mut self,
        gas: &U256,
        sender_address: &Address,
        receive_address: &Address,
        value: Option<U256>,
        data: &[u8],
        code_address: &Address,
        call_type: CallType,
        _trap: bool,
    ) -> Result<MessageCallResult, TrapKind> {
        if self.is_static() && call_type != CallType::StaticCall {
            panic!("MutableCallInStaticContext")
        }

        // hijack builtins
        if crate::builtins::is_precompile(receive_address) {
            return Ok(crate::builtins::process_precompile(
                receive_address,
                data,
                gas,
                &self.evm_gas_config,
            ));
        }

        let result = match call_type {
            CallType::None => {
                // Is not used.
                return Err(TrapKind::Call(ActionParams::default()));
            }
            CallType::Call => interpreter::call(
                self.sub_state,
                &self.origin,
                sender_address,
                value,
                self.depth,
                receive_address,
                &data.to_vec(),
                true, // should_commit
                gas,
                &self.evm_gas_config,
                self.chain_id,
            ),
            CallType::StaticCall => interpreter::static_call(
                self.sub_state,
                &self.origin,
                sender_address,
                self.depth,
                receive_address,
                &data.to_vec(),
                gas,
                &self.evm_gas_config,
                self.chain_id,
            ),
            CallType::CallCode => {
                // Call another contract using storage of the current contract. No longer used.
                return Err(TrapKind::Call(ActionParams::default()));
            }
            CallType::DelegateCall => interpreter::delegate_call(
                self.sub_state,
                &self.origin,
                sender_address,
                self.depth,
                receive_address,
                code_address,
                &data.to_vec(),
                gas,
                &self.evm_gas_config,
                self.chain_id,
            ),
        };
        result.map_err(|_| TrapKind::Call(ActionParams::default()))
    }

    /// Returns code at given address
    fn extcode(&self, address: &Address) -> EvmResult<Option<Arc<Bytes>>> {
        let code = self.sub_state.code_at(address).unwrap_or(None).map(Arc::new);
        Ok(code)
    }

    /// Returns code hash at given address
    fn extcodehash(&self, address: &Address) -> EvmResult<Option<H256>> {
        let code_opt = self.sub_state.code_at(address).unwrap_or(None);
        let code = match code_opt {
            Some(code) => code,
            None => return Ok(None),
        };
        if code.is_empty() {
            Ok(None)
        } else {
            Ok(Some(keccak(code)))
        }
    }

    /// Returns code size at given address
    fn extcodesize(&self, address: &Address) -> EvmResult<Option<usize>> {
        Ok(self.sub_state.code_at(address).unwrap_or(None).map(|c| c.len()))
    }

    /// Creates log entry with given topics and data
    fn log(&mut self, topics: Vec<H256>, data: &[u8]) -> EvmResult<()> {
        if self.is_static() {
            return Err(VmError::MutableCallInStaticContext);
        }
        if topics.len() > 256 {
            return Err(VmError::Internal("Too many topics".to_string()));
        }
        self.sub_state.state.logs.push(hex::encode(
            format_log(topics, data)
                .map_err(|err| VmError::Internal(format!("Failed to format event: {}", err)))?,
        ));
        Ok(())
    }

    /// Should be called when transaction calls `RETURN` opcode.
    /// Returns gas_left if cost of returning the data is not too high.
    fn ret(self, _gas: &U256, _data: &ReturnData, _apply_state: bool) -> EvmResult<U256> {
        // NOTE: this is only called through finalize(), but we are not using it
        // so it should be safe to ignore it here.
        Err(vm::Error::Internal("ret".to_string()))
    }

    /// Should be called when contract commits suicide.
    /// Address to which funds should be refunded.
    /// Deletes code, moves balance
    fn suicide(&mut self, refund_address: &Address) -> EvmResult<()> {
        self.sub_state
            .self_destruct(&self.context_addr)
            .map_err(|err| vm::Error::Internal(err.to_string()))?;

        let account =
            self.sub_state.get_account(&self.context_addr).unwrap_or(None).unwrap_or_default();
        self.sub_state
            .add_balance(refund_address, account.balance.into())
            .map_err(|err| vm::Error::Internal(err.to_string()))?;
        self.sub_state
            .sub_balance(&self.context_addr, account.balance.into())
            .map_err(|err| vm::Error::Internal(err.to_string()))?;
        Ok(())
    }

    /// Returns schedule.
    fn schedule(&self) -> &Schedule {
        &self.schedule
    }

    /// Returns environment info.
    fn env_info(&self) -> &EnvInfo {
        &self.info
    }

    /// Returns current depth of execution.
    ///
    /// If contract A calls contract B, and contract B calls C,
    /// then A depth is 0, B is 1, C is 2 and so on.
    fn depth(&self) -> usize {
        self.depth
    }

    /// Increments sstore refunds counter.
    fn add_sstore_refund(&mut self, _value: usize) {}

    /// Decrements sstore refunds counter.
    /// Left as NOP as evm gas is not metered
    fn sub_sstore_refund(&mut self, _value: usize) {}

    /// Decide if any more operations should be traced. Passthrough for the VM trace.
    fn trace_next_instruction(&mut self, _pc: usize, _instruction: u8, _current_gas: U256) -> bool {
        false
    }

    /// Prepare to trace an operation. Passthrough for the VM trace.
    fn trace_prepare_execute(
        &mut self,
        _pc: usize,
        _instruction: u8,
        _gas_cost: U256,
        _mem_written: Option<(usize, usize)>,
        _store_written: Option<(U256, U256)>,
    ) {
    }

    /// Trace the finalised execution of a single instruction.
    fn trace_executed(&mut self, _gas_used: U256, _stack_push: &[U256], _mem: &[u8]) {}

    /// Check if running in static context.
    fn is_static(&self) -> bool {
        self.static_flag
    }
}
