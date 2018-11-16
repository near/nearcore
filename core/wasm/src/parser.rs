// Copyright 2015-2018 Parity Technologies (UK) Ltd.
// This file is part of Parity.

// Parity is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

// Parity is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.

// You should have received a copy of the GNU General Public License
// along with Parity.  If not, see <http://www.gnu.org/licenses/>.

//! ActionParams parser for wasm

use wasm_utils::{self, rules};
use parity_wasm::elements;
use parity_wasm::elements::Deserialize;
use parity_wasm::peek_size;
use ext::WasmCosts;
use types::*;

fn gas_rules(wasm_costs: &WasmCosts) -> rules::Set {
	rules::Set::new(
		wasm_costs.regular,
		{
			let mut vals = ::std::collections::BTreeMap::new();
			vals.insert(rules::InstructionType::Load, rules::Metering::Fixed(wasm_costs.mem as u32));
			vals.insert(rules::InstructionType::Store, rules::Metering::Fixed(wasm_costs.mem as u32));
			vals.insert(rules::InstructionType::Div, rules::Metering::Fixed(wasm_costs.div as u32));
			vals.insert(rules::InstructionType::Mul, rules::Metering::Fixed(wasm_costs.mul as u32));
			vals
		})
		.with_grow_cost(wasm_costs.grow_mem)
		.with_forbidden_floats()
}

/// Splits payload to code and data according to params.params_type, also
/// loads the module instance from payload and injects gas counter according
/// to schedule.
pub fn payload<'a>(params: &'a ExecutionParams, wasm_costs: &WasmCosts)
	-> Result<(elements::Module, &'a [u8]), String>
{
	let code = match params.code {
		Some(ref code) => &code[..],
		None => { return Err("Invalid wasm call".to_owned()); }
	};

    let deserialized_module = elements::deserialize_buffer(code).map_err(|err| {
			format!("Error deserializing contract code ({:?})", err)
		})?;
    
    /*
	if deserialized_module.memory_section().map_or(false, |ms| ms.entries().len() > 0) {
		// According to WebAssembly spec, internal memory is hidden from embedder and should not
		// be interacted with. So we disable this kind of modules at decoding level.
		return Err(format!("Malformed wasm module: internal memory"));
	}
    */

	let contract_module = wasm_utils::inject_gas_counter(
		deserialized_module,
		&gas_rules(wasm_costs),
	).map_err(|_| format!("Wasm contract error: bytecode invalid"))?;

	let contract_module = wasm_utils::stack_height::inject_limiter(
		contract_module,
		wasm_costs.max_stack_height,
	).map_err(|_| format!("Wasm contract error: stack limiter failure"))?;

	let data = match params.params_type {
		ParamsType::Embedded => {
			if data_position < code.len() { &code[data_position..] } else { &[] }
		},
		ParamsType::Separate => {
			match params.data {
				Some(ref s) => &s[..],
				None => &[]
			}
		}
	};

	Ok((contract_module, data))
}
