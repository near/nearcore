// Copyright 2017-2018 Parity Technologies (UK) Ltd.
// This file is part of Substrate.

// Substrate is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

// Substrate is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.

// You should have received a copy of the GNU General Public License
// along with Substrate.  If not, see <http://www.gnu.org/licenses/>.

//! Substrate chain configurations.

use std::collections::HashMap;
use std::fs::File;
use std::path::PathBuf;
use primitives::storage::{StorageKey, StorageData};
use runtime_primitives::{BuildStorage, StorageMap};
use serde_json as json;
use components::RuntimeGenesis;

enum GenesisSource<G> {
	File(PathBuf),
	Embedded(&'static [u8]),
	Factory(fn() -> G),
}

impl<G: RuntimeGenesis> Clone for GenesisSource<G> {
	fn clone(&self) -> Self {
		match *self {
			GenesisSource::File(ref path) => GenesisSource::File(path.clone()),
			GenesisSource::Embedded(d) => GenesisSource::Embedded(d),
			GenesisSource::Factory(f) => GenesisSource::Factory(f),
		}
	}
}

impl<G: RuntimeGenesis> GenesisSource<G> {
	fn resolve(&self) -> Result<Genesis<G>, String> {
		#[derive(Serialize, Deserialize)]
		struct GenesisContainer<G> {
			genesis: Genesis<G>,
		}

		match *self {
			GenesisSource::File(ref path) => {
				let file = File::open(path).map_err(|e| format!("Error opening spec file: {}", e))?;
				let genesis: GenesisContainer<G> = json::from_reader(file).map_err(|e| format!("Error parsing spec file: {}", e))?;
				Ok(genesis.genesis)
			},
			GenesisSource::Embedded(buf) => {
				let genesis: GenesisContainer<G> = json::from_reader(buf).map_err(|e| format!("Error parsing embedded file: {}", e))?;
				Ok(genesis.genesis)
			},
			GenesisSource::Factory(f) => Ok(Genesis::Runtime(f())),
		}
	}
}

impl<'a, G: RuntimeGenesis> BuildStorage for &'a ChainSpec<G> {
	fn build_storage(self) -> Result<StorageMap, String> {
		match self.genesis.resolve()? {
			Genesis::Runtime(gc) => gc.build_storage(),
			Genesis::Raw(map) => Ok(map.into_iter().map(|(k, v)| (k.0, v.0)).collect()),
		}
	}
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[serde(deny_unknown_fields)]
enum Genesis<G> {
	Runtime(G),
	Raw(HashMap<StorageKey, StorageData>),
}

#[derive(Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
struct ChainSpecFile {
	pub name: String,
	pub id: String,
	pub boot_nodes: Vec<String>,
	pub telemetry_url: Option<String>,
	pub protocol_id: Option<String>,
	pub consensus_engine: Option<String>,
}

/// A configuration of a chain. Can be used to build a genesis block.
pub struct ChainSpec<G: RuntimeGenesis> {
	spec: ChainSpecFile,
	genesis: GenesisSource<G>,
}

impl<G: RuntimeGenesis> Clone for ChainSpec<G> {
	fn clone(&self) -> Self {
		ChainSpec {
			spec: self.spec.clone(),
			genesis: self.genesis.clone(),
		}
	}
}

impl<G: RuntimeGenesis> ChainSpec<G> {
	pub fn boot_nodes(&self) -> &[String] {
		&self.spec.boot_nodes
	}

	pub fn name(&self) -> &str {
		&self.spec.name
	}

	pub fn id(&self) -> &str {
		&self.spec.id
	}

	pub fn telemetry_url(&self) -> Option<&str> {
		self.spec.telemetry_url.as_ref().map(String::as_str)
	}

	pub fn protocol_id(&self) -> Option<&str> {
		self.spec.protocol_id.as_ref().map(String::as_str)
	}

	pub fn consensus_engine(&self) -> Option<&str> {
		self.spec.consensus_engine.as_ref().map(String::as_str)
	}

	/// Parse json content into a `ChainSpec`
	pub fn from_embedded(json: &'static [u8]) -> Result<Self, String> {
		let spec = json::from_slice(json).map_err(|e| format!("Error parsing spec file: {}", e))?;
		Ok(ChainSpec {
			spec,
			genesis: GenesisSource::Embedded(json),
		})
	}

	/// Parse json file into a `ChainSpec`
	pub fn from_json_file(path: PathBuf) -> Result<Self, String> {
		let file = File::open(&path).map_err(|e| format!("Error opening spec file: {}", e))?;
		let spec = json::from_reader(file).map_err(|e| format!("Error parsing spec file: {}", e))?;
		Ok(ChainSpec {
			spec,
			genesis: GenesisSource::File(path),
		})
	}

	/// Create hardcoded spec.
	pub fn from_genesis(
		name: &str,
		id: &str,
		constructor: fn() -> G,
		boot_nodes: Vec<String>,
		telemetry_url: Option<&str>,
		protocol_id: Option<&str>,
		consensus_engine: Option<&str>,
	) -> Self
	{
		let spec = ChainSpecFile {
			name: name.to_owned(),
			id: id.to_owned(),
			boot_nodes: boot_nodes,
			telemetry_url: telemetry_url.map(str::to_owned),
			protocol_id: protocol_id.map(str::to_owned),
			consensus_engine: consensus_engine.map(str::to_owned),
		};
		ChainSpec {
			spec,
			genesis: GenesisSource::Factory(constructor),
		}
	}

	/// Dump to json string.
	pub fn to_json(self, raw: bool) -> Result<String, String> {
		#[derive(Serialize, Deserialize)]
		struct Container<G> {
			#[serde(flatten)]
			spec: ChainSpecFile,
			genesis: Genesis<G>,

		};
		let genesis = match (raw, self.genesis.resolve()?) {
			(true, Genesis::Runtime(g)) => {
				let storage = g.build_storage()?.into_iter()
					.map(|(k, v)| (StorageKey(k), StorageData(v)))
					.collect();

				Genesis::Raw(storage)
			},
			(_, genesis) => genesis,
		};
		let spec = Container {
			spec: self.spec,
			genesis,
		};
		json::to_string_pretty(&spec).map_err(|e| format!("Error generating spec json: {}", e))
	}
}
