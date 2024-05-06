use crate::config::Config;
use near_jsonrpc::RpcConfig;
use near_network::config_json::{ExperimentalConfig, NetworkConfigOverrides};
use near_o11y::testonly::init_test_logger;
use near_store::StoreConfig;
use serde::ser::{
    SerializeMap, SerializeSeq, SerializeStruct, SerializeStructVariant, SerializeTuple,
    SerializeTupleStruct, SerializeTupleVariant,
};
use serde::{Serialize, Serializer};

/// Tests that we serialize all near_async::time::Duration as
/// std::time::Duration in the config. This is because they used to be
/// serialized this way and we should be consistent, and also because
/// std::time::Duration has a better serialized format.
///
/// Why don't we just use std::time::Duration in the config structs?
/// That's because most of the codebase uses near_async::time::Duration
/// so we would then have to sprinkle conversion code all over the place.
#[test]
fn test_config_duration_all_std() {
    init_test_logger();
    // Construct a config with no Option::None's. That way, every part
    // of the serialization is checked. It's still not perfect, I suppose,
    // because there are Vec's. So it's best-effort.
    let config = Config {
        chunk_distribution_network: Some(Default::default()),
        store: StoreConfig { path: Some(Default::default()), ..Default::default() },
        cold_store: Some(StoreConfig { path: Some(Default::default()), ..Default::default() }),
        enable_multiline_logging: Some(Default::default()),
        expected_shutdown: Some(Default::default()),
        genesis_records_file: Some(Default::default()),
        max_gas_burnt_view: Some(Default::default()),
        produce_chunk_add_transactions_time_limit: Some(Default::default()),
        rpc: Some(RpcConfig {
            experimental_debug_pages_src_path: Some(Default::default()),
            prometheus_addr: Some(Default::default()),
            ..Default::default()
        }),
        rosetta_rpc: Some(Default::default()),
        save_trie_changes: Some(Default::default()),
        split_storage: Some(Default::default()),
        tracked_shard_schedule: Some(Default::default()),
        transaction_pool_size_limit: Some(Default::default()),
        state_sync: Some(Default::default()),
        trie_viewer_state_size_limit: Some(Default::default()),
        network: near_network::config_json::Config {
            experimental: ExperimentalConfig {
                network_config_overrides: NetworkConfigOverrides {
                    accounts_data_broadcast_rate_limit_burst: Some(0),
                    accounts_data_broadcast_rate_limit_qps: Some(0.0),
                    connect_to_reliable_peers_on_startup: Some(true),
                    highest_peer_horizon: Some(0),
                    max_routes_to_store: Some(0),
                    max_send_peers: Some(0),
                    outbound_disabled: Some(true),
                    push_info_period_millis: Some(0),
                    routed_message_ttl: Some(0),
                    routing_table_update_rate_limit_burst: Some(0),
                    routing_table_update_rate_limit_qps: Some(0.0),
                },
                ..Default::default()
            },
            ..Default::default()
        },
        ..Default::default()
    };
    // We use this custom serializer to check if there is a tuple of
    // (i64, i32) being serialized, which most likely means that we
    // are serializing near_async::time::Duration as is.
    let serializer = DurationCheckSerializer::new();
    assert_eq!(config.serialize(serializer), Ok(()));
}

#[derive(Clone)]
struct DurationCheckSerializer {
    current_path: Vec<String>,
}

#[derive(Clone)]
struct DurationCheckTupleSerializer {
    parent: DurationCheckSerializer,
    field_types: Vec<String>,
}

impl DurationCheckSerializer {
    pub fn new() -> Self {
        Self { current_path: Vec::new() }
    }

    pub fn of(&self, inner: &str) -> Self {
        Self {
            current_path: self
                .current_path
                .clone()
                .into_iter()
                .chain([inner.to_string()].into_iter())
                .collect(),
        }
    }

    pub fn tuple(&self) -> DurationCheckTupleSerializer {
        DurationCheckTupleSerializer { parent: self.clone(), field_types: Vec::new() }
    }
}

impl Serializer for DurationCheckSerializer {
    type Ok = ();
    type Error = std::fmt::Error;

    type SerializeSeq = DurationCheckSerializer;
    type SerializeTuple = DurationCheckTupleSerializer;
    type SerializeTupleStruct = DurationCheckSerializer;
    type SerializeTupleVariant = DurationCheckSerializer;
    type SerializeMap = DurationCheckSerializer;
    type SerializeStruct = DurationCheckSerializer;
    type SerializeStructVariant = DurationCheckSerializer;

    fn serialize_bool(self, _v: bool) -> Result<Self::Ok, Self::Error> {
        Ok(())
    }

    fn serialize_i8(self, _v: i8) -> Result<Self::Ok, Self::Error> {
        Ok(())
    }

    fn serialize_i16(self, _v: i16) -> Result<Self::Ok, Self::Error> {
        Ok(())
    }

    fn serialize_i32(self, _v: i32) -> Result<Self::Ok, Self::Error> {
        Ok(())
    }

    fn serialize_i64(self, _v: i64) -> Result<Self::Ok, Self::Error> {
        Ok(())
    }

    fn serialize_u8(self, _v: u8) -> Result<Self::Ok, Self::Error> {
        Ok(())
    }

    fn serialize_u16(self, _v: u16) -> Result<Self::Ok, Self::Error> {
        Ok(())
    }

    fn serialize_u32(self, _v: u32) -> Result<Self::Ok, Self::Error> {
        Ok(())
    }

    fn serialize_u64(self, _v: u64) -> Result<Self::Ok, Self::Error> {
        Ok(())
    }

    fn serialize_f32(self, _v: f32) -> Result<Self::Ok, Self::Error> {
        Ok(())
    }

    fn serialize_f64(self, _v: f64) -> Result<Self::Ok, Self::Error> {
        Ok(())
    }

    fn serialize_char(self, _v: char) -> Result<Self::Ok, Self::Error> {
        Ok(())
    }

    fn serialize_str(self, _v: &str) -> Result<Self::Ok, Self::Error> {
        Ok(())
    }

    fn serialize_bytes(self, _v: &[u8]) -> Result<Self::Ok, Self::Error> {
        Ok(())
    }

    fn serialize_none(self) -> Result<Self::Ok, Self::Error> {
        panic!("The test setup should not leave any None's, otherwise it can miss some fields to check; None seen at field path {:?}", self.current_path);
    }

    fn serialize_some<T>(self, value: &T) -> Result<Self::Ok, Self::Error>
    where
        T: Serialize + ?Sized,
    {
        value.serialize(self)
    }

    fn serialize_unit(self) -> Result<Self::Ok, Self::Error> {
        Ok(())
    }

    fn serialize_unit_struct(self, _name: &'static str) -> Result<Self::Ok, Self::Error> {
        Ok(())
    }

    fn serialize_unit_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
    ) -> Result<Self::Ok, Self::Error> {
        Ok(())
    }

    fn serialize_newtype_struct<T>(
        self,
        name: &'static str,
        value: &T,
    ) -> Result<Self::Ok, Self::Error>
    where
        T: Serialize + ?Sized,
    {
        value.serialize(self.of(name))
    }

    fn serialize_newtype_variant<T>(
        self,
        name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        value: &T,
    ) -> Result<Self::Ok, Self::Error>
    where
        T: Serialize + ?Sized,
    {
        value.serialize(self.of(name))
    }

    fn serialize_seq(self, _len: Option<usize>) -> Result<Self::SerializeSeq, Self::Error> {
        Ok(self)
    }

    fn serialize_tuple(self, _len: usize) -> Result<Self::SerializeTuple, Self::Error> {
        Ok(self.tuple())
    }

    fn serialize_tuple_struct(
        self,
        name: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleStruct, Self::Error> {
        Ok(self.of(name))
    }

    fn serialize_tuple_variant(
        self,
        name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleVariant, Self::Error> {
        Ok(self.of(name))
    }

    fn serialize_map(self, _len: Option<usize>) -> Result<Self::SerializeMap, Self::Error> {
        Ok(self)
    }

    fn serialize_struct(
        self,
        name: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStruct, Self::Error> {
        Ok(self.of(name))
    }

    fn serialize_struct_variant(
        self,
        name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStructVariant, Self::Error> {
        Ok(self.of(name))
    }
}

impl SerializeSeq for DurationCheckSerializer {
    type Ok = ();
    type Error = std::fmt::Error;

    fn serialize_element<T>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: Serialize + ?Sized,
    {
        value.serialize(self.clone())
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(())
    }
}

impl SerializeMap for DurationCheckSerializer {
    type Ok = ();
    type Error = std::fmt::Error;

    fn serialize_entry<K, V>(&mut self, _key: &K, value: &V) -> Result<(), Self::Error>
    where
        K: Serialize + ?Sized,
        V: Serialize + ?Sized,
    {
        value.serialize(self.clone())
    }

    fn serialize_key<T>(&mut self, _key: &T) -> Result<(), Self::Error>
    where
        T: Serialize + ?Sized,
    {
        unreachable!()
    }

    fn serialize_value<T>(&mut self, _value: &T) -> Result<(), Self::Error>
    where
        T: Serialize + ?Sized,
    {
        unreachable!()
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(())
    }
}

impl SerializeStruct for DurationCheckSerializer {
    type Ok = ();
    type Error = std::fmt::Error;

    fn serialize_field<T>(&mut self, key: &'static str, value: &T) -> Result<(), Self::Error>
    where
        T: Serialize + ?Sized,
    {
        value.serialize(self.of(key))
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(())
    }
}

impl SerializeTuple for DurationCheckTupleSerializer {
    type Ok = ();
    type Error = std::fmt::Error;

    fn serialize_element<T>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: Serialize + ?Sized,
    {
        self.field_types.push(std::any::type_name::<T>().to_string());
        value.serialize(self.parent.clone())
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        if self.field_types == vec!["i64".to_string(), "i32".to_string()] {
            panic!(
                "Detected attempted serialization of time::Duration struct in the Config,\
                     at path {:?}, make sure you use annotate the field with \
                     serde(with = \"near_async::time::serde_duration_as_std\")",
                self.parent.current_path
            );
        }
        Ok(())
    }
}

impl SerializeTupleStruct for DurationCheckSerializer {
    type Ok = ();
    type Error = std::fmt::Error;

    fn serialize_field<T>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: Serialize + ?Sized,
    {
        value.serialize(self.clone())
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(())
    }
}

impl SerializeTupleVariant for DurationCheckSerializer {
    type Ok = ();
    type Error = std::fmt::Error;

    fn serialize_field<T>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: Serialize + ?Sized,
    {
        value.serialize(self.clone())
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(())
    }
}

impl SerializeStructVariant for DurationCheckSerializer {
    type Ok = ();
    type Error = std::fmt::Error;

    fn serialize_field<T>(&mut self, key: &'static str, value: &T) -> Result<(), Self::Error>
    where
        T: Serialize + ?Sized,
    {
        value.serialize(self.of(key))
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(())
    }
}
