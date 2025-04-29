/// This file implements a completely new serialization format which
/// transforms anything that implements serde::Serialize into an
/// EntityDataValue (i.e. a generic tree of string key-value pairs).
///
/// This is used for the Entity Debug UI. The reason why we don't use
/// JSON serialization is because we would have to maintain an equivalent
/// schema on the UI side to be able to effectively render it, which is not
/// only a lot of work but also will get easily out of sync with any Rust
/// changes. (Frameworks to generate TypeScript typings from Rust structs
/// do exist, but they all have serious limitations at the moment.) Also,
/// it is discouraging if in order to support a new query one has to also
/// write TypeScript type definitions.
use near_jsonrpc_primitives::types::entity_debug::{
    EntityDataEntry, EntityDataStruct, EntityDataValue,
};
use serde::{
    Serialize, Serializer,
    ser::{
        SerializeMap, SerializeSeq, SerializeStruct, SerializeStructVariant, SerializeTuple,
        SerializeTupleStruct, SerializeTupleVariant,
    },
};

/// Root data for serialization output.
struct EntitySerializerData {
    pub output: EntityDataStruct,
}

/// Serializer for values.
struct EntitySerializer<'a> {
    // Upon self.commit(value), the value will be written to the key at this
    // parent.
    parent: &'a mut EntityDataStruct,
    key: String,
}

/// Serializer for structs, maps, tuples, arrays, etc.
struct EntitySerializerStruct<'a> {
    // Upon self.commit(), the children will be wrapped as an EntityDataValue
    // and then committed to the parent.
    parent: EntitySerializer<'a>,
    children: EntityDataStruct,
    // The next index, for arrays.
    index: usize,
    // If present, when committing, the value will be additionally wrapped in
    // another struct with a single entry with this key.
    outer_key: Option<String>,
}

impl EntitySerializerData {
    pub fn new() -> EntitySerializerData {
        EntitySerializerData { output: EntityDataStruct::new() }
    }

    pub fn serializer<'a>(&'a mut self, key: String) -> EntitySerializer<'a> {
        EntitySerializer { parent: &mut self.output, key }
    }
}

impl<'a> EntitySerializer<'a> {
    /// Creates a child struct serializer that when committed will commit a
    /// struct value to self.
    pub fn child_struct(self, outer_key: Option<String>) -> EntitySerializerStruct<'a> {
        EntitySerializerStruct {
            parent: self,
            children: EntityDataStruct::new(),
            index: 0,
            outer_key,
        }
    }

    pub fn commit(self, value: EntityDataValue) {
        self.parent.entries.push(EntityDataEntry { name: self.key, value });
    }
}

impl<'a> EntitySerializerStruct<'a> {
    /// Prepares to serialize a child array element.
    /// The element is added to children when the returned serializer commits.
    pub fn push<'b>(&'b mut self) -> EntitySerializer<'b>
    where
        'a: 'b,
    {
        let result =
            EntitySerializer { parent: &mut self.children, key: format!("{}", self.index) };
        self.index += 1;
        result
    }

    /// Prepares to serialize a child struct field.
    /// The element is added to children when the returned serializer commits.
    pub fn child<'b>(&'b mut self, key: String) -> EntitySerializer<'b>
    where
        'a: 'b,
    {
        EntitySerializer { parent: &mut self.children, key }
    }

    pub fn commit(self) {
        let value = EntityDataValue::Struct(self.children.into());
        match self.outer_key {
            Some(outer_key) => {
                self.parent.commit(EntityDataValue::Struct(Box::new(EntityDataStruct {
                    entries: vec![EntityDataEntry { name: outer_key, value }],
                })))
            }
            None => self.parent.commit(value),
        }
    }
}

impl<'a> Serializer for EntitySerializer<'a> {
    type Ok = ();

    type Error = std::fmt::Error; // can be anything, we don't error

    type SerializeSeq = EntitySerializerStruct<'a>;
    type SerializeTuple = EntitySerializerStruct<'a>;
    type SerializeTupleStruct = EntitySerializerStruct<'a>;
    type SerializeTupleVariant = EntitySerializerStruct<'a>;
    type SerializeMap = EntitySerializerStruct<'a>;
    type SerializeStruct = EntitySerializerStruct<'a>;
    type SerializeStructVariant = EntitySerializerStruct<'a>;

    fn serialize_bool(self, v: bool) -> Result<Self::Ok, Self::Error> {
        self.commit(EntityDataValue::String(format!("{}", v)));
        Ok(())
    }

    fn serialize_i8(self, v: i8) -> Result<Self::Ok, Self::Error> {
        self.commit(EntityDataValue::String(format!("{}", v)));
        Ok(())
    }

    fn serialize_i16(self, v: i16) -> Result<Self::Ok, Self::Error> {
        self.commit(EntityDataValue::String(format!("{}", v)));
        Ok(())
    }

    fn serialize_i32(self, v: i32) -> Result<Self::Ok, Self::Error> {
        self.commit(EntityDataValue::String(format!("{}", v)));
        Ok(())
    }

    fn serialize_i64(self, v: i64) -> Result<Self::Ok, Self::Error> {
        self.commit(EntityDataValue::String(format!("{}", v)));
        Ok(())
    }

    fn serialize_i128(self, v: i128) -> Result<Self::Ok, Self::Error> {
        self.commit(EntityDataValue::String(format!("{}", v)));
        Ok(())
    }

    fn serialize_u8(self, v: u8) -> Result<Self::Ok, Self::Error> {
        self.commit(EntityDataValue::String(format!("{}", v)));
        Ok(())
    }

    fn serialize_u16(self, v: u16) -> Result<Self::Ok, Self::Error> {
        self.commit(EntityDataValue::String(format!("{}", v)));
        Ok(())
    }

    fn serialize_u32(self, v: u32) -> Result<Self::Ok, Self::Error> {
        self.commit(EntityDataValue::String(format!("{}", v)));
        Ok(())
    }

    fn serialize_u64(self, v: u64) -> Result<Self::Ok, Self::Error> {
        self.commit(EntityDataValue::String(format!("{}", v)));
        Ok(())
    }

    fn serialize_u128(self, v: u128) -> Result<Self::Ok, Self::Error> {
        self.commit(EntityDataValue::String(format!("{}", v)));
        Ok(())
    }

    fn serialize_f32(self, v: f32) -> Result<Self::Ok, Self::Error> {
        self.commit(EntityDataValue::String(format!("{}", v)));
        Ok(())
    }

    fn serialize_f64(self, v: f64) -> Result<Self::Ok, Self::Error> {
        self.commit(EntityDataValue::String(format!("{}", v)));
        Ok(())
    }

    fn serialize_char(self, v: char) -> Result<Self::Ok, Self::Error> {
        self.commit(EntityDataValue::String(format!("{}", v)));
        Ok(())
    }

    fn serialize_str(self, v: &str) -> Result<Self::Ok, Self::Error> {
        self.commit(EntityDataValue::String(format!("{}", v)));
        Ok(())
    }

    fn serialize_bytes(self, v: &[u8]) -> Result<Self::Ok, Self::Error> {
        self.commit(EntityDataValue::String(hex::encode(v)));
        Ok(())
    }

    fn serialize_none(self) -> Result<Self::Ok, Self::Error> {
        self.commit(EntityDataValue::String("null".to_owned()));
        Ok(())
    }

    fn serialize_some<T>(self, value: &T) -> Result<Self::Ok, Self::Error>
    where
        T: Serialize + ?Sized,
    {
        value.serialize(self).unwrap();
        Ok(())
    }

    fn serialize_unit(self) -> Result<Self::Ok, Self::Error> {
        self.commit(EntityDataValue::String("()".to_owned()));
        Ok(())
    }

    fn serialize_unit_struct(self, _name: &'static str) -> Result<Self::Ok, Self::Error> {
        self.serialize_unit()
    }

    fn serialize_unit_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        variant: &'static str,
    ) -> Result<Self::Ok, Self::Error> {
        self.serialize_str(variant).unwrap();
        Ok(())
    }

    fn serialize_newtype_struct<T>(
        self,
        _name: &'static str,
        value: &T,
    ) -> Result<Self::Ok, Self::Error>
    where
        T: Serialize + ?Sized,
    {
        value.serialize(self).unwrap();
        Ok(())
    }

    fn serialize_newtype_variant<T>(
        self,
        _name: &'static str,
        _variant_index: u32,
        variant: &'static str,
        value: &T,
    ) -> Result<Self::Ok, Self::Error>
    where
        T: Serialize + ?Sized,
    {
        let mut child = self.child_struct(None);
        value.serialize(child.child(variant.to_owned())).unwrap();
        child.commit();
        Ok(())
    }

    fn serialize_seq(self, _len: Option<usize>) -> Result<Self::SerializeSeq, Self::Error> {
        Ok(self.child_struct(None))
    }

    fn serialize_tuple(self, _len: usize) -> Result<Self::SerializeTuple, Self::Error> {
        Ok(self.child_struct(None))
    }

    fn serialize_tuple_struct(
        self,
        _name: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleStruct, Self::Error> {
        Ok(self.child_struct(None))
    }

    fn serialize_tuple_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleVariant, Self::Error> {
        Ok(self.child_struct(Some(variant.to_owned())))
    }

    fn serialize_map(self, _len: Option<usize>) -> Result<Self::SerializeMap, Self::Error> {
        Ok(self.child_struct(None))
    }

    fn serialize_struct(
        self,
        _name: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStruct, Self::Error> {
        Ok(self.child_struct(None))
    }

    fn serialize_struct_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStructVariant, Self::Error> {
        Ok(self.child_struct(Some(variant.to_owned())))
    }
}

impl<'a> SerializeSeq for EntitySerializerStruct<'a> {
    type Ok = ();
    type Error = std::fmt::Error;

    fn serialize_element<T>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: Serialize + ?Sized,
    {
        value.serialize(self.push()).unwrap();
        Ok(())
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        self.commit();
        Ok(())
    }
}

impl<'a> SerializeMap for EntitySerializerStruct<'a> {
    type Ok = ();

    type Error = std::fmt::Error;

    fn serialize_entry<K, V>(&mut self, key: &K, value: &V) -> Result<(), Self::Error>
    where
        K: Serialize + ?Sized,
        V: Serialize + ?Sized,
    {
        let mut key_data = EntitySerializerData::new();
        key.serialize(key_data.serializer("".to_owned())).unwrap();
        let key = match key_data.output.entries.into_iter().next().unwrap().value {
            EntityDataValue::String(key_str) => key_str,
            EntityDataValue::Struct(_) => "invalid_key_type".to_owned(),
        };
        value.serialize(self.child(key)).unwrap();
        Ok(())
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        self.commit();
        Ok(())
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
}

impl<'a> SerializeStruct for EntitySerializerStruct<'a> {
    type Ok = ();

    type Error = std::fmt::Error;

    fn serialize_field<T>(&mut self, key: &'static str, value: &T) -> Result<(), Self::Error>
    where
        T: Serialize + ?Sized,
    {
        value.serialize(self.child(key.to_owned())).unwrap();
        Ok(())
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        self.commit();
        Ok(())
    }
}

impl<'a> SerializeTuple for EntitySerializerStruct<'a> {
    type Ok = ();

    type Error = std::fmt::Error;

    fn serialize_element<T>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: Serialize + ?Sized,
    {
        value.serialize(self.push()).unwrap();
        Ok(())
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        self.commit();
        Ok(())
    }
}

impl<'a> SerializeTupleStruct for EntitySerializerStruct<'a> {
    type Ok = ();

    type Error = std::fmt::Error;

    fn serialize_field<T>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: Serialize + ?Sized,
    {
        value.serialize(self.push()).unwrap();
        Ok(())
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        self.commit();
        Ok(())
    }
}

impl<'a> SerializeTupleVariant for EntitySerializerStruct<'a> {
    type Ok = ();

    type Error = std::fmt::Error;

    fn serialize_field<T>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: Serialize + ?Sized,
    {
        value.serialize(self.push()).unwrap();
        Ok(())
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        self.commit();
        Ok(())
    }
}

impl<'a> SerializeStructVariant for EntitySerializerStruct<'a> {
    type Ok = ();

    type Error = std::fmt::Error;

    fn serialize_field<T>(&mut self, key: &'static str, value: &T) -> Result<(), Self::Error>
    where
        T: Serialize + ?Sized,
    {
        value.serialize(self.child(key.to_owned())).unwrap();
        Ok(())
    }
    fn end(self) -> Result<Self::Ok, Self::Error> {
        self.commit();
        Ok(())
    }
}

/// Serializes anything that implements Serialize into an EntityDataValue
/// that can be easily displayed by the Entity Debug UI.
pub fn serialize_entity<T>(value: &T) -> EntityDataValue
where
    T: Serialize + ?Sized,
{
    let mut data = EntitySerializerData::new();
    value.serialize(data.serializer(String::new())).unwrap();
    data.output.entries.into_iter().next().unwrap().value
}

#[cfg(test)]
mod tests {
    use super::serialize_entity;
    use near_jsonrpc_primitives::types::entity_debug::{
        EntityDataEntry, EntityDataStruct, EntityDataValue,
    };
    use serde::Serialize;

    fn val<T: ToString>(s: T) -> EntityDataValue {
        EntityDataValue::String(s.to_string())
    }

    fn tree(entries: Vec<(&str, EntityDataValue)>) -> EntityDataValue {
        EntityDataValue::Struct(Box::new(EntityDataStruct {
            entries: entries
                .into_iter()
                .map(|(name, value)| EntityDataEntry { name: name.to_owned(), value })
                .collect(),
        }))
    }

    #[test]
    fn test_serialize_primitives() {
        assert_eq!(serialize_entity(&(123 as u64)), val("123"));
        assert_eq!(
            serialize_entity(&(10000000000000000000000000000000 as u128)),
            val("10000000000000000000000000000000")
        );
        assert_eq!(serialize_entity(&"abc"), val("abc"));
        assert_eq!(serialize_entity(&true), val("true"));
    }

    #[test]
    fn test_serialize_structs() {
        #[derive(Serialize)]
        struct A {
            a: u64,
            b: String,
        }
        assert_eq!(
            serialize_entity(&A { a: 123, b: "abc".to_owned() }),
            tree(vec![("a", val("123")), ("b", val("abc"))])
        );

        #[derive(Serialize)]
        struct B {
            x: A,
            y: Box<A>,
        }
        assert_eq!(
            serialize_entity(&B {
                x: A { a: 123, b: "abc".to_owned() },
                y: Box::new(A { a: 456, b: "def".to_owned() }),
            }),
            tree(vec![
                ("x", tree(vec![("a", val("123")), ("b", val("abc"))])),
                ("y", tree(vec![("a", val("456")), ("b", val("def"))])),
            ])
        );
    }

    #[test]
    fn test_serialize_vecs() {
        assert_eq!(
            serialize_entity(&vec![1, 2, 3]),
            tree(vec![("0", val("1")), ("1", val("2")), ("2", val("3"))])
        );

        #[derive(Serialize)]
        struct A {
            x: u64,
        }
        assert_eq!(
            serialize_entity(&vec![A { x: 1 }, A { x: 2 }]),
            tree(vec![("0", tree(vec![("x", val("1"))])), ("1", tree(vec![("x", val("2"))])),])
        );
    }

    #[test]
    fn test_serialize_tuples() {
        assert_eq!(
            serialize_entity(&(1, 2, 3)),
            tree(vec![("0", val("1")), ("1", val("2")), ("2", val("3"))])
        );
    }

    #[test]
    fn test_serialize_maps() {
        use std::collections::HashMap;
        let mut map = HashMap::new();
        map.insert("a".to_owned(), 1);
        assert_eq!(serialize_entity(&map), tree(vec![("a", val("1"))]));
    }

    #[test]
    fn test_serialize_enums() {
        #[derive(Serialize)]
        enum A {
            A1,
            A2(u64),
            A3 { x: u64 },
            A4(()),
            A5((String, String)),
        }

        assert_eq!(serialize_entity(&A::A1), val("A1"));
        assert_eq!(serialize_entity(&A::A2(123)), tree(vec![("A2", val("123"))]));
        assert_eq!(
            serialize_entity(&A::A3 { x: 123 }),
            tree(vec![("A3", tree(vec![("x", val("123"))]))])
        );
        assert_eq!(serialize_entity(&A::A4(())), tree(vec![("A4", val("()"))]));
        assert_eq!(
            serialize_entity(&A::A5(("abc".to_owned(), "def".to_owned()))),
            tree(vec![("A5", tree(vec![("0", val("abc")), ("1", val("def"))]))])
        );
    }
}
