use std::collections::HashMap;

use borsh::{BorshDeserialize, BorshSerialize};

use crate::{hash::CryptoHash, namespace::Namespace};

#[derive(BorshSerialize, BorshDeserialize, Clone, Debug, Default)]
pub struct RoutingTable {
    namespace_table: HashMap<Namespace, CryptoHash>,
    method_resolution_table: HashMap<String, (Namespace, String)>,
}

pub struct RegisteredNamespace<'a> {
    registered_to: &'a mut RoutingTable,
    namespace: Namespace,
}

impl<'a> RegisteredNamespace<'a> {
    pub fn add(&mut self, incoming_method_name: String, target_method_name: String) {
        self.registered_to
            .method_resolution_table
            .insert(incoming_method_name, (self.namespace.clone(), target_method_name));
    }
}

impl RoutingTable {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn namespace<'s>(&'s mut self, namespace: Namespace) -> Option<RegisteredNamespace<'s>> {
        if self.namespace_table.contains_key(&namespace) {
            Some(RegisteredNamespace { registered_to: self, namespace })
        } else {
            None
        }
    }

    pub fn register_namespace(
        &mut self,
        namespace: Namespace,
        code_hash: CryptoHash,
    ) -> RegisteredNamespace {
        self.namespace_table.insert(namespace.clone(), code_hash);
        RegisteredNamespace { registered_to: self, namespace }
    }

    pub fn unregister_namespace(&mut self, namespace: &Namespace) {
        self.namespace_table.remove(namespace);
    }

    pub fn merge(&mut self, other: RoutingTable) {
        self.namespace_table.extend(other.namespace_table);
        self.method_resolution_table.extend(other.method_resolution_table);
    }

    pub fn resolve_method(&self, incoming_method_name: &str) -> Option<&(Namespace, String)> {
        self.method_resolution_table.get(incoming_method_name)
    }

    pub fn inverse_resolve_method(
        &self,
        target_namespace: &Namespace,
        target_method_name: &str,
    ) -> Option<&String> {
        self.method_resolution_table
            .iter()
            .find(|(_, (namespace, method_name))| {
                namespace == target_namespace && method_name == target_method_name
            })
            .map(|(incoming_method_name, _)| incoming_method_name)
    }
}

#[cfg(test)]
mod tests {
    use crate::namespace::Namespace;

    use super::RoutingTable;

    #[test]
    fn routing_table_merge() {
        let mut table_a = RoutingTable::new();
        let mut table_b = RoutingTable::new();

        let namespace_a: Namespace = "namespace_a".into();
        let namespace_b: Namespace = "namespace_b".into();

        let method_a = "method_a".to_string();
        let method_b = "method_b".to_string();
        let method_c = "method_c".to_string();

        let mut ta_a = table_a.register_namespace(namespace_a.clone(), Default::default());
        let mut tb_b = table_b.register_namespace(namespace_b.clone(), Default::default());

        ta_a.add(method_a.clone(), method_a.clone());
        ta_a.add(method_b.clone(), method_b.clone());

        tb_b.add(method_b.clone(), method_b.clone());
        tb_b.add(method_c.clone(), method_c.clone());

        table_a.merge(table_b);

        assert_eq!(
            table_a.resolve_method(&method_a),
            Some(&(namespace_a.clone(), method_a.clone())),
        );
        assert_eq!(
            table_a.resolve_method("method_b"),
            Some(&(namespace_b.clone(), method_b.clone())),
        );
    }
}
