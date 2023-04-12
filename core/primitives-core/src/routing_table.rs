use std::collections::HashMap;

use borsh::{BorshDeserialize, BorshSerialize};

use crate::{hash::CryptoHash, namespace::Namespace};

#[derive(BorshSerialize, BorshDeserialize, Clone, Debug, Default)]
pub struct RoutingTable {
    method_resolution_table: HashMap<String, (Namespace, String)>,
}

impl RoutingTable {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn add(
        &mut self,
        incoming_method_name: String,
        target_namespace: Namespace,
        target_method_name: String,
    ) {
        self.method_resolution_table
            .insert(incoming_method_name, (target_namespace, target_method_name));
    }

    pub fn merge(&mut self, other: RoutingTable) {
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

        table_a.add(method_a.clone(), namespace_a.clone(), method_a.clone());
        table_a.add(method_b.clone(), namespace_a.clone(), method_b.clone());

        table_b.add(method_b.clone(), namespace_b.clone(), method_b.clone());
        table_b.add(method_c.clone(), namespace_b.clone(), method_c.clone());

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
