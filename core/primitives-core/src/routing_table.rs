use std::collections::HashMap;

use borsh::{BorshDeserialize, BorshSerialize};

use crate::namespace::Namespace;

#[derive(BorshSerialize, BorshDeserialize, Clone, Debug, Default)]
pub struct RoutingTable {
    table: HashMap<String, (Namespace, String)>,
}

impl RoutingTable {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn merge(&mut self, other: RoutingTable) {
        self.table.extend(other.table);
    }

    pub fn add(
        &mut self,
        incoming_method_name: String,
        target_namespace: Namespace,
        target_method_name: String,
    ) {
        self.table.insert(incoming_method_name, (target_namespace, target_method_name));
    }

    pub fn resolve(&self, incoming_method_name: &str) -> Option<&(Namespace, String)> {
        self.table.get(incoming_method_name)
    }

    pub fn inverse_resolve(
        &self,
        target_namespace: &Namespace,
        target_method_name: &str,
    ) -> Option<&String> {
        self.table
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

        table_a.add("method_a".to_string(), namespace_a.clone(), "method_a".to_string());
        table_a.add("method_b".to_string(), namespace_a.clone(), "method_b".to_string());

        table_b.add("method_b".to_string(), namespace_b.clone(), "method_b".to_string());
        table_b.add("method_c".to_string(), namespace_b.clone(), "method_c".to_string());

        table_a.merge(table_b);

        assert_eq!(
            table_a.resolve("method_a"),
            Some(&(namespace_a.clone(), "method_a".to_string()))
        );
        assert_eq!(
            table_a.resolve("method_b"),
            Some(&(namespace_b.clone(), "method_b".to_string()))
        );
    }
}
