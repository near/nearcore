//rust 1.30.0 

pub struct TrieNode {
    value: char,
    is_final: bool,
    child_nodes: Vec<Box<TrieNode>>,
}

impl TrieNode {
    // Create new node
    pub fn create(c: char, is_final: bool) -> TrieNode {}
    // Check if a node has that value
    pub fn check_value(self, c: char) -> bool {
        return true;
    }
}

struct TrieStruct {
    root_node: TrieNode,
}

impl TrieStruct {
    // Insert a string
    pub fn insert(string_val: String) {}
    // Find a string
    pub fn find(string_val: String) -> bool {
        return true;
    }
}

use std::collections::HashMap;

pub struct TrieNode {
    value: Option<char>,
    is_final: bool,
    child_nodes: HashMap<char, TrieNode>,
}

impl TrieNode {
    // Create new node
    pub fn new(c: char, is_final: bool) -> TrieNode {
        TrieNode {
            value: Option::Some(c),
            is_final: is_final,
            child_nodes: HashMap::new(),
        }
    }

    pub fn new_root() -> TrieNode {
        TrieNode {
            value: Option::None,
            is_final: false,
            child_nodes: HashMap::new(),
        }
    }

    // Check if a node has that value
    pub fn check_value(self, c: char) -> bool {
        self.value == Some(c)
    }

    pub fn insert_value(&mut self, c: char, is_final: bool) {
        self.child_nodes.insert(c, TrieNode::new(c, is_final));
    }
}

#[derive(Debug)]
struct TrieStruct {
    root_node: TrieNode,
}

impl TrieStruct {
    // Create a TrieStruct
    pub fn create() -> TrieStruct {
        TrieStruct {
            root_node: TrieNode::new_root(),
        }
    }

    // Insert a string
    pub fn insert(&mut self, string_val: String) {
        let mut current_node = &mut self.root_node;
        let char_list: Vec<char> = string_val.chars().collect();
        let mut last_match = 0;

        for letter_counter in 0..char_list.len() {
            if current_node
                .child_nodes
                .contains_key(&char_list[letter_counter])
            {
                current_node = current_node
                    .child_nodes
                    .get_mut(&char_list[letter_counter])
                    .unwrap();
            } else {
                last_match = letter_counter;
                break;
            }
            last_match = letter_counter + 1;
        }

        if last_match == char_list.len() {
            current_node.is_final = true;
        } else {
            for new_counter in last_match..char_list.len() {
                println!(
                    "Inserting {} into {}",
                    char_list[new_counter],
                    current_node.value.unwrap_or_default()
                );
                current_node.insert_value(char_list[new_counter], false);
                current_node = current_node
                    .child_nodes
                    .get_mut(&char_list[new_counter])
                    .unwrap();
            }
            current_node.is_final = true;
        }
    }

    // Find a string
    pub fn find(&mut self, string_val: String) -> bool {
        let mut current_node = &mut self.root_node;
        let char_list: Vec<char> = string_val.chars().collect();

        for counter in 0..char_list.len() {
            if !current_node.child_nodes.contains_key(&char_list[counter]) {
                return false;
            } else {
                current_node = current_node
                    .child_nodes
                    .get_mut(&char_list[counter])
                    .unwrap();
            }
        }
        return true;
    }
}


// Find Stuff
    println!(
        "Is Testing in the trie? {}",
        trie_test.find("Testing".to_string())
    );
    println!(
        "Is Brown in the trie? {}",
        trie_test.find("Brown".to_string())
    );