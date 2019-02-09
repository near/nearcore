use std::cmp::max;
use std::collections::{HashMap, HashSet};

use primitives::hash::{CryptoHash, hash};
use primitives::serialize::Encode;
use primitives::traits::Payload;

pub type AuthorityId = usize;

#[derive(PartialEq, Eq, Debug)]
pub enum NSResult {
    Success,
    Finalize(Vec<CryptoHash>),
    Retrieve(Vec<CryptoHash>),
    Error(String),
}

impl NSResult {
    #[allow(dead_code)]
    fn is_success(&self) -> bool {
        match &self {
            NSResult::Success => true,
            _ => false
        }
    }

    #[allow(dead_code)]
    fn is_finalize(&self) -> bool {
        match &self {
            NSResult::Finalize(_) => true,
            _ => false
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Message<P> {
    pub author: AuthorityId,
    pub parents: Vec<CryptoHash>,
    pub data: P,
}

impl<P: Payload> Message<P> {
    pub fn calculate_hash(&self) -> CryptoHash {
        hash(&self.encode().expect("Serialization failed"))
    }
}

#[derive(Debug)]
struct Node<P: Payload> {
    message: Message<P>,
    depth: i64,
    endorses: AuthorityId,
    last_depth: Vec<i64>,
    max_score: Vec<u64>,
    max_confidence: Vec<u64>,
}

pub struct Nightshade<P: Payload> {
    owner_id: usize,
    num_authorities: usize,
    nodes: HashMap<CryptoHash, Node<P>>,
    nodes_per_author: Vec<HashMap<i64, CryptoHash>>,
    global_last_depth: Vec<i64>,
    tips: HashSet<CryptoHash>,
    pending_messages: HashMap<CryptoHash, (Message<P>, HashSet<CryptoHash>)>,
    missing_hashes: HashMap<CryptoHash, HashSet<CryptoHash>>,
}

impl<P: Payload> Nightshade<P> {
    pub fn new(owner_id: usize, num_authorities: usize) -> Self {
        Nightshade {
            owner_id,
            num_authorities,
            nodes: Default::default(),
            nodes_per_author: vec![Default::default(); num_authorities],
            global_last_depth: vec![-1; num_authorities],
            pending_messages: Default::default(),
            missing_hashes: Default::default(),
            tips: Default::default(),
        }
    }

    fn filter_candidates(&self, candidates: Vec<usize>, scores: &Vec<u64>) -> Vec<usize> {
        let mut result = vec![];
        for &candidate in candidates.iter() {
            if result.is_empty() || scores[candidate] == scores[result[0]] {
                result.push(candidate);
            } else if scores[candidate] > scores[result[0]] {
                result = vec![candidate];
            }
        }
        result
    }

    fn update_result(&self, current_result: NSResult, new_result: NSResult) -> NSResult {
        match new_result {
            NSResult::Finalize(r) => match current_result {
                NSResult::Finalize(mut v) => { v.extend(r); NSResult::Finalize(v) },
                _ => { NSResult::Finalize(r) }
            },
            other => other
        }
    }

    pub fn process_message(&mut self, message: Message<P>) -> NSResult {
        let hash = message.calculate_hash();
        if self.nodes.contains_key(&hash) || self.pending_messages.contains_key(&hash) {
            return NSResult::Success;
        }
        let unknown_hashes: HashSet<CryptoHash> = (&message.parents)
            .iter()
            .filter_map(|h| if self.nodes.contains_key(h) { None } else { Some(*h) })
            .collect();

        if unknown_hashes.is_empty() {
            return self.add_message(hash, message);
        }
        for h in unknown_hashes.iter() {
            self.missing_hashes.entry(*h).or_insert_with(HashSet::new).insert(hash);
        }
        let retieve_hashes = unknown_hashes.clone().drain().collect();
        self.pending_messages.insert(hash, (message, unknown_hashes));
        NSResult::Retrieve(retieve_hashes)
    }

    fn add_message(&mut self, hash: CryptoHash, message: Message<P>) -> NSResult {
        let mut result = self.insert_message(hash, message);
        // Get messages that were blocked by this one.
        let blocked_messages = match self.missing_hashes.remove(&hash) {
            None => return result,
            Some(blocked_messages) => blocked_messages,
        };
        for blocked_message_hash in blocked_messages {
            let is_unblocked = {
                let (_, unknown_parents) = self.pending_messages.get_mut(&blocked_message_hash).expect("Candidates out of sync");
                unknown_parents.remove(&hash);
                unknown_parents.is_empty()
            };
            if is_unblocked {
                let (_, (blocked_message, _)) = self.pending_messages.remove_entry(&blocked_message_hash).expect("Candidates out of sync");
                let new_result = self.insert_message(blocked_message_hash, blocked_message);
                result = self.update_result(result, new_result);
            }
        }
        result
    }

    fn insert_message(&mut self, hash: CryptoHash, message: Message<P>) -> NSResult {
        let endorses;
        let mut last_depth = vec![-1; self.num_authorities];
        let mut max_score = vec![0; self.num_authorities];
        let mut max_confidence = vec![0; self.num_authorities];
        if message.parents.is_empty() {
            endorses = message.author;
            max_score[message.author] = 1;
        } else {
            let mut local_score: Vec<u64> = vec![0; self.num_authorities];
            for parent in message.parents.iter() {
                if self.tips.contains(parent) {
                    self.tips.remove(parent);
                }
                let parent_node = self.nodes.get(parent).expect("Checked that parents are present");
                for i in 0..self.num_authorities {
                    last_depth[i] = max(last_depth[i], parent_node.last_depth[i]);
                    max_score[i] = max(max_score[i], parent_node.max_score[i]);
                    max_confidence[i] = max(max_confidence[i], parent_node.max_confidence[i]);
                }
            }
            for (i, &ld) in last_depth.iter().enumerate() {
                if ld > self.global_last_depth[i] {
                    self.global_last_depth[i] = ld;
                }
                if ld != -1 {
                    let node = self.nodes.get(self.nodes_per_author[i].get(&(ld as i64)).expect("Depth should be present")).expect("Node should be present");
                    if local_score[node.endorses] <= (self.num_authorities * 2 / 3) as u64 {
                        local_score[node.endorses] += 1;
                    }
                }
            }
            for i in 0..self.num_authorities {
                max_score[i] = max(max_score[i], local_score[i]);
            }
            let mut candidates: Vec<usize> = (0..self.num_authorities).collect();
            candidates = self.filter_candidates(candidates, &max_confidence);
            candidates = self.filter_candidates(candidates, &max_score);
            endorses = candidates[0];

            let mut confidence = 0;
            for (i, &ld) in last_depth.iter().enumerate() {
                if ld != -1 {
                    let node = self.nodes.get(self.nodes_per_author[i].get(&(ld as i64)).expect("Depth should be present")).expect("Node should be present");
                    if node.endorses == endorses && node.max_confidence[endorses] == max_confidence[endorses] && node.max_score[endorses] == max_score[endorses] {
                        confidence += 1;
                    }
                }
            }
            if confidence > (self.num_authorities * 2) / 3 {
                max_confidence[endorses] += 1;
            }
        }
        self.tips.insert(hash);
        let mut finalize = true;
        for i in 0..self.num_authorities {
            if i != endorses && max_confidence[endorses] - max_confidence[i] < 3 {
                finalize = false;
            }
        }
        let depth = last_depth[message.author] + 1;
        last_depth[message.author] = depth;
        self.nodes_per_author[message.author].insert(depth, hash);
        let node = Node {
            message,
            depth,
            endorses,
            last_depth,
            max_score,
            max_confidence
        };
        self.nodes.insert(hash, node);
        if finalize {
            NSResult::Finalize(vec![hash])
        } else {
            NSResult::Success
        }
    }

    pub fn create_message(&mut self, data: P) -> (Message<P>, NSResult) {
        let mut parents: Vec<CryptoHash> = self.tips.iter().cloned().collect();
        parents.sort();
        let m = Message {
            author: self.owner_id,
            parents,
            data,
        };
        let hash = m.calculate_hash();
        let r = self.insert_message(hash, m.clone());
        (m, r)
    }

    pub fn copy_message_data_by_hash(&self, hash: &CryptoHash) -> Option<Message<P>> {
        self.nodes.get(hash).map(|n| n.message.clone())
    }
}

#[cfg(test)]
mod tests {
    use crate::fake_network::FakePayload;

    use super::*;

    fn message(author: AuthorityId, parents: Vec<CryptoHash>) -> (Message<FakePayload>, CryptoHash) {
        let m = Message { author, parents, data: FakePayload { content: 0 } };
        let h = hash(&m.encode().unwrap());
        (m, h)
    }

    fn nightshade_all_sync(num_authorities: usize, num_rounds: usize) {
        let mut ns = vec![];
        for i in 0..num_authorities {
            ns.push(Nightshade::<FakePayload>::new(i, num_authorities));
        }
        for _ in 0..num_rounds {
            let mut messages = vec![];
            for n in ns.iter_mut() {
                let (m, r) = n.create_message(FakePayload { content: 1 });
                assert!(r.is_success());
                messages.push(m);
            }
            for (i, n) in ns.iter_mut().enumerate() {
                for (j, m) in messages.iter().enumerate() {
                    if i != j {
                        assert!(n.process_message(m.clone()).is_success());
                    }

                }
            }
        }
        for n in ns.iter_mut() {
            let (_, r) = n.create_message(FakePayload { content: 0 });
            assert!(r.is_finalize());
        }
    }

    #[test]
    fn test_nightshade_one_authority() {
        nightshade_all_sync(1, 0);
    }

    #[test]
    fn test_nightshade_two_authorities() {
        nightshade_all_sync(2, 5);
    }

    #[test]
    fn test_nightshade_three_authorities() {
        nightshade_all_sync(3, 5);
    }

    #[test]
    fn test_nightshade_ten_authorities() {
        nightshade_all_sync(10, 5);
    }

    #[test]
    fn test_nightshade_basics() {
        let mut ns = Nightshade::new(0, 3);
        let (m1, mh1) = message(0, vec![]);
        let (m2, mh2) = message(1, vec![]);
        let (m3, mh3) = message(2, vec![]);
        assert_eq!(ns.process_message(m1.clone()), NSResult::Success);
        assert_eq!(ns.process_message(m1), NSResult::Success);
        assert_eq!(ns.process_message(m2), NSResult::Success);
        assert_eq!(ns.process_message(m3), NSResult::Success);
        let (m4, mh4) = message(0, vec![mh1]);
        let (m5, mh5) = message(0, vec![mh4]);
        assert_eq!(ns.process_message(m5), NSResult::Retrieve(vec![mh4]));
        assert_eq!(ns.process_message(m4), NSResult::Success);
        let (m6, r6) = ns.create_message(FakePayload { content: 0 });
        assert_eq!(r6, NSResult::Success);
        assert_eq!(m6.parents, vec![mh3, mh2, mh5]);
    }
}