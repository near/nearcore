use primitives::traits::{Payload, WitnessSelector};
use primitives::types::{UID, MessageDataBody};
use std::hash::{Hash, Hasher};
use std::collections::hash_map::DefaultHasher;
use super::Message;

#[derive(Hash, Clone, Debug)]
pub struct FakePayload {}

impl Payload for FakePayload {
    fn verify(&self) -> Result<(), &'static str> {
        Ok(())
    }
}

pub fn simple_message<'a, W>(owner_uid: UID, epoch: u64, parents: Vec<&'a Message<'a, FakePayload>>,
    recompute_epoch: bool, starting_epoch: u64, witness_selector: &W) -> Message<'a, FakePayload>
    where W: WitnessSelector {
    let body = MessageDataBody {
            owner_uid,
            parents: (&parents).into_iter().map(|m| m.computed_hash).collect(),
            epoch,
            payload: FakePayload {},
            endorsements: vec![],
        };
    let hash = {
        let mut hasher = DefaultHasher::new();
        body.hash(&mut hasher);
        hasher.finish()
    };
    let mut message = Message::new(
        ::primitives::types::SignedMessageData {
            owner_sig: 0,
            hash,
            body,
        });
    message.parents = parents.into_iter().collect();
    message.init(recompute_epoch, starting_epoch, witness_selector);
    message
}

/// Same as `simple_bare_messages`, but creates `Message` instances instead of bare `SingedMessageData`.
/// Takes additional arguments: `starting_epoch`, `witness_selector`, `recompute_epoch`.
/// # Examples:
///
/// Standard usage with linking to a variable for later use.
///
/// ```
/// let a;
/// simple_messages!(0, &selector, arena [[0, 0, false => a; 1, 2, false;] => 2, 3, true;]);
/// simple_messages!(0, &selector, arena [[=> a; 3, 3, false;] => 3, 3, true;]);
/// ```
macro_rules! simple_messages {
    ($starting_epoch:expr, $witness_selector:expr, $arena:ident, $messages:ident [  ]) => (());

    ($starting_epoch:expr, $witness_selector:expr, $arena:ident, $messages:ident [ [ $($parents:tt)* ] => $owner:expr, $epoch:expr, $recompute_epoch:expr; $($rest:tt)* ]) => {{
        let ps = simple_messages!($starting_epoch, $witness_selector, $arena [ $($parents)* ]);
        $messages.push(&*$arena.alloc(::dag::message::testing_utils::simple_message($owner, $epoch, ps, $recompute_epoch, $starting_epoch, $witness_selector)));
        simple_messages!($starting_epoch, $witness_selector, $arena, $messages [$($rest)*]);
    }};

    ($starting_epoch:expr, $witness_selector:expr, $arena:ident, $messages:ident [ [ $($parents:tt)* ] => $owner:expr, $epoch:expr, $recompute_epoch:expr => $name:ident; $($rest:tt)* ]) => {{
        let ps = simple_messages!($starting_epoch, $witness_selector, $arena [ $($parents)* ]);
        $name = &*$arena.alloc(::dag::message::testing_utils::simple_message($owner, $epoch, ps, $recompute_epoch, $starting_epoch, $witness_selector));
        $messages.push($name);
        simple_messages!($starting_epoch, $witness_selector, $arena, $messages [$($rest)*]);
    }};

    ($starting_epoch:expr, $witness_selector:expr, $arena:ident, $messages:ident [ => $element:expr; $($rest:tt)* ]) => {{
        $messages.push($element);
        simple_messages!($starting_epoch, $witness_selector, $arena, $messages [$($rest)*]);
    }};

    ($starting_epoch:expr, $witness_selector:expr, $arena:ident, $messages:ident [ $owner:expr, $epoch:expr, $recompute_epoch:expr; $($rest:tt)* ]) => {{
        $messages.push(&*$arena.alloc(::dag::message::testing_utils::simple_message($owner, $epoch, vec![], $recompute_epoch, $starting_epoch, $witness_selector)));
        simple_messages!($starting_epoch, $witness_selector, $arena, $messages [ $($rest)* ]);
    }};

    ($starting_epoch:expr, $witness_selector:expr, $arena:ident, $messages:ident [ $owner:expr, $epoch:expr, $recompute_epoch:expr; $($rest:tt)* ]) => {{
        $messages.push(&*$arena.alloc(::dag::message::testing_utils::simple_message($owner, $epoch, vec![], $recompute_epoch, $starting_epoch, $witness_selector)));
        simple_messages!($starting_epoch, $witness_selector, $arena, $messages [ $($rest)* ]);
    }};

    ($starting_epoch:expr, $witness_selector:expr, $arena:ident, $messages:ident [ $owner:expr, $epoch:expr, $recompute_epoch:expr => $name:ident; $($rest:tt)* ]) => {{
        $name = &*$arena.alloc(::dag::message::testing_utils::simple_message($owner, $epoch, vec![], $recompute_epoch, $starting_epoch, $witness_selector));
        $messages.push($name);
        simple_messages!($starting_epoch, $witness_selector, $arena, $messages [ $($rest)* ]);
    }};

    ($starting_epoch:expr, $witness_selector:expr, $arena:ident [ $($rest:tt)* ]) => {{
        let mut v = vec![];
        {
          let p = &mut v;
          simple_messages!($starting_epoch, $witness_selector, $arena, p [ $($rest)* ]);
        }
        v
    }};
}

#[cfg(test)]
mod tests {
    use std::collections::{HashSet, HashMap};
    use primitives::traits::WitnessSelector;
    use typed_arena::Arena;

    struct FakeWitnessSelector {
        schedule: HashMap<u64, HashSet<u64>>,
    }

    impl FakeWitnessSelector {
        fn new() -> FakeWitnessSelector {
            FakeWitnessSelector {
                schedule: map!{
               0 => set!{0, 1, 2, 3}, 1 => set!{1, 2, 3, 4},
               2 => set!{2, 3, 4, 5}, 3 => set!{3, 4, 5, 6}}
            }
        }
    }

    impl WitnessSelector for FakeWitnessSelector {
        fn epoch_witnesses(&self, epoch: u64) -> &HashSet<u64> {
            self.schedule.get(&epoch).unwrap()
        }
        fn epoch_leader(&self, epoch: u64) -> u64 {
            *self.epoch_witnesses(epoch).iter().min().unwrap()
        }
    }

    #[test]
    fn flat_messages() {
        let selector = FakeWitnessSelector::new();
        let arena = Arena::new();
        let v = simple_messages!(0, &selector, arena [0, 1, false;]);
        assert_eq!(v.len(), 1);
    }

    #[test]
    fn link_messages() {
        let selector = FakeWitnessSelector::new();
        let arena = Arena::new();
        let a;
        simple_messages!(0, &selector, arena [[0, 1, false => a; 1, 2, false;] => 2, 3, true;]);
    }

    #[test]
    fn reuse_messages() {
        let selector = FakeWitnessSelector::new();
        let arena = Arena::new();
        let a;
        simple_messages!(0, &selector, arena [[0, 0, false => a; 1, 2, false;] => 2, 3, true;]);
        simple_messages!(0, &selector, arena [[=> a; 3, 3, false;] => 3, 3, true;]);
    }
}
