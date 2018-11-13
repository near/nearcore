use primitives::traits::{Payload, WitnessSelector};
use primitives::types::UID;
use std::hash::{Hash, Hasher};

#[derive(Hash, Clone, Debug)]
pub struct FakePayload {}

impl Payload for FakePayload {
    fn verify(&self) -> Result<(), &'static str> {
        Ok(())
    }
}

pub fn simple_bare_message(owner_uid: UID, epoch: u64,
                           parents: Vec<&::primitives::types::SignedMessageData<FakePayload>>)
    -> ::primitives::types::SignedMessageData<FakePayload> {
    let body = ::primitives::types::MessageDataBody {
            owner_uid,
            parents: parents.into_iter().map(|m| m.hash).collect(),
            epoch,
            payload: ::testing_utils::FakePayload {},
            endorsements: vec![],
        };
    let hash = {
        let mut hasher = ::std::collections::hash_map::DefaultHasher::new();
        body.hash(&mut hasher);
        hasher.finish()
    };
    ::primitives::types::SignedMessageData {
        owner_sig: 0,
        hash,
        body,
    }
}

pub fn simple_message<'a, W>(owner_uid: UID, epoch: u64,
                      parents: Vec<&'a ::message::Message<'a, FakePayload>>,
    recompute_epoch: bool, starting_epoch: u64, witness_selector: &W) -> ::message::Message<'a, FakePayload>
    where W: WitnessSelector {
    let body = ::primitives::types::MessageDataBody {
            owner_uid,
            parents: (&parents).into_iter().map(|m| m.computed_hash).collect(),
            epoch,
            payload: ::testing_utils::FakePayload {},
            endorsements: vec![],
        };
    let hash = {
        let mut hasher = ::std::collections::hash_map::DefaultHasher::new();
        body.hash(&mut hasher);
        hasher.finish()
    };
    let mut message = ::message::Message::new(
        ::primitives::types::SignedMessageData {
            owner_sig: 0,
            hash,
            body,
        });
    message.parents = parents.into_iter().collect();
    message.init(recompute_epoch, starting_epoch, witness_selector);
    message
}

/// Allows to build a DAG from `SignedMessageData` objects by constructing forests.
/// # Examples:
/// Create two messages with `owner_uid=0`, `epoch=2` and `owner_uid=1`, `epoch=3`.
///
/// ```
/// simple_bare_messages!(arena, all_messages [0, 2; 1, 3;]);
/// ```
///
/// Create two messages and save a reference to the first message in `a`.
///
/// ```
/// let a;
/// simple_bare_messages!(arena, all_messages [0, 2 => a; 1, 3;]);
/// ```
///
/// Create two messages and link them to the third message as parents.
///
/// ```
/// simple_bare_messages!(arena, all_messages [[0, 2; 1, 3;] => 0, 4;]);
/// ```
///
/// Reuse some message instead of creating a new one.
///
/// ```
/// let a;
/// simple_bare_messages!(arena, all_messages [[0, 0 => a; 1, 2;] => 2, 3;]);
/// simple_bare_messages!(arena, all_messages [[=> a; 3, 4;] => 4, 5;]);
/// ```
///
/// Create several forests with different structure.
///
/// ```
/// simple_bare_messages!(arena, all_messages [[0, 1;] => 0, 5; [0, 2; 1, 3;] => 0, 4;]);
/// simple_bare_messages!(arena, all_messages [0, 1; [0, 2; 1, 3;] => 0, 4;]);
/// ```
///
macro_rules! simple_bare_messages {
    ($arena:ident, $all_messages:ident, $messages:ident [  ]) => (());

    ($arena:ident, $all_messages:ident, $messages:ident [ [ $($parents:tt)* ] => $owner:expr, $epoch:expr; $($rest:tt)* ]) => {{
        let ps = simple_bare_messages!($arena, $all_messages [ $($parents)* ]);
        let r = &*$arena.alloc(::testing_utils::simple_bare_message($owner, $epoch, ps));
        $all_messages.push(r);
        $messages.push(r);
        simple_bare_messages!($arena, $all_messages, $messages [$($rest)*]);
    }};

    ($arena:ident, $all_messages:ident, $messages:ident [ [ $($parents:tt)* ] => $owner:expr, $epoch:expr => $name:ident; $($rest:tt)* ]) => {{
        let ps = simple_bare_messages!($arena, $all_messages [ $($parents)* ]);
        $name = &*$arena.alloc(::testing_utils::simple_bare_message($owner, $epoch, ps));
        $all_messages.push($name);
        $messages.push($name);
        simple_bare_messages!($arena, $all_messages, $messages [$($rest)*]);
    }};

    ($arena:ident, $all_messages:ident, $messages:ident [ => $element:expr; $($rest:tt)* ]) => {{
        $all_messages.push($element);
        $messages.push($element);
        simple_bare_messages!($arena, $all_messages, $messages [$($rest)*]);
    }};

    ($arena:ident, $all_messages:ident, $messages:ident [ $owner:expr, $epoch:expr; $($rest:tt)* ]) => {{
        let r = &*$arena.alloc(::testing_utils::simple_bare_message($owner, $epoch, vec![]));
        $all_messages.push(r);
        $messages.push(r);
        simple_bare_messages!($arena, $all_messages, $messages [ $($rest)* ]);
    }};

    ($arena:ident, $all_messages:ident, $messages:ident [ $owner:expr, $epoch:expr; $($rest:tt)* ]) => {{
        let r = &*$arena.alloc(::testing_utils::simple_bare_message($owner, $epoch, vec![]));
        $all_messages.push(r);
        $messages.push(r);
        simple_bare_messages!($arena, $all_messages, $messages [ $($rest)* ]);
    }};

    ($arena:ident, $all_messages:ident, $messages:ident [ $owner:expr, $epoch:expr => $name:ident; $($rest:tt)* ]) => {{
        $name = &*$arena.alloc(::testing_utils::simple_bare_message($owner, $epoch, vec![]));
        $all_messages.push($name);
        $messages.push($name);
        simple_bare_messages!($arena, $all_messages, $messages [ $($rest)* ]);
    }};

    ($arena:ident, $all_messages:ident [ $($rest:tt)* ]) => {{
        let mut v = vec![];
        {
          let p = &mut v;
          simple_bare_messages!($arena, $all_messages, p [ $($rest)* ]);
        }
        v
    }};
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
        $messages.push(&*$arena.alloc(::testing_utils::simple_message($owner, $epoch, ps, $recompute_epoch, $starting_epoch, $witness_selector)));
        simple_messages!($starting_epoch, $witness_selector, $arena, $messages [$($rest)*]);
    }};

    ($starting_epoch:expr, $witness_selector:expr, $arena:ident, $messages:ident [ [ $($parents:tt)* ] => $owner:expr, $epoch:expr, $recompute_epoch:expr => $name:ident; $($rest:tt)* ]) => {{
        let ps = simple_messages!($starting_epoch, $witness_selector, $arena [ $($parents)* ]);
        $name = &*$arena.alloc(::testing_utils::simple_message($owner, $epoch, ps, $recompute_epoch, $starting_epoch, $witness_selector));
        $messages.push($name);
        simple_messages!($starting_epoch, $witness_selector, $arena, $messages [$($rest)*]);
    }};

    ($starting_epoch:expr, $witness_selector:expr, $arena:ident, $messages:ident [ => $element:expr; $($rest:tt)* ]) => {{
        $messages.push($element);
        simple_messages!($starting_epoch, $witness_selector, $arena, $messages [$($rest)*]);
    }};

    ($starting_epoch:expr, $witness_selector:expr, $arena:ident, $messages:ident [ $owner:expr, $epoch:expr, $recompute_epoch:expr; $($rest:tt)* ]) => {{
        $messages.push(&*$arena.alloc(::testing_utils::simple_message($owner, $epoch, vec![], $recompute_epoch, $starting_epoch, $witness_selector)));
        simple_messages!($starting_epoch, $witness_selector, $arena, $messages [ $($rest)* ]);
    }};

    ($starting_epoch:expr, $witness_selector:expr, $arena:ident, $messages:ident [ $owner:expr, $epoch:expr, $recompute_epoch:expr; $($rest:tt)* ]) => {{
        $messages.push(&*$arena.alloc(::testing_utils::simple_message($owner, $epoch, vec![], $recompute_epoch, $starting_epoch, $witness_selector)));
        simple_messages!($starting_epoch, $witness_selector, $arena, $messages [ $($rest)* ]);
    }};

    ($starting_epoch:expr, $witness_selector:expr, $arena:ident, $messages:ident [ $owner:expr, $epoch:expr, $recompute_epoch:expr => $name:ident; $($rest:tt)* ]) => {{
        $name = &*$arena.alloc(::testing_utils::simple_message($owner, $epoch, vec![], $recompute_epoch, $starting_epoch, $witness_selector));
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

    #[test]
    fn flat_bare_messages() {
        let arena = Arena::new();
        let mut all = vec![];
        let a;
        let v = simple_bare_messages!(arena, all [0, 0 => a; 1, 2;]);
        assert_eq!(v.len(), 2);
        assert_eq!((&a.body).epoch, 0);
    }

    #[test]
    fn link_bare_messages() {
        let arena = Arena::new();
        let mut all = vec![];
        let a;
        let v = simple_bare_messages!(arena, all [[0, 0 => a; 1, 2;] => 2, 3; [0, 1; 2, 3;] => 3, 4;]);
        assert_eq!(v.len(), 2);
        assert_eq!(v[0].body.parents.len(), 2);
    }

    #[test]
    fn reuse_bare_messages() {
        let arena = Arena::new();
        let mut all = vec![];
        let a;
        simple_bare_messages!(arena, all [[0, 0 => a; 1, 2;] => 2, 3;]);
        simple_bare_messages!(arena, all [[=> a; 3, 4;] => 4, 5;]);
    }

    #[test]
    fn bare_several_trees() {
        let arena = Arena::new();
        let mut all = vec![];
        simple_bare_messages!(arena, all [[0, 1;] => 0, 5; [0, 2; 1, 3;] => 0, 4;]);
        simple_bare_messages!(arena, all [0, 1; [0, 2; 1, 3;] => 0, 4;]);
    }

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
