use primitives::traits::PayloadLike;
use std::hash::{Hash, Hasher};
use std::collections::hash_map::DefaultHasher;

#[derive(Hash)]
pub struct FakePayload {}

impl PayloadLike for FakePayload {
    fn verify(&self) -> Result<(), &'static str> {
        Ok(())
    }
}

pub fn simple_message(owner_uid: u64, epoch: u64) -> ::primitives::types::SignedMessageData<FakePayload> {
    let body = ::primitives::types::MessageDataBody {
            owner_uid,
            parents: vec![],
            epoch,
            payload: ::testing_utils::FakePayload {},
            endorsements: vec![],
        };
    let hash = {
        let mut hasher = DefaultHasher::new();
        body.hash(&mut hasher);
        hasher.finish()
    };
    ::primitives::types::SignedMessageData {
        owner_sig: 0,
        hash,
        body,
    }
}

pub fn simple_message_parents(owner_uid: u64, epoch: u64,
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
        let mut hasher = DefaultHasher::new();
        body.hash(&mut hasher);
        hasher.finish()
    };
    ::primitives::types::SignedMessageData {
        owner_sig: 0,
        hash,
        body,
    }
}


/// Create several messages, and set their owner_uid and epoch.
/// # Examples:
/// Create two messages owned by 10 and one message owned by 11, with epochs 1,2,3.
///
/// ```
/// let messages = test_messages!(1=>10, 2=>10, 3=>11);
/// assert_eq!(messages.len(), 3);
/// ```
macro_rules! test_messages(
        { $($epoch:expr => $owner_uid:expr),+ } => {
            {
                let mut messages = vec![];
                $(
                    {
                       let message = ::testing_utils::simple_message($owner_uid, $epoch);
                       messages.push(message);
                    }
                )+
                messages
            }
        };
);

///
/// ```
/// simple_messages!(arena [0, 2; 1, 3;]);
/// ```
///
/// ```
/// let a;
/// simple_messages!(arena [0, 2 => a; 1, 3;]);
/// ```
///
/// ```
/// simple_messages!(arena [[0, 2; 1, 3;] => 0, 4;]);
/// simple_messages!(arena [[0, 1;] => 0, 5; [0, 2; 1, 3;] => 0, 4;]);
/// simple_messages!(arena [0, 1; [0, 2; 1, 3;] => 0, 4;]);
macro_rules! simple_messages {
    ($arena:ident, $messages:ident [  ]) => (());

    ($arena:ident, $messages:ident [ [ $($parents:tt)* ] => $owner:expr, $epoch:expr; $($rest:tt)* ]) => {{
        let ps = simple_messages!($arena [ $($parents)* ]);
        $messages.push(&*$arena.alloc(::testing_utils::simple_message_parents($owner, $epoch, ps)));
        simple_messages!($arena, $messages [$($rest)*]);
    }};

    ($arena:ident, $messages:ident [ $element:expr; $($rest:tt)* ]) => {{
        $messages.push($element);
        simple_messages!($arena, $messages [$($rest)*]);
    }};

    ($arena:ident, $messages:ident [ $owner:expr, $epoch:expr; $($rest:tt)* ]) => {{
        $messages.push(&*$arena.alloc(::testing_utils::simple_message($owner, $epoch)));
        simple_messages!($arena, $messages [ $($rest)* ]);
    }};

    ($arena:ident, $messages:ident [ $owner:expr, $epoch:expr; $($rest:tt)* ]) => {{
        $messages.push(&*$arena.alloc(::testing_utils::simple_message($owner, $epoch)));
        simple_messages!($arena, $messages [ $($rest)* ]);
    }};

    ($arena:ident, $messages:ident [ $owner:expr, $epoch:expr => $name:ident; $($rest:tt)* ]) => {{
        $name = &*$arena.alloc(::testing_utils::simple_message($owner, $epoch));
        $messages.push($name);
        simple_messages!($arena, $messages [ $($rest)* ]);
    }};

    ($arena:ident [ $($rest:tt)* ]) => {{
        let mut v = vec![];
        {
          let p = &mut v;
          simple_messages!($arena, p [ $($rest)* ]);
        }
        v
    }};
}

///// Populate messages linked into a DAG.
//macro_rules! populate_messages(
//        {$starting_epoch:expr, $witness_selector:expr, $($message:expr => $recompute_epoch:expr),+ } => {
//            {
//                $(
//                    {
//                      $message.borrow_mut().populate_from_parents($recompute_epoch, $starting_epoch, &$witness_selector);
//                    }
//                )+
//            }
//        };
//    );

#[cfg(test)]
mod tests {
    use std::collections::{HashSet, HashMap};
    use primitives::traits::WitnessSelectorLike;

    #[test]
    fn simple_test_messages() {
        let messages = test_messages!(1=>10, 2=>10, 3=>11);
        assert_eq!(messages.len(), 3);
    }

    #[test]
    fn test_flat_messages() {
        use typed_arena::Arena;
        let arena = Arena::new();
        let a;
        let v = simple_messages!(arena [0, 0 => a; 1, 2;]);
        assert_eq!(v.len(), 2);
        assert_eq!((&a.body).epoch, 0);
    }

    #[test]
    fn test_connected_messages() {
        use typed_arena::Arena;
        let arena = Arena::new();
        let a;
        let v = simple_messages!(arena [[0, 0 => a; 1, 2;] => 2, 3;]);
        assert_eq!(v.len(), 1);
        assert_eq!(v[0].body.parents.len(), 2);
    }

//    #[test]
//    fn populate_messages() {
//        struct FakeWitnessSelector {
//            schedule: HashMap<u64, HashSet<u64>>,
//        }
//        impl FakeWitnessSelector {
//            fn new() -> FakeWitnessSelector {
//                let mut schedule = HashMap::new();
//                let mut epoch_schedule = HashSet::new();
//                epoch_schedule.insert(10);
//                epoch_schedule.insert(11);
//                schedule.insert(1, epoch_schedule.clone());
//                schedule.insert(2, epoch_schedule.clone());
//                FakeWitnessSelector {
//                    schedule,
//                }
//            }
//        }

//        impl WitnessSelectorLike for FakeWitnessSelector {
//            fn epoch_witnesses(&self, epoch: u64) -> &HashSet<u64> {
//                self.schedule.get(&epoch).unwrap()
//            }
//            fn epoch_leader(&self, epoch: u64) -> u64 {
//                *self.epoch_witnesses(epoch).iter().min().unwrap()
//            }
//        }
//        let selector = FakeWitnessSelector::new();
//        let messages = test_messages!(1=>10, 1=>10, 1=>11);
//        link_messages!(
//        &messages[0] => &messages[1],
//        &messages[1] => &messages[2],
//        &messages[0] => &messages[2]);
//        populate_messages!(1, selector,
//        &messages[0] => false,
//        &messages[1] => true,
//        &messages[2] => true);
//    }
}
