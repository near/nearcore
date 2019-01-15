use primitives::signature::DEFAULT_SIGNATURE;
use primitives::traits::Payload;
use primitives::types::UID;
use std::hash::{Hash, Hasher};

#[derive(Hash, Serialize, Deserialize, Clone, Debug)]
pub struct FakePayload {}

impl Payload for FakePayload {
    fn verify(&self) -> Result<(), &'static str> {
        Ok(())
    }
    fn union_update(&mut self, _other: Self) { }
    fn is_empty(&self) -> bool {true}
    fn new() -> Self {Self{}}
}

pub fn simple_bare_message(
    owner_uid: UID,
    epoch: u64,
    parents: Vec<&::primitives::types::SignedMessageData<FakePayload>>,
) -> ::primitives::types::SignedMessageData<FakePayload> {
    let body = ::primitives::types::MessageDataBody {
        owner_uid,
        parents: parents.into_iter().map(|m| m.hash).collect(),
        epoch,
        payload: crate::testing_utils::FakePayload {},
        endorsements: vec![],
    };
    let hash = {
        let mut hasher = ::std::collections::hash_map::DefaultHasher::new();
        body.hash(&mut hasher);
        hasher.finish()
    };
    ::primitives::types::SignedMessageData {
        owner_sig: DEFAULT_SIGNATURE,
        hash,
        body,
    }
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
        let r = &*$arena.alloc(crate::testing_utils::simple_bare_message($owner, $epoch, ps));
        $all_messages.push(r);
        $messages.push(r);
        simple_bare_messages!($arena, $all_messages, $messages [$($rest)*]);
    }};

    ($arena:ident, $all_messages:ident, $messages:ident [ [ $($parents:tt)* ] => $owner:expr, $epoch:expr => $name:ident; $($rest:tt)* ]) => {{
        let ps = simple_bare_messages!($arena, $all_messages [ $($parents)* ]);
        $name = &*$arena.alloc(crate::testing_utils::simple_bare_message($owner, $epoch, ps));
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
        let r = &*$arena.alloc(crate::testing_utils::simple_bare_message($owner, $epoch, vec![]));
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
        $name = &*$arena.alloc(crate::testing_utils::simple_bare_message($owner, $epoch, vec![]));
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

#[cfg(test)]
mod tests {
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
        let v =
            simple_bare_messages!(arena, all [[0, 0 => a; 1, 2;] => 2, 3; [0, 1; 2, 3;] => 3, 4;]);
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
}
