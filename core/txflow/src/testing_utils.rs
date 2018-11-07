#[derive(Hash)]
pub struct FakePayload {}

pub fn simple_message(owner_uid: u64, epoch: u64) -> ::primitives::types::SignedMessageData<FakePayload> {
    ::primitives::types::SignedMessageData {
        owner_sig: 0,
        hash: 0,
        body: ::primitives::types::MessageDataBody {
            owner_uid,
            parents: vec![],
            epoch,
            payload: ::testing_utils::FakePayload {},
            endorsements: vec![],
        },
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
                       let message = ::message::Message::new(::testing_utils::simple_message(
                       $owner_uid, $epoch));
                       messages.push(message);
                    }
                )+
                messages
            }
        };
);

/// Link several messages into a DAG.
/// # Examples:
///
/// ```
/// let messages = test_messages!(1=>10, 2=>10, 3=>11);
/// link_messages!(&messages[0] => &messages[1],
///                &messages[1] => &messages[2],
///                &messages[0] => &messages[2]);
/// ```
macro_rules! link_messages(
        { $($a:expr => $b:expr),+ } => {
            {
                $(
                    ::message::Message::link(&$a, &$b);
                )+
            }
        };
    );

/// Populate messages linked into a DAG.
macro_rules! populate_messages(
        {$starting_epoch:expr, $witness_selector:expr, $($message:expr => $recompute_epoch:expr),+ } => {
            {
                $(
                    {
                      $message.borrow_mut().populate_from_parents($recompute_epoch, $starting_epoch, &$witness_selector);
                    }
                )+
            }
        };
    );

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
    fn simple_link_messages() {
        let messages = test_messages!(1=>10, 2=>10, 3=>11);
        link_messages!(
        &messages[0] => &messages[1],
        &messages[1] => &messages[2],
        &messages[0] => &messages[2]);
    }

    #[test]
    fn populate_messages() {
        struct FakeWitnessSelector {
            schedule: HashMap<u64, HashSet<u64>>,
        }
        impl FakeWitnessSelector {
            fn new() -> FakeWitnessSelector {
                let mut schedule = HashMap::new();
                let mut epoch_schedule = HashSet::new();
                epoch_schedule.insert(10);
                epoch_schedule.insert(11);
                schedule.insert(1, epoch_schedule.clone());
                schedule.insert(2, epoch_schedule.clone());
                FakeWitnessSelector {
                    schedule,
                }
            }
        }

        impl WitnessSelectorLike for FakeWitnessSelector {
            fn epoch_witnesses(&self, epoch: u64) -> &HashSet<u64> {
                self.schedule.get(&epoch).unwrap()
            }
            fn epoch_leader(&self, epoch: u64) -> u64 {
                *self.epoch_witnesses(epoch).iter().min().unwrap()
            }
        }
        let selector = FakeWitnessSelector::new();
        let messages = test_messages!(1=>10, 1=>10, 1=>11);
        link_messages!(
        &messages[0] => &messages[1],
        &messages[1] => &messages[2],
        &messages[0] => &messages[2]);
        populate_messages!(1, selector,
        &messages[0] => false,
        &messages[1] => true,
        &messages[2] => true);
    }
}
