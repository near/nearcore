use std::hash::{Hash, Hasher};
use std::fmt::{Debug, Formatter, Result};
use super::message::MessageWeakRef;

/// A wrapper around MessageWeakRef<T> so that it can be used in HashSet.
/// We cannot implement Hash for Rc<RefCell<Message<T>>> because Hash,Rc,RefCell are from external
/// crate.
pub struct HashableMessage<T: Hash> {
    pub message: MessageWeakRef<T>,
    pub hash: u64,
}

impl<T: Hash> HashableMessage<T> {
    /// Takes ownership of the Weak.
    pub fn new(message: MessageWeakRef<T>) -> HashableMessage<T> {
        let hash = {
            let message = message.upgrade().expect("Can only use message that was not deallocated.");
            let hash = message.borrow().computed_hash
                .expect("Message needs its message to be computed before it is used in a HashSet/HashMap key.");
            hash
        };
        HashableMessage {
            message,
            hash,
        }
    }
}

impl<T: Hash> Hash for HashableMessage<T> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write_u64(self.hash);
    }
}

impl<T: Hash> PartialEq for HashableMessage<T> {
    fn eq(&self, other: &HashableMessage<T>) -> bool {
        self.hash == other.hash
    }
}

impl<T: Hash> Eq for HashableMessage<T> {}

impl<T: Hash> Clone for HashableMessage<T> {
    fn clone(&self) -> Self {
        HashableMessage{
            message: self.message.clone(),
            hash: self.hash,
        }
    }
}

impl<T: Hash> Debug for HashableMessage<T> {
    fn fmt(&self, f: &mut Formatter) -> Result {
        write!(f, "Message(hash={})", self.hash)
    }
}
