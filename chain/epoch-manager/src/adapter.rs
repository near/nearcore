/// A trait that abstracts the interface of the EpochManager.
///
/// It is intended to be an intermediate state in a refactor: we want to remove
/// epoch manager stuff from RuntimeAdapter's interface, and, as a first step,
/// we move it to a new trait. The end goal is for the code to use the concrete
/// epoch manager type directly. Though, we might want to still keep this trait
/// in, to allow for easy overriding of epoch manager in tests.
pub trait EpochManagerAdapter: Send + Sync {

}
