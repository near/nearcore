/// Validator schedule describes how block and chunk producers are selected by
/// the KeyValue runtime.
///
/// In the real runtime, we use complex algorithm based on randomness and stake
/// to select the validators. For for tests though, we just want to select them
/// by fiat.
pub struct ValidatorSchedule {

}
