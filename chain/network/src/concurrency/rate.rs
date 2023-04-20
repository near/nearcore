/// Config of a rate limiter algorithm, which behaves like a semaphore
/// - with maximal capacity `burst`
/// - with a new ticket added automatically every 1/qps seconds (qps stands for "queries per
///   second")
/// In case of large load, semaphore will be empty most of the time,
/// letting through requests at frequency `qps`.
/// In case a number of requests come after a period of inactivity, semaphore will immediately
/// let through up to `burst` requests, before going into the previous mode.
#[derive(Copy, Clone)]
pub struct Limit {
    pub burst: u64,
    pub qps: f64,
}

impl Limit {
    // TODO(gprusak): consider having a constructor for RateLimit which enforces validation
    // and getters for fields, so that they cannot be modified after construction.
    pub fn validate(&self) -> anyhow::Result<()> {
        if self.qps <= 0. {
            anyhow::bail!("qps has to be >0");
        }
        if self.burst == 0 {
            anyhow::bail!("burst has to be >0");
        }
        Ok(())
    }
}
