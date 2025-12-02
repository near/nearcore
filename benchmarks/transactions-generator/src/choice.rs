pub(crate) struct TxAccountsSelector {
    sender: ZipfMandelbrot,
    receiver: ZipfMandelbrot,
    receivers_from_senders_ratio: f64,
}

pub(crate) enum FromSendersOrReceivers {
    FromSenders(usize),
    FromReceivers(usize),
}

pub(crate) struct Choice {
    pub sender_idx: usize,
    pub receiver_idx: FromSendersOrReceivers,
}

impl TxAccountsSelector {
    pub(crate) fn new(
        num_accounts: usize,
        num_receiver_accounts: usize,
        sender_skew: f64,
        receiver_skew: f64,
        receivers_from_senders_ratio: f64,
    ) -> Self {
        let q = 2.7;
        tracing::info!(receivers_from_senders_ratio, "Created TxAccountsSelector");
        Self {
            sender: ZipfMandelbrot::new(num_accounts - 1, sender_skew, q),
            receiver: ZipfMandelbrot::new(num_receiver_accounts - 1, receiver_skew, q),
            receivers_from_senders_ratio,
        }
    }

    pub(crate) fn sample<R: rand::Rng + ?Sized>(&self, rng: &mut R) -> Choice {
        let sender_idx = self.sender.sample(rng) as usize;
        if rng.gen_range(0. ..1.0) < self.receivers_from_senders_ratio {
            // choose receiver from senders
            let mut receiver_idx = self.sender.sample(rng) as usize;
            while receiver_idx == sender_idx {
                receiver_idx = self.sender.sample(rng) as usize;
            }
            return Choice {
                sender_idx,
                receiver_idx: FromSendersOrReceivers::FromSenders(receiver_idx),
            };
        }
        let receiver_idx = self.receiver.sample(rng) as usize;

        return Choice {
            sender_idx,
            receiver_idx: FromSendersOrReceivers::FromReceivers(receiver_idx),
        };
    }
}

/// Struct for sampling from a Zipf-Mandelbrot distribution
pub struct ZipfMandelbrot {
    cdf: Vec<f64>, // cumulative distribution function
}

impl ZipfMandelbrot {
    /// Create a Zipf-Mandelbrot sampler
    /// n: number of ranks, s: exponent, q: shift
    pub fn new(n: usize, s: f64, q: f64) -> Self {
        let mut cdf = Vec::with_capacity(n);
        let mut sum = 0.0;
        for k in 0..n {
            // k is 0-indexed
            sum += 1.0 / ((k as f64 + 1.0 + q).powf(s)); // still add 1 because Zipf formula uses 1..N
            cdf.push(sum);
        }
        // Normalize
        for p in &mut cdf {
            *p /= sum;
        }
        Self { cdf }
    }

    /// Sample a rank 0..=N-1 from the distribution
    pub fn sample<R: rand::Rng + ?Sized>(&self, rng: &mut R) -> usize {
        let x: f64 = rng.r#gen(); // uniform [0,1)
        match self.cdf.binary_search_by(|p| p.partial_cmp(&x).unwrap()) {
            Ok(idx) => idx,
            Err(idx) => idx,
        }
    }
}
