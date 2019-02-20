use network::nightshade_protocol::{start_peers};

fn run_alphanet(num_authorities: usize) {
    // 1. Initialize all authorities and let them discover themselves on the network
    start_peers(num_authorities);

    // 2. Initialize nightshade task for all authorities. Create proposals.
    //    Everybody knows from the beginning others public keys

    // 3. Start protocol. Connect consensus channels with network channels. Encode + Decode messages/gossips
}


#[cfg(test)]
mod tests {
    use crate::run_alphanet;

    #[test]
    fn test_alphanet(){
        run_alphanet(4);
    }
}