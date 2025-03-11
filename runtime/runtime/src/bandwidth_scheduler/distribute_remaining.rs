use near_primitives::bandwidth_scheduler::Bandwidth;
use near_primitives::shard_layout::ShardLayout;
use near_primitives::types::ShardIndex;

use super::scheduler::{ShardIndexMap, ShardLink, ShardLinkMap};

/// After bandwidth scheduler processes all of the bandwidth requests, there's usually some leftover
/// budget for sending and receiving data between shards. This function is responsible for
/// distributing the remaining bandwidth in a fair manner. It looks at how much more each shard
/// could send/receive and grants bandwidth on links to fully utilize the available bandwidth.
/// The `is_link_allowed_map` argument makes it possible to disallow granting bandwidth on some
/// links. This usually happens when a shard is fully congested and is allowed to receive receipts
/// only from the allowed sender shard.
/// The algorithm processes senders and receivers in the order of increasing budget. It grants a bit
/// of bandwidth, keeping in mind that others will also want to send some data there. Processing
/// them in this order gives the guarantee that all senders processed later will send at least as
/// much as the one being processed right now. This means that we can grant `remaining_bandwidth /
/// remaining_senders` and be sure that utilization will be high.
/// The algorithm is safe because it never grants more than `remaining_bandwidth`, which ensures
/// that the grants stay under the budget.
/// The algorithm isn't ideal, the utilization can be a bit lower when there are a lot of disallowed
/// links, but it's good enough for bandwidth scheduler.
pub fn distribute_remaining_bandwidth(
    sender_budgets: &ShardIndexMap<Bandwidth>,
    receiver_budgets: &ShardIndexMap<Bandwidth>,
    is_link_allowed: &ShardLinkMap<bool>,
    shard_layout: &ShardLayout,
) -> ShardLinkMap<Bandwidth> {
    let mut sender_infos: ShardIndexMap<EndpointInfo> = ShardIndexMap::new(shard_layout);
    let mut receiver_infos: ShardIndexMap<EndpointInfo> = ShardIndexMap::new(shard_layout);

    for shard_index in shard_layout.shard_indexes() {
        let sender_budget = sender_budgets.get(&shard_index).copied().unwrap_or(0);
        sender_infos
            .insert(shard_index, EndpointInfo { links_num: 0, bandwidth_left: sender_budget });

        let receiver_budget = receiver_budgets.get(&shard_index).copied().unwrap_or(0);
        receiver_infos
            .insert(shard_index, EndpointInfo { links_num: 0, bandwidth_left: receiver_budget });
    }

    for sender in shard_layout.shard_indexes() {
        for receiver in shard_layout.shard_indexes() {
            if *is_link_allowed.get(&ShardLink::new(sender, receiver)).unwrap_or(&false) {
                sender_infos.get_mut(&sender).unwrap().links_num += 1;
                receiver_infos.get_mut(&receiver).unwrap().links_num += 1;
            }
        }
    }

    let mut senders_by_avg_link_bandwidth: Vec<ShardIndex> = shard_layout.shard_indexes().collect();
    senders_by_avg_link_bandwidth
        .sort_by_key(|shard| sender_infos.get(shard).unwrap().average_link_bandwidth());

    let mut receivers_by_avg_link_bandwidth: Vec<ShardIndex> =
        shard_layout.shard_indexes().collect();
    receivers_by_avg_link_bandwidth
        .sort_by_key(|shard| receiver_infos.get(shard).unwrap().average_link_bandwidth());

    let mut bandwidth_grants: ShardLinkMap<Bandwidth> = ShardLinkMap::new(shard_layout);
    for sender in senders_by_avg_link_bandwidth {
        let sender_info = sender_infos.get_mut(&sender).unwrap();
        for &receiver in &receivers_by_avg_link_bandwidth {
            if !*is_link_allowed.get(&ShardLink::new(sender, receiver)).unwrap_or(&false) {
                continue;
            }

            let receiver_info = receiver_infos.get_mut(&receiver).unwrap();

            if sender_info.links_num == 0 || receiver_info.links_num == 0 {
                break;
            }

            let sender_proposition = sender_info.link_proposition();
            let receiver_proposition = receiver_info.link_proposition();
            let granted_bandwidth = std::cmp::min(sender_proposition, receiver_proposition);
            bandwidth_grants.insert(ShardLink::new(sender, receiver), granted_bandwidth);

            sender_info.bandwidth_left -= granted_bandwidth;
            sender_info.links_num -= 1;

            receiver_info.bandwidth_left -= granted_bandwidth;
            receiver_info.links_num -= 1;
        }
    }

    bandwidth_grants
}

/// Information about sender or receiver shard, used in `distribute_remaining_bandwidth`
struct EndpointInfo {
    /// How much more bandwidth can be sent/received
    bandwidth_left: Bandwidth,
    /// How many more links the bandwidth will be granted on
    links_num: u64,
}

impl EndpointInfo {
    /// How much can be sent on every link on average
    fn average_link_bandwidth(&self) -> Bandwidth {
        if self.links_num == 0 {
            return 0;
        }
        self.bandwidth_left / self.links_num
    }

    /// Propose amount of bandwidth to grant on the next link.
    /// Both sides of the link propose something and the minimum of the two is granted on the link.
    fn link_proposition(&self) -> Bandwidth {
        self.bandwidth_left / self.links_num
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use near_primitives::bandwidth_scheduler::Bandwidth;
    use near_primitives::shard_layout::ShardLayout;
    use near_primitives::types::ShardIndex;
    use rand::{Rng, SeedableRng};
    use rand_chacha::ChaCha20Rng;

    use crate::bandwidth_scheduler::distribute_remaining::distribute_remaining_bandwidth;
    use crate::bandwidth_scheduler::scheduler::{ShardIndexMap, ShardLink, ShardLinkMap};
    use testlib::bandwidth_scheduler::estimate_link_throughputs;

    fn run_distribute_remaining(
        sender_budgets: &[Bandwidth],
        receiver_budgets: &[Bandwidth],
        allowed_links: AllowedLinks,
    ) -> ShardLinkMap<Bandwidth> {
        assert_eq!(sender_budgets.len(), receiver_budgets.len());
        let shard_layout = ShardLayout::multi_shard(sender_budgets.len().try_into().unwrap(), 0);
        let mut sender_budgets_map = ShardIndexMap::new(&shard_layout);
        for (i, sender_budget) in sender_budgets.iter().enumerate() {
            sender_budgets_map.insert(i, *sender_budget);
        }
        let mut receiver_budgets_map = ShardIndexMap::new(&shard_layout);
        for (i, receiver_budget) in receiver_budgets.iter().enumerate() {
            receiver_budgets_map.insert(i, *receiver_budget);
        }

        let mut is_link_allowed_map = ShardLinkMap::new(&shard_layout);
        let default_allowed = match &allowed_links {
            AllowedLinks::AllAllowed => true,
            AllowedLinks::AllowedList(_) | AllowedLinks::NoneAllowed => false,
        };
        for sender_index in shard_layout.shard_indexes() {
            for receiver_index in shard_layout.shard_indexes() {
                is_link_allowed_map
                    .insert(ShardLink::new(sender_index, receiver_index), default_allowed);
            }
        }
        if let AllowedLinks::AllowedList(allowed_links_list) = allowed_links {
            for (sender_index, receiver_index) in allowed_links_list {
                is_link_allowed_map.insert(ShardLink::new(sender_index, receiver_index), true);
            }
        }

        distribute_remaining_bandwidth(
            &sender_budgets_map,
            &receiver_budgets_map,
            &is_link_allowed_map,
            &shard_layout,
        )
    }

    /// Convenient way to specify is_link_allowed_map in the tests
    enum AllowedLinks {
        /// All links are allowed
        AllAllowed,
        /// All links are forbidden
        NoneAllowed,
        /// Only the links in the list are allowed
        AllowedList(Vec<(ShardIndex, ShardIndex)>),
    }

    fn assert_grants(
        granted: &ShardLinkMap<Bandwidth>,
        expected: &[(ShardIndex, ShardIndex, Bandwidth)],
    ) {
        let mut granted_map: BTreeMap<(ShardIndex, ShardIndex), Bandwidth> = BTreeMap::new();
        for sender in 0..granted.num_indexes() {
            for receiver in 0..granted.num_indexes() {
                if let Some(grant) = granted.get(&ShardLink::new(sender, receiver)) {
                    granted_map.insert((sender, receiver), *grant);
                }
            }
        }

        let expected_map: BTreeMap<(ShardIndex, ShardIndex), Bandwidth> = expected
            .iter()
            .map(|(sender, receiver, grant)| ((*sender, *receiver), *grant))
            .collect();

        assert_eq!(granted_map, expected_map);
    }

    /// A single link, sender can send less than the receiver can receive.
    #[test]
    fn test_one_link() {
        let granted = run_distribute_remaining(&[50], &[100], AllowedLinks::AllAllowed);
        assert_grants(&granted, &[(0, 0, 50)]);
    }

    /// A single link which is not allowed. No bandwidth should be granted.
    #[test]
    fn test_one_link_not_allowed() {
        let granted = run_distribute_remaining(&[50], &[100], AllowedLinks::NoneAllowed);
        assert_grants(&granted, &[]);
    }

    /// Three shards, all links are allowed.
    /// Bandwidth should be distributed equally.
    #[test]
    fn test_three_shards() {
        let granted =
            run_distribute_remaining(&[300, 300, 300], &[300, 300, 300], AllowedLinks::AllAllowed);
        assert_grants(
            &granted,
            &[
                (0, 0, 100),
                (0, 1, 100),
                (0, 2, 100),
                (1, 0, 100),
                (1, 1, 100),
                (1, 2, 100),
                (2, 0, 100),
                (2, 1, 100),
                (2, 2, 100),
            ],
        );
    }

    /// (1) can send to (0) and (2)
    /// (0) and (2) can send to (1)
    /// Each active link should get half of the available budget.
    #[test]
    fn test_two_to_one() {
        let allowed_links = AllowedLinks::AllowedList(vec![(1, 0), (1, 2), (0, 1), (2, 1)]);
        let granted = run_distribute_remaining(&[100, 100, 100], &[100, 100, 100], allowed_links);
        assert_grants(&granted, &[(0, 1, 50), (1, 0, 50), (1, 2, 50), (2, 1, 50)]);
    }

    /// Three shards, two of them are fully congested.
    /// (1) can only receive receipts from (0)
    /// (2) can only receive receipts from (1)
    /// (0) can receive receipts from all shards.
    #[test]
    fn test_two_fully_congested() {
        let allowed_links = AllowedLinks::AllowedList(vec![(0, 0), (0, 1), (1, 0), (1, 2), (2, 0)]);
        let granted = run_distribute_remaining(&[300, 300, 300], &[300, 300, 300], allowed_links);
        assert_grants(&granted, &[(0, 0, 100), (0, 1, 200), (1, 0, 100), (1, 2, 200), (2, 0, 100)]);
    }

    /// Run `distribute_remaining_bandwidth` on a random test scenario
    fn randomized_test(seed: u64) {
        println!("\n\n# Test with seed {}", seed);
        let mut rng = ChaCha20Rng::seed_from_u64(seed);
        let num_shards = rng.gen_range(1..10);
        let all_links_allowed = rng.gen_bool(0.5);
        println!("num_shards: {}", num_shards);
        println!("all_links_allowed: {}", all_links_allowed);

        let mut active_links: BTreeSet<(ShardIndex, ShardIndex)> = BTreeSet::new();
        for sender in 0..num_shards {
            for receiver in 0..num_shards {
                if !all_links_allowed && rng.gen_bool(0.5) {
                    continue;
                }
                active_links.insert((sender, receiver));
            }
        }

        println!("active_links: {:?}", active_links);

        fn generate_budget(rng: &mut ChaCha20Rng) -> Bandwidth {
            if rng.gen_bool(0.1) { 0 } else { rng.gen_range(0..1000) }
        }
        let sender_budgets: Vec<Bandwidth> =
            (0..num_shards).map(|_| generate_budget(&mut rng)).collect();
        let receiver_budgets: Vec<Bandwidth> =
            (0..num_shards).map(|_| generate_budget(&mut rng)).collect();
        println!("sender_budgets: {:?}", sender_budgets);
        println!("receiver_budgets: {:?}", receiver_budgets);

        let allowed_links = AllowedLinks::AllowedList(active_links.iter().copied().collect());
        let grants = run_distribute_remaining(&sender_budgets, &receiver_budgets, allowed_links);

        let mut total_incoming = vec![0; num_shards];
        let mut total_outgoing = vec![0; num_shards];
        let mut total_throughput = 0;

        for sender in 0..num_shards {
            for receiver in 0..num_shards {
                if let Some(grant) = grants.get(&ShardLink::new(sender, receiver)) {
                    total_outgoing[sender] += grant;
                    total_incoming[receiver] += grant;
                    total_throughput += grant;
                }
            }
        }

        // Assert that granted bandwidth doesn't exceed sender or receiver budgets.
        for i in 0..num_shards {
            assert!(total_outgoing[i] <= sender_budgets[i]);
            assert!(total_incoming[i] <= receiver_budgets[i]);
        }

        // Make sure that bandwidth utilization is high
        dbg!(total_throughput);
        if all_links_allowed {
            // When all links are allowed the algorithm achieves 99% bandwidth utilization.
            let total_sending_budget: u64 = sender_budgets.iter().sum();
            let total_receiver_budget: u64 = receiver_budgets.iter().sum();
            let theoretical_throughput = std::cmp::min(total_sending_budget, total_receiver_budget);
            dbg!(theoretical_throughput);
            assert!(total_throughput >= theoretical_throughput * 99 / 100);
        } else {
            // When some links are not allowed, the algorithm achieves good enough bandwidth utilization.
            // (> 75% of estimated possible throughput).
            let estimated_link_throughputs =
                estimate_link_throughputs(&active_links, &sender_budgets, &receiver_budgets);
            let estimated_total_throughput: Bandwidth = estimated_link_throughputs
                .iter()
                .map(|(_link, throughput)| throughput.as_u64())
                .sum();
            dbg!(estimated_total_throughput);
            assert!(total_throughput >= estimated_total_throughput * 75 / 100);
        }

        // Ensure that bandwidth is not granted on forbidden links
        for sender in 0..num_shards {
            for receiver in 0..num_shards {
                let granted = *grants.get(&ShardLink::new(sender, receiver)).unwrap_or(&0);

                if !active_links.contains(&(sender, receiver)) {
                    assert_eq!(granted, 0);
                }
            }
        }
    }

    #[test]
    fn test_randomized() {
        for i in 0..1000 {
            randomized_test(i);
        }
    }
}
