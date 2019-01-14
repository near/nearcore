#[cfg(test)]
mod tests {
    use crate::dag::message::Message;
    use primitives::traits::{Payload, WitnessSelector};
    use primitives::types::UID;
    use std::collections::HashSet;
    use typed_arena::Arena;

    struct FakeNonContinuousWitnessSelector {
        num_users: u64,
        users: HashSet<UID>,
    }

    impl FakeNonContinuousWitnessSelector {
        fn new(num_users: u64) -> Self {
            let mut users = set! {0};
            for i in 1..num_users {
                users.insert(i);
            }
            Self { num_users, users }
        }
    }

    impl WitnessSelector for FakeNonContinuousWitnessSelector {
        fn epoch_witnesses(&self, _epoch: u64) -> &HashSet<UID> {
            &self.users
        }
        fn epoch_leader(&self, epoch: u64) -> UID {
            epoch % self.num_users
        }
        fn random_witnesses(&self, _epoch: u64, _sample_size: usize) -> HashSet<UID> {
            unimplemented!()
        }
    }

    fn make_assertions<P: Payload>(
        messages: &[Option<&Message<P>>],
        assertions: &[(u64, Option<u64>, bool, u64)],
    ) {
        for it in messages.iter().zip(assertions.iter()) {
            let (msg, a) = it;
            if let Some(msg) = msg.as_ref() {
                // If this assert triggers, the last element of tuples indicates the node uid
                assert_eq!(
                    (a.0, a.1, a.2, a.3),
                    (
                        msg.computed_epoch,
                        msg.computed_is_representative,
                        msg.computed_is_kickout,
                        a.3
                    )
                );
            }
        }
    }

    fn epoch_representative_approved_by<P: Payload>(
        message: &Message<P>,
        epoch: u64,
        owner: u64,
    ) -> bool {
        message.approved_endorsements.contains_any_approval(epoch, owner)
            || (owner == message.data.body.owner_uid
                && message.computed_endorsements.contains_epoch(epoch))
            || (owner == message.data.body.owner_uid
                && Some(epoch) == message.computed_is_representative)
    }

    fn test_endorsements<P: Payload>(
        message: &Message<P>,
        endorsements: &[Vec<u64>],
        num_users: u64,
    ) {
        for epoch in 0..endorsements.len() {
            let who = &endorsements[epoch];
            let mut lst = 0;

            for i in 0..num_users {
                if lst < who.len() && who[lst] == i {
                    assert!(
                        epoch_representative_approved_by(message, epoch as u64, i),
                        "epoch: {}, i: {}, {:?}",
                        epoch,
                        i,
                        message.computed_endorsements
                    );
                    lst += 1;
                } else {
                    assert!(
                        !epoch_representative_approved_by(message, epoch as u64, i),
                        "epoch: {}, i: {}",
                        epoch,
                        i
                    );
                }
            }
        }
    }

    #[test]
    fn generated_just_one_msg() {
        /* Just one message from Alice that is representative and endorses itself. */

        /* {"s":[{"owner":0,"parents":[]}],"n":4,"c":"Just one message from Alice that is representative and endorses itself.","f":"just_one_msg"} */
        let arena = Arena::new();
        let selector = FakeNonContinuousWitnessSelector::new(4);
        let v0;
        let mut v = [None; 1];
        let test = |v: &[_]| make_assertions(&v, &[(0, Some(0), false, 0)]);
        simple_messages!(0, &selector, arena [0, 0, true => v0;]);
        v[0] = Some(v0);
        test(&v);
        test_endorsements(v0, &[vec![0]], 4);
    }

    #[test]
    #[should_panic] // https://github.com/nearprotocol/nearcore/issues/99
    fn generated_bob_second_msg_in_kickout_epoch() {
        /* In this test Bob has another message in the same epoch in which it already has a kickout. This second message shall not be a kickout. */

        /* {"s":[{"owner":1,"parents":[]},{"owner":2,"parents":[]},{"owner":3,"parents":[]},{"owner":1,"parents":[0,1,2]},{"owner":1,"parents":[3]}],"n":4,"c":"In this test Bob has another message in the same epoch in which it already has a kickout. This second message shall not be a kickout.","f":"bob_second_msg_in_kickout_epoch"} */
        let arena = Arena::new();
        let selector = FakeNonContinuousWitnessSelector::new(4);
        let (v0, v3, v4, v5, v6);
        let mut v = [None; 5];
        let test = |v: &[_]| {
            make_assertions(
                &v,
                &[
                    (0, None, false, 0),
                    (0, None, false, 3),
                    (0, None, false, 4),
                    (1, None, true, 5),
                    (1, None, false, 6),
                ],
            )
        };
        simple_messages!(0, &selector, arena [1, 0, true => v0;]);
        v[0] = Some(v0);
        test(&v);
        test_endorsements(v0, &[], 4);
        simple_messages!(0, &selector, arena [2, 0, true => v3;]);
        v[1] = Some(v3);
        test(&v);
        test_endorsements(v3, &[], 4);
        simple_messages!(0, &selector, arena [3, 0, true => v4;]);
        v[2] = Some(v4);
        test(&v);
        test_endorsements(v4, &[], 4);
        simple_messages!(0, &selector, arena [[=> v0; => v3; => v4; ] => 1, 0, true => v5;]);
        v[3] = Some(v5);
        test(&v);
        test_endorsements(v5, &[], 4);
        simple_messages!(0, &selector, arena [[=> v5; ] => 1, 0, true => v6;]);
        v[4] = Some(v6);
        test(&v);
        test_endorsements(v6, &[], 4);
    }

    #[test]
    fn generated_simple_test() {
        /* Simple test with two representative messages and no kickouts */

        /* {"s":[{"owner":0,"parents":[]},{"owner":1,"parents":[]},{"owner":2,"parents":[0,1]},{"owner":1,"parents":[2]}],"n":4,"c":"Simple test with two representative messages and no kickouts","f":"simple_test"} */
        let arena = Arena::new();
        let selector = FakeNonContinuousWitnessSelector::new(4);
        let (v0, v1, v2, v3);
        let mut v = [None; 4];
        let test = |v: &[_]| {
            make_assertions(
                &v,
                &[
                    (0, Some(0), false, 0),
                    (0, None, false, 1),
                    (0, None, false, 2),
                    (1, Some(1), false, 3),
                ],
            )
        };
        simple_messages!(0, &selector, arena [0, 0, true => v0;]);
        v[0] = Some(v0);
        test(&v);
        test_endorsements(v0, &[vec![0]], 4);
        simple_messages!(0, &selector, arena [1, 0, true => v1;]);
        v[1] = Some(v1);
        test(&v);
        test_endorsements(v1, &[], 4);
        simple_messages!(0, &selector, arena [[=> v0; => v1; ] => 2, 0, true => v2;]);
        v[2] = Some(v2);
        test(&v);
        test_endorsements(v2, &[vec![0, 2]], 4);
        simple_messages!(0, &selector, arena [[=> v2; ] => 1, 0, true => v3;]);
        v[3] = Some(v3);
        test(&v);
        test_endorsements(v3, &[vec![0, 1, 2], vec![1]], 4);
    }

    #[test]
    #[should_panic] // https://github.com/nearprotocol/nearcore/issues/99
    fn generated_bob_fork() {
        /* Representative message fork from Bob, testing endorsements */

        /* {"s":[{"owner":1,"parents":[]},{"owner":2,"parents":[0]},{"owner":3,"parents":[1]},{"owner":0,"parents":[2]},{"owner":1,"parents":[3]},{"owner":1,"parents":[3]},{"owner":2,"parents":[4]},{"owner":0,"parents":[4,5]},{"owner":3,"parents":[5]},{"owner":2,"parents":[5,6]}],"n":4,"c":"Representative message fork from Bob, testing endorsements","f":"bob_fork"} */
        let arena = Arena::new();
        let selector = FakeNonContinuousWitnessSelector::new(4);
        let (v0, v1, v2, v3, v4, v5, v6, v7, v8, v9);
        let mut v = [None; 10];
        let test = |v: &[_]| {
            make_assertions(
                &v,
                &[
                    (0, None, false, 0),
                    (0, None, false, 1),
                    (0, None, false, 2),
                    (0, Some(0), false, 3),
                    (1, Some(1), false, 4),
                    (1, Some(1), false, 5),
                    (1, None, false, 6),
                    (1, None, false, 7),
                    (1, None, false, 8),
                    (1, None, false, 9),
                ],
            )
        };
        simple_messages!(0, &selector, arena [1, 0, true => v0;]);
        v[0] = Some(v0);
        test(&v);
        test_endorsements(v0, &[], 4);
        simple_messages!(0, &selector, arena [[=> v0; ] => 2, 0, true => v1;]);
        v[1] = Some(v1);
        test(&v);
        test_endorsements(v1, &[], 4);
        simple_messages!(0, &selector, arena [[=> v1; ] => 3, 0, true => v2;]);
        v[2] = Some(v2);
        test(&v);
        test_endorsements(v2, &[], 4);
        simple_messages!(0, &selector, arena [[=> v2; ] => 0, 0, true => v3;]);
        v[3] = Some(v3);
        test(&v);
        test_endorsements(v3, &[vec![0]], 4);
        simple_messages!(0, &selector, arena [[=> v3; ] => 1, 0, true => v4;]);
        v[4] = Some(v4);
        test(&v);
        test_endorsements(v4, &[vec![0, 1], vec![1]], 4);
        simple_messages!(0, &selector, arena [[=> v3; ] => 1, 0, true => v5;]);
        v[5] = Some(v5);
        test(&v);
        test_endorsements(v5, &[vec![0, 1], vec![1]], 4);
        simple_messages!(0, &selector, arena [[=> v4; ] => 2, 0, true => v6;]);
        v[6] = Some(v6);
        test(&v);
        test_endorsements(v6, &[vec![0, 1, 2], vec![1, 2]], 4);
        simple_messages!(0, &selector, arena [[=> v4; => v5; ] => 0, 0, true => v7;]);
        v[7] = Some(v7);
        test(&v);
        test_endorsements(v7, &[vec![0, 1], vec![1]], 4);
        simple_messages!(0, &selector, arena [[=> v5; ] => 3, 0, true => v8;]);
        v[8] = Some(v8);
        test(&v);
        test_endorsements(v8, &[vec![0, 1, 3], vec![1, 3]], 4);
        simple_messages!(0, &selector, arena [[=> v5; => v6; ] => 2, 0, true => v9;]);
        v[9] = Some(v9);
        test(&v);
        test_endorsements(v9, &[vec![0, 1, 2], vec![1, 2]], 4);
    }

    #[test]
    fn generated_used_to_panic() {
        /* This used to panic */

        /* {"s":[{"owner":1,"parents":[]},{"owner":0,"parents":[0]},{"owner":2,"parents":[1]},{"owner":3,"parents":[2]},{"owner":1,"parents":[2]},{"owner":2,"parents":[3,4]}],"n":4,"c":"This used to panic","f":"used_to_panic"} */
        let arena = Arena::new();
        let selector = FakeNonContinuousWitnessSelector::new(4);
        let (v0, v1, v2, v3, v4, v5);
        let mut v = [None; 6];
        let test = |v: &[_]| {
            make_assertions(
                &v,
                &[
                    (0, None, false, 0),
                    (0, Some(0), false, 1),
                    (0, None, false, 2),
                    (0, None, false, 3),
                    (1, Some(1), false, 4),
                    (1, None, false, 5),
                ],
            )
        };
        simple_messages!(0, &selector, arena [1, 0, true => v0;]);
        v[0] = Some(v0);
        test(&v);
        test_endorsements(v0, &[], 4);
        simple_messages!(0, &selector, arena [[=> v0; ] => 0, 0, true => v1;]);
        v[1] = Some(v1);
        test(&v);
        test_endorsements(v1, &[vec![0]], 4);
        simple_messages!(0, &selector, arena [[=> v1; ] => 2, 0, true => v2;]);
        v[2] = Some(v2);
        test(&v);
        test_endorsements(v2, &[vec![0, 2]], 4);
        simple_messages!(0, &selector, arena [[=> v2; ] => 3, 0, true => v3;]);
        v[3] = Some(v3);
        test(&v);
        test_endorsements(v3, &[vec![0, 2, 3]], 4);
        simple_messages!(0, &selector, arena [[=> v2; ] => 1, 0, true => v4;]);
        v[4] = Some(v4);
        test(&v);
        test_endorsements(v4, &[vec![0, 1, 2], vec![1]], 4);
        simple_messages!(0, &selector, arena [[=> v3; => v4; ] => 2, 0, true => v5;]);
        v[5] = Some(v5);
        test(&v);
        test_endorsements(v5, &[vec![0, 1, 2, 3], vec![1, 2]], 4);
    }

    #[test]
    fn generated_kickouts() {
        /* Some kickout messages */

        /* {"s":[{"owner":1,"parents":[]},{"owner":0,"parents":[0]},{"owner":2,"parents":[1]},{"owner":3,"parents":[2]},{"owner":1,"parents":[2]},{"owner":2,"parents":[3,4]},{"owner":0,"parents":[4]},{"owner":1,"parents":[5]},{"owner":0,"parents":[6,7]},{"owner":3,"parents":[8]},{"owner":3,"parents":[9]},{"owner":1,"parents":[10]},{"owner":3,"parents":[11]},{"owner":2,"parents":[11]},{"owner":3,"parents":[12,13]},{"owner":1,"parents":[12,13]},{"owner":2,"parents":[15]},{"owner":1,"parents":[14,16]},{"owner":2,"parents":[14,16]},{"owner":3,"parents":[14,16]},{"owner":1,"parents":[17,18,19]},{"owner":2,"parents":[20]},{"owner":3,"parents":[20]},{"owner":1,"parents":[21,22]}],"n":4,"c":"Some kickout messages","f":"kickouts"} */
        let arena = Arena::new();
        let selector = FakeNonContinuousWitnessSelector::new(4);
        let (
            v0,
            v1,
            v2,
            v3,
            v4,
            v5,
            v6,
            v7,
            v8,
            v9,
            v10,
            v11,
            v12,
            v13,
            v14,
            v15,
            v16,
            v17,
            v18,
            v19,
            v20,
            v21,
            v22,
            v23,
        );
        let mut v = [None; 24];
        let test = |v: &[_]| {
            make_assertions(
                &v,
                &[
                    (0, None, false, 0),
                    (0, Some(0), false, 1),
                    (0, None, false, 2),
                    (0, None, false, 3),
                    (1, Some(1), false, 4),
                    (1, None, false, 5),
                    (1, None, false, 6),
                    (1, None, false, 7),
                    (2, None, false, 8),
                    (1, None, false, 9),
                    (2, None, false, 10),
                    (2, None, false, 11),
                    (3, None, true, 12),
                    (2, Some(2), false, 13),
                    (3, Some(3), false, 14),
                    (3, None, false, 15),
                    (3, None, false, 16),
                    (4, None, false, 17),
                    (4, None, false, 18),
                    (4, None, false, 19),
                    (5, None, true, 20),
                    (5, None, false, 21),
                    (5, None, false, 22),
                    (6, Some(5), false, 23),
                ],
            )
        };
        simple_messages!(0, &selector, arena [1, 0, true => v0;]);
        v[0] = Some(v0);
        test(&v);
        test_endorsements(v0, &[], 4);
        simple_messages!(0, &selector, arena [[=> v0; ] => 0, 0, true => v1;]);
        v[1] = Some(v1);
        test(&v);
        test_endorsements(v1, &[vec![0]], 4);
        simple_messages!(0, &selector, arena [[=> v1; ] => 2, 0, true => v2;]);
        v[2] = Some(v2);
        test(&v);
        test_endorsements(v2, &[vec![0, 2]], 4);
        simple_messages!(0, &selector, arena [[=> v2; ] => 3, 0, true => v3;]);
        v[3] = Some(v3);
        test(&v);
        test_endorsements(v3, &[vec![0, 2, 3]], 4);
        simple_messages!(0, &selector, arena [[=> v2; ] => 1, 0, true => v4;]);
        v[4] = Some(v4);
        test(&v);
        test_endorsements(v4, &[vec![0, 1, 2], vec![1]], 4);
        simple_messages!(0, &selector, arena [[=> v3; => v4; ] => 2, 0, true => v5;]);
        v[5] = Some(v5);
        test(&v);
        test_endorsements(v5, &[vec![0, 1, 2, 3], vec![1, 2]], 4);
        simple_messages!(0, &selector, arena [[=> v4; ] => 0, 0, true => v6;]);
        v[6] = Some(v6);
        test(&v);
        test_endorsements(v6, &[vec![0, 1, 2], vec![0, 1]], 4);
        simple_messages!(0, &selector, arena [[=> v5; ] => 1, 0, true => v7;]);
        v[7] = Some(v7);
        test(&v);
        test_endorsements(v7, &[vec![0, 1, 2, 3], vec![1, 2]], 4);
        simple_messages!(0, &selector, arena [[=> v6; => v7; ] => 0, 0, true => v8;]);
        v[8] = Some(v8);
        test(&v);
        test_endorsements(v8, &[vec![0, 1, 2, 3], vec![0, 1, 2]], 4);
        simple_messages!(0, &selector, arena [[=> v8; ] => 3, 0, true => v9;]);
        v[9] = Some(v9);
        test(&v);
        test_endorsements(v9, &[vec![0, 1, 2, 3], vec![0, 1, 2, 3]], 4);
        simple_messages!(0, &selector, arena [[=> v9; ] => 3, 0, true => v10;]);
        v[10] = Some(v10);
        test(&v);
        test_endorsements(v10, &[vec![0, 1, 2, 3], vec![0, 1, 2, 3]], 4);
        simple_messages!(0, &selector, arena [[=> v10; ] => 1, 0, true => v11;]);
        v[11] = Some(v11);
        test(&v);
        test_endorsements(v11, &[vec![0, 1, 2, 3], vec![0, 1, 2, 3]], 4);
        simple_messages!(0, &selector, arena [[=> v11; ] => 3, 0, true => v12;]);
        v[12] = Some(v12);
        test(&v);
        test_endorsements(v12, &[vec![0, 1, 2, 3], vec![0, 1, 2, 3]], 4);
        simple_messages!(0, &selector, arena [[=> v11; ] => 2, 0, true => v13;]);
        v[13] = Some(v13);
        test(&v);
        test_endorsements(v13, &[vec![0, 1, 2, 3], vec![0, 1, 2, 3], vec![2]], 4);
        simple_messages!(0, &selector, arena [[=> v12; => v13; ] => 3, 0, true => v14;]);
        v[14] = Some(v14);
        test(&v);
        test_endorsements(v14, &[vec![0, 1, 2, 3], vec![0, 1, 2, 3], vec![2], vec![3]], 4);
        simple_messages!(0, &selector, arena [[=> v12; => v13; ] => 1, 0, true => v15;]);
        v[15] = Some(v15);
        test(&v);
        test_endorsements(v15, &[vec![0, 1, 2, 3], vec![0, 1, 2, 3], vec![1, 2]], 4);
        simple_messages!(0, &selector, arena [[=> v15; ] => 2, 0, true => v16;]);
        v[16] = Some(v16);
        test(&v);
        test_endorsements(v16, &[vec![0, 1, 2, 3], vec![0, 1, 2, 3], vec![1, 2]], 4);
        simple_messages!(0, &selector, arena [[=> v14; => v16; ] => 1, 0, true => v17;]);
        v[17] = Some(v17);
        test(&v);
        test_endorsements(v17, &[vec![0, 1, 2, 3], vec![0, 1, 2, 3], vec![1, 2], vec![1, 3]], 4);
        simple_messages!(0, &selector, arena [[=> v14; => v16; ] => 2, 0, true => v18;]);
        v[18] = Some(v18);
        test(&v);
        test_endorsements(v18, &[vec![0, 1, 2, 3], vec![0, 1, 2, 3], vec![1, 2], vec![2, 3]], 4);
        simple_messages!(0, &selector, arena [[=> v14; => v16; ] => 3, 0, true => v19;]);
        v[19] = Some(v19);
        test(&v);
        test_endorsements(v19, &[vec![0, 1, 2, 3], vec![0, 1, 2, 3], vec![1, 2], vec![3]], 4);
        simple_messages!(0, &selector, arena [[=> v17; => v18; => v19; ] => 1, 0, true => v20;]);
        v[20] = Some(v20);
        test(&v);
        test_endorsements(v20, &[vec![0, 1, 2, 3], vec![0, 1, 2, 3], vec![1, 2], vec![1, 2, 3]], 4);
        simple_messages!(0, &selector, arena [[=> v20; ] => 2, 0, true => v21;]);
        v[21] = Some(v21);
        test(&v);
        test_endorsements(v21, &[vec![0, 1, 2, 3], vec![0, 1, 2, 3], vec![1, 2], vec![1, 2, 3]], 4);
        simple_messages!(0, &selector, arena [[=> v20; ] => 3, 0, true => v22;]);
        v[22] = Some(v22);
        test(&v);
        test_endorsements(v22, &[vec![0, 1, 2, 3], vec![0, 1, 2, 3], vec![1, 2], vec![1, 2, 3]], 4);
        simple_messages!(0, &selector, arena [[=> v21; => v22; ] => 1, 0, true => v23;]);
        v[23] = Some(v23);
        test(&v);
        test_endorsements(
            v23,
            &[vec![0, 1, 2, 3], vec![0, 1, 2, 3], vec![1, 2], vec![1, 2, 3], vec![], vec![1]],
            4,
        );
    }

    #[test]
    fn generated_no_endorse_after_kickout() {
        /* Alice's epoch block is approved after the kickout message, shall not constitute an epoch block */

        /* {"s":[{"owner":0,"parents":[]},{"owner":1,"parents":[]},{"owner":2,"parents":[1]},{"owner":3,"parents":[2]},{"owner":1,"parents":[3]},{"owner":2,"parents":[4]},{"owner":3,"parents":[4]},{"owner":2,"parents":[0,5]},{"owner":3,"parents":[0,6]},{"owner":1,"parents":[7,8]},{"owner":2,"parents":[9]},{"owner":3,"parents":[9]}],"n":4,"c":"Alice's epoch block is approved after the kickout message, shall not constitute an epoch block","f":"no_endorse_after_kickout"} */
        let arena = Arena::new();
        let selector = FakeNonContinuousWitnessSelector::new(4);
        let (v0, v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v11);
        let mut v = [None; 12];
        let test = |v: &[_]| {
            make_assertions(
                &v,
                &[
                    (0, Some(0), false, 0),
                    (0, None, false, 1),
                    (0, None, false, 2),
                    (0, None, false, 3),
                    (1, None, true, 4),
                    (1, None, false, 5),
                    (1, None, false, 6),
                    (1, None, false, 7),
                    (1, None, false, 8),
                    (2, Some(1), false, 9),
                    (2, Some(2), false, 10),
                    (2, None, false, 11),
                ],
            )
        };
        simple_messages!(0, &selector, arena [0, 0, true => v0;]);
        v[0] = Some(v0);
        test(&v);
        test_endorsements(v0, &[vec![0]], 4);
        simple_messages!(0, &selector, arena [1, 0, true => v1;]);
        v[1] = Some(v1);
        test(&v);
        test_endorsements(v1, &[], 4);
        simple_messages!(0, &selector, arena [[=> v1; ] => 2, 0, true => v2;]);
        v[2] = Some(v2);
        test(&v);
        test_endorsements(v2, &[], 4);
        simple_messages!(0, &selector, arena [[=> v2; ] => 3, 0, true => v3;]);
        v[3] = Some(v3);
        test(&v);
        test_endorsements(v3, &[], 4);
        simple_messages!(0, &selector, arena [[=> v3; ] => 1, 0, true => v4;]);
        v[4] = Some(v4);
        test(&v);
        test_endorsements(v4, &[], 4);
        simple_messages!(0, &selector, arena [[=> v4; ] => 2, 0, true => v5;]);
        v[5] = Some(v5);
        test(&v);
        test_endorsements(v5, &[], 4);
        simple_messages!(0, &selector, arena [[=> v4; ] => 3, 0, true => v6;]);
        v[6] = Some(v6);
        test(&v);
        test_endorsements(v6, &[], 4);
        simple_messages!(0, &selector, arena [[=> v0; => v5; ] => 2, 0, true => v7;]);
        v[7] = Some(v7);
        test(&v);
        test_endorsements(v7, &[vec![0]], 4);
        simple_messages!(0, &selector, arena [[=> v0; => v6; ] => 3, 0, true => v8;]);
        v[8] = Some(v8);
        test(&v);
        test_endorsements(v8, &[vec![0]], 4);
        simple_messages!(0, &selector, arena [[=> v7; => v8; ] => 1, 0, true => v9;]);
        v[9] = Some(v9);
        test(&v);
        test_endorsements(v9, &[vec![0], vec![1]], 4);
        simple_messages!(0, &selector, arena [[=> v9; ] => 2, 0, true => v10;]);
        v[10] = Some(v10);
        test(&v);
        test_endorsements(v10, &[vec![0], vec![1, 2], vec![2]], 4);
        simple_messages!(0, &selector, arena [[=> v9; ] => 3, 0, true => v11;]);
        v[11] = Some(v11);
        test(&v);
        test_endorsements(v11, &[vec![0], vec![1, 3]], 4);
    }

    #[test]
    fn generated_kickout_cases() {
        /* If a kickout message and the representative message being kicked out are endorsed simultaneously, it is an endorsement. If the representative message is endorsed later, it is not. */

        /* {"s":[{"owner":0,"parents":[]},{"owner":2,"parents":[0]},{"owner":3,"parents":[0]},{"owner":1,"parents":[1,2]},{"owner":1,"parents":[3]},{"owner":3,"parents":[4]},{"owner":2,"parents":[5]},{"owner":1,"parents":[5]},{"owner":0,"parents":[5]},{"owner":3,"parents":[6,7,8]},{"owner":1,"parents":[6,7,8]},{"owner":0,"parents":[6,7,8]},{"owner":2,"parents":[6,7,8]},{"owner":3,"parents":[9,10,11]},{"owner":1,"parents":[12,13]},{"owner":0,"parents":[13]},{"owner":0,"parents":[12,15]}],"n":4,"c":"If a kickout message and the representative message being kicked out are endorsed simultaneously, it is an endorsement. If the representative message is endorsed later, it is not.","f":"kickout_cases"} */
        let arena = Arena::new();
        let selector = FakeNonContinuousWitnessSelector::new(4);
        let (v0, v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v11, v12, v13, v14, v15, v16);
        let mut v = [None; 17];
        let test = |v: &[_]| {
            make_assertions(
                &v,
                &[
                    (0, Some(0), false, 0),
                    (0, None, false, 1),
                    (0, None, false, 2),
                    (0, None, false, 3),
                    (1, Some(1), false, 4),
                    (1, None, false, 5),
                    (1, None, false, 6),
                    (1, None, false, 7),
                    (1, None, false, 8),
                    (2, None, false, 9),
                    (2, None, false, 10),
                    (2, None, false, 11),
                    (2, Some(2), false, 12),
                    (3, None, true, 13),
                    (3, None, false, 14),
                    (3, None, false, 15),
                    (3, None, false, 16),
                ],
            )
        };
        simple_messages!(0, &selector, arena [0, 0, true => v0;]);
        v[0] = Some(v0);
        test(&v);
        test_endorsements(v0, &[vec![0]], 4);
        simple_messages!(0, &selector, arena [[=> v0; ] => 2, 0, true => v1;]);
        v[1] = Some(v1);
        test(&v);
        test_endorsements(v1, &[vec![0, 2]], 4);
        simple_messages!(0, &selector, arena [[=> v0; ] => 3, 0, true => v2;]);
        v[2] = Some(v2);
        test(&v);
        test_endorsements(v2, &[vec![0, 3]], 4);
        simple_messages!(0, &selector, arena [[=> v1; => v2; ] => 1, 0, true => v3;]);
        v[3] = Some(v3);
        test(&v);
        test_endorsements(v3, &[vec![0, 1, 2, 3]], 4);
        simple_messages!(0, &selector, arena [[=> v3; ] => 1, 0, true => v4;]);
        v[4] = Some(v4);
        test(&v);
        test_endorsements(v4, &[vec![0, 1, 2, 3], vec![1]], 4);
        simple_messages!(0, &selector, arena [[=> v4; ] => 3, 0, true => v5;]);
        v[5] = Some(v5);
        test(&v);
        test_endorsements(v5, &[vec![0, 1, 2, 3], vec![1, 3]], 4);
        simple_messages!(0, &selector, arena [[=> v5; ] => 2, 0, true => v6;]);
        v[6] = Some(v6);
        test(&v);
        test_endorsements(v6, &[vec![0, 1, 2, 3], vec![1, 2, 3]], 4);
        simple_messages!(0, &selector, arena [[=> v5; ] => 1, 0, true => v7;]);
        v[7] = Some(v7);
        test(&v);
        test_endorsements(v7, &[vec![0, 1, 2, 3], vec![1, 3]], 4);
        simple_messages!(0, &selector, arena [[=> v5; ] => 0, 0, true => v8;]);
        v[8] = Some(v8);
        test(&v);
        test_endorsements(v8, &[vec![0, 1, 2, 3], vec![0, 1, 3]], 4);
        simple_messages!(0, &selector, arena [[=> v6; => v7; => v8; ] => 3, 0, true => v9;]);
        v[9] = Some(v9);
        test(&v);
        test_endorsements(v9, &[vec![0, 1, 2, 3], vec![0, 1, 2, 3]], 4);
        simple_messages!(0, &selector, arena [[=> v6; => v7; => v8; ] => 1, 0, true => v10;]);
        v[10] = Some(v10);
        test(&v);
        test_endorsements(v10, &[vec![0, 1, 2, 3], vec![0, 1, 2, 3]], 4);
        simple_messages!(0, &selector, arena [[=> v6; => v7; => v8; ] => 0, 0, true => v11;]);
        v[11] = Some(v11);
        test(&v);
        test_endorsements(v11, &[vec![0, 1, 2, 3], vec![0, 1, 2, 3]], 4);
        simple_messages!(0, &selector, arena [[=> v6; => v7; => v8; ] => 2, 0, true => v12;]);
        v[12] = Some(v12);
        test(&v);
        test_endorsements(v12, &[vec![0, 1, 2, 3], vec![0, 1, 2, 3], vec![2]], 4);
        simple_messages!(0, &selector, arena [[=> v9; => v10; => v11; ] => 3, 0, true => v13;]);
        v[13] = Some(v13);
        test(&v);
        test_endorsements(v13, &[vec![0, 1, 2, 3], vec![0, 1, 2, 3]], 4);
        simple_messages!(0, &selector, arena [[=> v12; => v13; ] => 1, 0, true => v14;]);
        v[14] = Some(v14);
        test(&v);
        test_endorsements(v14, &[vec![0, 1, 2, 3], vec![0, 1, 2, 3], vec![1, 2]], 4);
        simple_messages!(0, &selector, arena [[=> v13; ] => 0, 0, true => v15;]);
        v[15] = Some(v15);
        test(&v);
        test_endorsements(v15, &[vec![0, 1, 2, 3], vec![0, 1, 2, 3]], 4);
        simple_messages!(0, &selector, arena [[=> v12; => v15; ] => 0, 0, true => v16;]);
        v[16] = Some(v16);
        test(&v);
        test_endorsements(v16, &[vec![0, 1, 2, 3], vec![0, 1, 2, 3], vec![2]], 4);
    }

    #[test]
    #[should_panic] // https://github.com/nearprotocol/nearcore/issues/99
    fn generated_kickouts_and_epoch_blocks_1() {
        /* Interesting interaction between kickouts and epoch blocks. Representative block from Carol for Epoch 2 (which happens to be in Epoch 3) approves Representative block from Bob for Epoch 1 and Representative block from Alice for Epoch 0. */

        /* {"s":[{"owner":0,"parents":[]},{"owner":1,"parents":[]},{"owner":2,"parents":[1]},{"owner":3,"parents":[2]},{"owner":1,"parents":[3]},{"owner":2,"parents":[4]},{"owner":3,"parents":[5]},{"owner":1,"parents":[6]},{"owner":2,"parents":[5,6]},{"owner":3,"parents":[8]},{"owner":1,"parents":[7,8]},{"owner":2,"parents":[0,9,10]},{"owner":0,"parents":[11]},{"owner":1,"parents":[11]},{"owner":3,"parents":[11]}],"n":4,"c":"Interesting interaction between kickouts and epoch blocks. Representative block from Carol for Epoch 2 (which happens to be in Epoch 3) approves Representative block from Bob for Epoch 1 and Representative block from Alice for Epoch 0.","f":"kickouts_and_epoch_blocks_1"} */
        let arena = Arena::new();
        let selector = FakeNonContinuousWitnessSelector::new(4);
        let (v0, v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v11, v12, v13, v14);
        let mut v = [None; 15];
        let test = |v: &[_]| {
            make_assertions(
                &v,
                &[
                    (0, Some(0), false, 0),
                    (0, None, false, 1),
                    (0, None, false, 2),
                    (0, None, false, 3),
                    (1, None, true, 4),
                    (1, None, false, 5),
                    (1, None, false, 6),
                    (2, Some(1), false, 7),
                    (2, None, true, 8),
                    (2, None, false, 9),
                    (2, None, false, 10),
                    (3, Some(2), false, 11),
                    (1, None, false, 12),
                    (3, None, false, 13),
                    (3, Some(3), false, 14),
                ],
            )
        };
        simple_messages!(0, &selector, arena [0, 0, true => v0;]);
        v[0] = Some(v0);
        test(&v);
        test_endorsements(v0, &[vec![0]], 4);
        simple_messages!(0, &selector, arena [1, 0, true => v1;]);
        v[1] = Some(v1);
        test(&v);
        test_endorsements(v1, &[], 4);
        simple_messages!(0, &selector, arena [[=> v1; ] => 2, 0, true => v2;]);
        v[2] = Some(v2);
        test(&v);
        test_endorsements(v2, &[], 4);
        simple_messages!(0, &selector, arena [[=> v2; ] => 3, 0, true => v3;]);
        v[3] = Some(v3);
        test(&v);
        test_endorsements(v3, &[], 4);
        simple_messages!(0, &selector, arena [[=> v3; ] => 1, 0, true => v4;]);
        v[4] = Some(v4);
        test(&v);
        test_endorsements(v4, &[], 4);
        simple_messages!(0, &selector, arena [[=> v4; ] => 2, 0, true => v5;]);
        v[5] = Some(v5);
        test(&v);
        test_endorsements(v5, &[], 4);
        simple_messages!(0, &selector, arena [[=> v5; ] => 3, 0, true => v6;]);
        v[6] = Some(v6);
        test(&v);
        test_endorsements(v6, &[], 4);
        simple_messages!(0, &selector, arena [[=> v6; ] => 1, 0, true => v7;]);
        v[7] = Some(v7);
        test(&v);
        test_endorsements(v7, &[vec![], vec![1]], 4);
        simple_messages!(0, &selector, arena [[=> v5; => v6; ] => 2, 0, true => v8;]);
        v[8] = Some(v8);
        test(&v);
        test_endorsements(v8, &[], 4);
        simple_messages!(0, &selector, arena [[=> v8; ] => 3, 0, true => v9;]);
        v[9] = Some(v9);
        test(&v);
        test_endorsements(v9, &[], 4);
        simple_messages!(0, &selector, arena [[=> v7; => v8; ] => 1, 0, true => v10;]);
        v[10] = Some(v10);
        test(&v);
        test_endorsements(v10, &[vec![], vec![1]], 4);
        simple_messages!(0, &selector, arena [[=> v0; => v9; => v10; ] => 2, 0, true => v11;]);
        v[11] = Some(v11);
        test(&v);
        test_endorsements(v11, &[vec![0], vec![1], vec![2]], 4);
        simple_messages!(0, &selector, arena [[=> v11; ] => 0, 0, true => v12;]);
        v[12] = Some(v12);
        test(&v);
        test_endorsements(v12, &[vec![0], vec![0, 1], vec![0, 2]], 4);
        simple_messages!(0, &selector, arena [[=> v11; ] => 1, 0, true => v13;]);
        v[13] = Some(v13);
        test(&v);
        test_endorsements(v13, &[vec![0], vec![1], vec![1, 2]], 4);
        simple_messages!(0, &selector, arena [[=> v11; ] => 3, 0, true => v14;]);
        v[14] = Some(v14);
        test(&v);
        test_endorsements(v14, &[vec![0], vec![1], vec![2, 3], vec![3]], 4);
    }

    #[test]
    fn generated_kickouts_and_epoch_blocks_2() {
        /* In this test if you consider the graph from perspective of the second Bob's message in Epoch 2, the only epoch block is his R1. Thus from the perspective of the first Carol's message in Epoch 3 Alice's node can't be considered an epoch block, but Bob's must be. */

        /* {"s":[{"owner":1,"parents":[]},{"owner":0,"parents":[]},{"owner":2,"parents":[0]},{"owner":3,"parents":[0]},{"owner":4,"parents":[0]},{"owner":5,"parents":[0]},{"owner":1,"parents":[2,3,4,5]},{"owner":2,"parents":[6]},{"owner":3,"parents":[6]},{"owner":4,"parents":[6]},{"owner":5,"parents":[6]},{"owner":1,"parents":[7,8,9,10]},{"owner":2,"parents":[7,8,9,10]},{"owner":3,"parents":[11]},{"owner":4,"parents":[11]},{"owner":5,"parents":[11]},{"owner":6,"parents":[11]},{"owner":2,"parents":[1,11,12]},{"owner":0,"parents":[1,12]},{"owner":1,"parents":[13,14,15,16]},{"owner":3,"parents":[13,17]},{"owner":4,"parents":[14,17]},{"owner":5,"parents":[15,17]},{"owner":0,"parents":[17,18]},{"owner":2,"parents":[20,21,22,23]}],"n":7,"c":"In this test if you consider the graph from perspective of the second Bob's message in Epoch 2, the only epoch block is his R1. Thus from the perspective of the first Carol's message in Epoch 3 Alice's node can't be considered an epoch block, but Bob's must be.","f":"kickouts_and_epoch_blocks_2"} */
        let arena = Arena::new();
        let selector = FakeNonContinuousWitnessSelector::new(7);
        let (
            v0,
            v1,
            v2,
            v3,
            v4,
            v5,
            v6,
            v7,
            v8,
            v9,
            v10,
            v11,
            v12,
            v13,
            v14,
            v15,
            v16,
            v17,
            v18,
            v19,
            v20,
            v21,
            v22,
            v23,
            v24,
        );
        let mut v = [None; 25];
        let test = |v: &[_]| {
            make_assertions(
                &v,
                &[
                    (0, None, false, 0),
                    (0, Some(0), false, 1),
                    (0, None, false, 2),
                    (0, None, false, 3),
                    (0, None, false, 4),
                    (0, None, false, 5),
                    (1, None, true, 6),
                    (1, None, false, 7),
                    (1, None, false, 8),
                    (1, None, false, 9),
                    (1, None, false, 10),
                    (2, Some(1), false, 11),
                    (2, None, true, 12),
                    (2, None, false, 13),
                    (2, None, false, 14),
                    (2, None, false, 15),
                    (0, None, false, 16),
                    (2, Some(2), false, 17),
                    (1, None, false, 18),
                    (2, None, false, 19),
                    (2, None, false, 20),
                    (2, None, false, 21),
                    (2, None, false, 22),
                    (2, None, false, 23),
                    (3, None, false, 24),
                ],
            )
        };
        simple_messages!(0, &selector, arena [1, 0, true => v0;]);
        v[0] = Some(v0);
        test(&v);
        test_endorsements(v0, &[], 7);
        simple_messages!(0, &selector, arena [0, 0, true => v1;]);
        v[1] = Some(v1);
        test(&v);
        test_endorsements(v1, &[vec![0]], 7);
        simple_messages!(0, &selector, arena [[=> v0; ] => 2, 0, true => v2;]);
        v[2] = Some(v2);
        test(&v);
        test_endorsements(v2, &[], 7);
        simple_messages!(0, &selector, arena [[=> v0; ] => 3, 0, true => v3;]);
        v[3] = Some(v3);
        test(&v);
        test_endorsements(v3, &[], 7);
        simple_messages!(0, &selector, arena [[=> v0; ] => 4, 0, true => v4;]);
        v[4] = Some(v4);
        test(&v);
        test_endorsements(v4, &[], 7);
        simple_messages!(0, &selector, arena [[=> v0; ] => 5, 0, true => v5;]);
        v[5] = Some(v5);
        test(&v);
        test_endorsements(v5, &[], 7);
        simple_messages!(0, &selector, arena [[=> v2; => v3; => v4; => v5; ] => 1, 0, true => v6;]);
        v[6] = Some(v6);
        test(&v);
        test_endorsements(v6, &[], 7);
        simple_messages!(0, &selector, arena [[=> v6; ] => 2, 0, true => v7;]);
        v[7] = Some(v7);
        test(&v);
        test_endorsements(v7, &[], 7);
        simple_messages!(0, &selector, arena [[=> v6; ] => 3, 0, true => v8;]);
        v[8] = Some(v8);
        test(&v);
        test_endorsements(v8, &[], 7);
        simple_messages!(0, &selector, arena [[=> v6; ] => 4, 0, true => v9;]);
        v[9] = Some(v9);
        test(&v);
        test_endorsements(v9, &[], 7);
        simple_messages!(0, &selector, arena [[=> v6; ] => 5, 0, true => v10;]);
        v[10] = Some(v10);
        test(&v);
        test_endorsements(v10, &[], 7);
        simple_messages!(0, &selector, arena [[=> v7; => v8; => v9; => v10; ] => 1, 0, true => v11;]);
        v[11] = Some(v11);
        test(&v);
        test_endorsements(v11, &[vec![], vec![1]], 7);
        simple_messages!(0, &selector, arena [[=> v7; => v8; => v9; => v10; ] => 2, 0, true => v12;]);
        v[12] = Some(v12);
        test(&v);
        test_endorsements(v12, &[], 7);
        simple_messages!(0, &selector, arena [[=> v11; ] => 3, 0, true => v13;]);
        v[13] = Some(v13);
        test(&v);
        test_endorsements(v13, &[vec![], vec![1, 3]], 7);
        simple_messages!(0, &selector, arena [[=> v11; ] => 4, 0, true => v14;]);
        v[14] = Some(v14);
        test(&v);
        test_endorsements(v14, &[vec![], vec![1, 4]], 7);
        simple_messages!(0, &selector, arena [[=> v11; ] => 5, 0, true => v15;]);
        v[15] = Some(v15);
        test(&v);
        test_endorsements(v15, &[vec![], vec![1, 5]], 7);
        simple_messages!(0, &selector, arena [[=> v11; ] => 6, 0, true => v16;]);
        v[16] = Some(v16);
        test(&v);
        test_endorsements(v16, &[vec![], vec![1, 6]], 7);
        simple_messages!(0, &selector, arena [[=> v1; => v11; => v12; ] => 2, 0, true => v17;]);
        v[17] = Some(v17);
        test(&v);
        test_endorsements(v17, &[vec![0], vec![1], vec![2]], 7);
        simple_messages!(0, &selector, arena [[=> v1; => v12; ] => 0, 0, true => v18;]);
        v[18] = Some(v18);
        test(&v);
        test_endorsements(v18, &[vec![0]], 7);
        simple_messages!(0, &selector, arena [[=> v13; => v14; => v15; => v16; ] => 1, 0, true => v19;]);
        v[19] = Some(v19);
        test(&v);
        test_endorsements(v19, &[vec![], vec![1, 3, 4, 5, 6]], 7);
        simple_messages!(0, &selector, arena [[=> v13; => v17; ] => 3, 0, true => v20;]);
        v[20] = Some(v20);
        test(&v);
        test_endorsements(v20, &[vec![0], vec![1, 3], vec![2, 3]], 7);
        simple_messages!(0, &selector, arena [[=> v14; => v17; ] => 4, 0, true => v21;]);
        v[21] = Some(v21);
        test(&v);
        test_endorsements(v21, &[vec![0], vec![1, 4], vec![2, 4]], 7);
        simple_messages!(0, &selector, arena [[=> v15; => v17; ] => 5, 0, true => v22;]);
        v[22] = Some(v22);
        test(&v);
        test_endorsements(v22, &[vec![0], vec![1, 5], vec![2, 5]], 7);
        simple_messages!(0, &selector, arena [[=> v17; => v18; ] => 0, 0, true => v23;]);
        v[23] = Some(v23);
        test(&v);
        test_endorsements(v23, &[vec![0], vec![1], vec![0, 2]], 7);
        simple_messages!(0, &selector, arena [[=> v20; => v21; => v22; => v23; ] => 2, 0, true => v24;]);
        v[24] = Some(v24);
        test(&v);
        test_endorsements(v24, &[vec![0], vec![1, 3, 4, 5], vec![0, 2, 3, 4, 5]], 7);
    }

    #[test]
    #[should_panic] // https://github.com/nearprotocol/nearcore/issues/123
    fn generated_endorse_kickout_fork() {
        /* Bob create fork creating both endorsing and kick-out for alice message. */

        /* {"s":[{"owner":0,"parents":[]},{"owner":1,"parents":[]},{"owner":2,"parents":[]},{"owner":3,"parents":[]},{"owner":1,"parents":[0,1,2]},{"owner":1,"parents":[1,2,3]},{"owner":2,"parents":[5]},{"owner":3,"parents":[0,4,6]}],"n":4,"c":"Bob create fork creating both endorsing and kick-out for alice message.","f":""} */
        let arena = Arena::new();
        let selector = FakeNonContinuousWitnessSelector::new(4);
        let (v0, v1, v2, v3, v5, v4, v6, v7);
        let mut v = [None; 8];
        let test = |v: &[_]| {
            make_assertions(
                &v,
                &[
                    (0, Some(0), false, 0),
                    (0, None, false, 1),
                    (0, None, false, 2),
                    (0, None, false, 3),
                    (1, Some(1), false, 5),
                    (1, None, true, 4),
                    (1, None, false, 6),
                    (1, None, false, 7),
                ],
            )
        };
        simple_messages!(0, &selector, arena [0, 0, true => v0;]);
        v[0] = Some(v0);
        test(&v);
        test_endorsements(v0, &[vec![0]], 4);
        simple_messages!(0, &selector, arena [1, 0, true => v1;]);
        v[1] = Some(v1);
        test(&v);
        test_endorsements(v1, &[], 4);
        simple_messages!(0, &selector, arena [2, 0, true => v2;]);
        v[2] = Some(v2);
        test(&v);
        test_endorsements(v2, &[], 4);
        simple_messages!(0, &selector, arena [3, 0, true => v3;]);
        v[3] = Some(v3);
        test(&v);
        test_endorsements(v3, &[], 4);
        simple_messages!(0, &selector, arena [[=> v0; => v1; => v2; ] => 1, 0, true => v5;]);
        v[4] = Some(v5);
        test(&v);
        test_endorsements(v5, &[vec![0, 1], vec![1]], 4);
        simple_messages!(0, &selector, arena [[=> v1; => v2; => v3; ] => 1, 0, true => v4;]);
        v[5] = Some(v4);
        test(&v);
        test_endorsements(v4, &[], 4);
        simple_messages!(0, &selector, arena [[=> v4; ] => 2, 0, true => v6;]);
        v[6] = Some(v6);
        test(&v);
        test_endorsements(v6, &[], 4);
        simple_messages!(0, &selector, arena [[=> v0; => v5; => v6; ] => 3, 0, true => v7;]);
        v[7] = Some(v7);
        test(&v);
        test_endorsements(v7, &[vec![0, 1, 3], vec![1]], 4);
    }
}
