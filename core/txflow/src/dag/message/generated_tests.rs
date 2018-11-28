#[cfg(test)]
mod tests {
    use primitives::traits::{Payload, WitnessSelector};
    use primitives::types::UID;
    use std::collections::HashSet;
    use typed_arena::Arena;
    use dag::message::Message;

    struct FakeNonContinuousWitnessSelector {
        num_users: u64,
        users: HashSet<UID>,
    }

    impl FakeNonContinuousWitnessSelector {
        fn new(num_users: u64) -> Self {
            let mut users = set!{0};
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

    fn make_assertions<P: Payload>(messages: &[Option<&Message<P>>], assertions: &[(u64, Option<u64>, bool, u64)]) {
        for it in messages.iter().zip(assertions.iter()) {
            let (msg, a) = it;
            if let Some(msg) = msg.as_ref() {
                // If this assert triggers, the last element of tuples indicates the node uid
                assert_eq!((a.0, a.1, a.2, a.3), (msg.computed_epoch, msg.computed_is_representative, msg.computed_is_kickout, a.3));
            }
        }
    }

    #[test]
    fn generated_simple_test() {
        /* Simple test with two representative messages and no kickouts */

        /* {"s":[{"owner":0,"parents":[]},{"owner":1,"parents":[]},{"owner":2,"parents":[0,1]},{"owner":1,"parents":[2]}],"n":4,"c":"Simple test with two representative messages and no kickouts","f":"simple_test"} */
        let arena = Arena::new();
        let selector = FakeNonContinuousWitnessSelector::new(4);
        let (v0,v1,v2,v3);
        let mut v = [None; 4];
        let test = |v:&[_]| make_assertions(&v, &[(0, Some(0), false, 0), (0, None, false, 1), (0, None, false, 2), (1, Some(1), false, 3)]);
        simple_messages!(0, &selector, arena [0, 0, true => v0;]); v[0] = Some(v0);
        test(&v);
        simple_messages!(0, &selector, arena [1, 0, true => v1;]); v[1] = Some(v1);
        test(&v);
        simple_messages!(0, &selector, arena [[=> v0; => v1; ] => 2, 0, true => v2;]); v[2] = Some(v2);
        test(&v);
        simple_messages!(0, &selector, arena [[=> v2; ] => 1, 0, true => v3;]); v[3] = Some(v3);
        test(&v);
    }

    #[test]
    fn generated_bob_fork() {
        /* Representative message fork from Bob, testing endorsements */

        /* {"s":[{"owner":1,"parents":[]},{"owner":2,"parents":[0]},{"owner":3,"parents":[1]},{"owner":0,"parents":[2]},{"owner":1,"parents":[3]},{"owner":1,"parents":[3]},{"owner":2,"parents":[4]},{"owner":0,"parents":[4,5]},{"owner":3,"parents":[5]},{"owner":2,"parents":[5,6]}],"n":4,"c":"Representative message fork from Bob, testing endorsements","f":"bob_fork"} */
        let arena = Arena::new();
        let selector = FakeNonContinuousWitnessSelector::new(4);
        let (v0,v1,v2,v3,v4,v5,v6,v7,v8,v9);
        let mut v = [None; 10];
        let test = |v:&[_]| make_assertions(&v, &[(0, None, false, 0), (0, None, false, 1), (0, None, false, 2), (0, Some(0), false, 3), (1, Some(1), false, 4), (1, Some(1), false, 5), (1, None, false, 6), (1, None, false, 7), (1, None, false, 8), (1, None, false, 9)]);
        simple_messages!(0, &selector, arena [1, 0, true => v0;]); v[0] = Some(v0);
        test(&v);
        simple_messages!(0, &selector, arena [[=> v0; ] => 2, 0, true => v1;]); v[1] = Some(v1);
        test(&v);
        simple_messages!(0, &selector, arena [[=> v1; ] => 3, 0, true => v2;]); v[2] = Some(v2);
        test(&v);
        simple_messages!(0, &selector, arena [[=> v2; ] => 0, 0, true => v3;]); v[3] = Some(v3);
        test(&v);
        simple_messages!(0, &selector, arena [[=> v3; ] => 1, 0, true => v4;]); v[4] = Some(v4);
        test(&v);
        simple_messages!(0, &selector, arena [[=> v3; ] => 1, 0, true => v5;]); v[5] = Some(v5);
        test(&v);
        simple_messages!(0, &selector, arena [[=> v4; ] => 2, 0, true => v6;]); v[6] = Some(v6);
        test(&v);
        simple_messages!(0, &selector, arena [[=> v4; => v5; ] => 0, 0, true => v7;]); v[7] = Some(v7);
        test(&v);
        simple_messages!(0, &selector, arena [[=> v5; ] => 3, 0, true => v8;]); v[8] = Some(v8);
        test(&v);
        simple_messages!(0, &selector, arena [[=> v5; => v6; ] => 2, 0, true => v9;]); v[9] = Some(v9);
        test(&v);
    }

    #[test]
    fn generated_used_to_panic() {
        /* This used to panic */

        /* {"s":[{"owner":1,"parents":[]},{"owner":0,"parents":[0]},{"owner":2,"parents":[1]},{"owner":3,"parents":[2]},{"owner":1,"parents":[2]},{"owner":2,"parents":[3,4]}],"n":4,"c":"This used to panic","f":"used_to_panic"} */
        let arena = Arena::new();
        let selector = FakeNonContinuousWitnessSelector::new(4);
        let (v0,v1,v2,v3,v4,v5);
        let mut v = [None; 6];
        let test = |v:&[_]| make_assertions(&v, &[(0, None, false, 0), (0, Some(0), false, 1), (0, None, false, 2), (0, None, false, 3), (1, Some(1), false, 4), (1, None, false, 5)]);
        simple_messages!(0, &selector, arena [1, 0, true => v0;]); v[0] = Some(v0);
        test(&v);
        simple_messages!(0, &selector, arena [[=> v0; ] => 0, 0, true => v1;]); v[1] = Some(v1);
        test(&v);
        simple_messages!(0, &selector, arena [[=> v1; ] => 2, 0, true => v2;]); v[2] = Some(v2);
        test(&v);
        simple_messages!(0, &selector, arena [[=> v2; ] => 3, 0, true => v3;]); v[3] = Some(v3);
        test(&v);
        simple_messages!(0, &selector, arena [[=> v2; ] => 1, 0, true => v4;]); v[4] = Some(v4);
        test(&v);
        simple_messages!(0, &selector, arena [[=> v3; => v4; ] => 2, 0, true => v5;]); v[5] = Some(v5);
        test(&v);
    }

    #[test]
    fn generated_kickouts() {
        /* Some kickout messages */

        /* {"s":[{"owner":1,"parents":[]},{"owner":0,"parents":[0]},{"owner":2,"parents":[1]},{"owner":3,"parents":[2]},{"owner":1,"parents":[2]},{"owner":2,"parents":[3,4]},{"owner":0,"parents":[4]},{"owner":1,"parents":[5]},{"owner":0,"parents":[6,7]},{"owner":3,"parents":[8]},{"owner":3,"parents":[9]},{"owner":1,"parents":[10]},{"owner":3,"parents":[11]},{"owner":2,"parents":[11]},{"owner":3,"parents":[12,13]},{"owner":1,"parents":[12,13]},{"owner":2,"parents":[15]},{"owner":1,"parents":[14,16]},{"owner":2,"parents":[14,16]},{"owner":3,"parents":[14,16]},{"owner":1,"parents":[17,18,19]},{"owner":2,"parents":[20]},{"owner":3,"parents":[20]},{"owner":1,"parents":[21,22]}],"n":4,"c":"Some kickout messages","f":"kickouts"} */
        let arena = Arena::new();
        let selector = FakeNonContinuousWitnessSelector::new(4);
        let (v0,v1,v2,v3,v4,v5,v6,v7,v8,v9,v10,v11,v12,v13,v14,v15,v16,v17,v18,v19,v20,v21,v22,v23);
        let mut v = [None; 24];
        let test = |v:&[_]| make_assertions(&v, &[(0, None, false, 0), (0, Some(0), false, 1), (0, None, false, 2), (0, None, false, 3), (1, Some(1), false, 4), (1, None, false, 5), (1, None, false, 6), (1, None, false, 7), (2, None, false, 8), (1, None, false, 9), (2, None, false, 10), (2, None, false, 11), (3, None, true, 12), (2, Some(2), false, 13), (3, Some(3), false, 14), (3, None, false, 15), (3, None, false, 16), (4, None, false, 17), (4, None, false, 18), (4, None, false, 19), (5, None, true, 20), (5, None, false, 21), (5, None, false, 22), (6, Some(5), false, 23)]);
        simple_messages!(0, &selector, arena [1, 0, true => v0;]); v[0] = Some(v0);
        test(&v);
        simple_messages!(0, &selector, arena [[=> v0; ] => 0, 0, true => v1;]); v[1] = Some(v1);
        test(&v);
        simple_messages!(0, &selector, arena [[=> v1; ] => 2, 0, true => v2;]); v[2] = Some(v2);
        test(&v);
        simple_messages!(0, &selector, arena [[=> v2; ] => 3, 0, true => v3;]); v[3] = Some(v3);
        test(&v);
        simple_messages!(0, &selector, arena [[=> v2; ] => 1, 0, true => v4;]); v[4] = Some(v4);
        test(&v);
        simple_messages!(0, &selector, arena [[=> v3; => v4; ] => 2, 0, true => v5;]); v[5] = Some(v5);
        test(&v);
        simple_messages!(0, &selector, arena [[=> v4; ] => 0, 0, true => v6;]); v[6] = Some(v6);
        test(&v);
        simple_messages!(0, &selector, arena [[=> v5; ] => 1, 0, true => v7;]); v[7] = Some(v7);
        test(&v);
        simple_messages!(0, &selector, arena [[=> v6; => v7; ] => 0, 0, true => v8;]); v[8] = Some(v8);
        test(&v);
        simple_messages!(0, &selector, arena [[=> v8; ] => 3, 0, true => v9;]); v[9] = Some(v9);
        test(&v);
        simple_messages!(0, &selector, arena [[=> v9; ] => 3, 0, true => v10;]); v[10] = Some(v10);
        test(&v);
        simple_messages!(0, &selector, arena [[=> v10; ] => 1, 0, true => v11;]); v[11] = Some(v11);
        test(&v);
        simple_messages!(0, &selector, arena [[=> v11; ] => 3, 0, true => v12;]); v[12] = Some(v12);
        test(&v);
        simple_messages!(0, &selector, arena [[=> v11; ] => 2, 0, true => v13;]); v[13] = Some(v13);
        test(&v);
        simple_messages!(0, &selector, arena [[=> v12; => v13; ] => 3, 0, true => v14;]); v[14] = Some(v14);
        test(&v);
        simple_messages!(0, &selector, arena [[=> v12; => v13; ] => 1, 0, true => v15;]); v[15] = Some(v15);
        test(&v);
        simple_messages!(0, &selector, arena [[=> v15; ] => 2, 0, true => v16;]); v[16] = Some(v16);
        test(&v);
        simple_messages!(0, &selector, arena [[=> v14; => v16; ] => 1, 0, true => v17;]); v[17] = Some(v17);
        test(&v);
        simple_messages!(0, &selector, arena [[=> v14; => v16; ] => 2, 0, true => v18;]); v[18] = Some(v18);
        test(&v);
        simple_messages!(0, &selector, arena [[=> v14; => v16; ] => 3, 0, true => v19;]); v[19] = Some(v19);
        test(&v);
        simple_messages!(0, &selector, arena [[=> v17; => v18; => v19; ] => 1, 0, true => v20;]); v[20] = Some(v20);
        test(&v);
        simple_messages!(0, &selector, arena [[=> v20; ] => 2, 0, true => v21;]); v[21] = Some(v21);
        test(&v);
        simple_messages!(0, &selector, arena [[=> v20; ] => 3, 0, true => v22;]); v[22] = Some(v22);
        test(&v);
        simple_messages!(0, &selector, arena [[=> v21; => v22; ] => 1, 0, true => v23;]); v[23] = Some(v23);
        test(&v);
    }
}
