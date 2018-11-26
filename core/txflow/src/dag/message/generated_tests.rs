#[cfg(test)]
mod tests {
    use primitives::traits::WitnessSelector;
    use primitives::types::UID;
    use std::collections::HashSet;
    use typed_arena::Arena;

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

    #[test]
    fn generated_simple_test() {
        /* Simple test with two representative messages and no kickouts */

        /* {"s":[{"owner":0,"parents":[]},{"owner":1,"parents":[]},{"owner":2,"parents":[0,1]},{"owner":1,"parents":[2]}],"n":4,"c":"Simple test with two representative messages and no kickouts","f":"simple_test"} */
        let arena = Arena::new();
        let selector = FakeNonContinuousWitnessSelector::new(4);
        let (v0, v1, v2, v3);
        simple_messages!(0, &selector, arena [0, 0, true => v0;]);
        simple_messages!(0, &selector, arena [1, 0, true => v1;]);
        simple_messages!(0, &selector, arena [[=> v0; => v1; ] => 2, 0, true => v2;]);
        simple_messages!(0, &selector, arena [[=> v2; ] => 1, 0, true => v3;]);

        assert_eq!(v0.computed_epoch, 0);
        assert_eq!(v0.computed_is_representative, true);
        assert_eq!(v0.computed_is_kickout, false);
        assert_eq!(v1.computed_epoch, 0);
        assert_eq!(v1.computed_is_representative, false);
        assert_eq!(v1.computed_is_kickout, false);
        assert_eq!(v2.computed_epoch, 0);
        assert_eq!(v2.computed_is_representative, false);
        assert_eq!(v2.computed_is_kickout, false);
        assert_eq!(v3.computed_epoch, 1);
        assert_eq!(v3.computed_is_representative, true);
        assert_eq!(v3.computed_is_kickout, false);
    }

    #[test]
    fn generated_bob_fork() {
        /* Representative message fork from Bob, testing endorsements */

        /* {"s":[{"owner":1,"parents":[]},{"owner":2,"parents":[0]},{"owner":3,"parents":[1]},{"owner":0,"parents":[2]},{"owner":1,"parents":[3]},{"owner":1,"parents":[3]},{"owner":2,"parents":[4]},{"owner":0,"parents":[4,5]},{"owner":3,"parents":[5]},{"owner":2,"parents":[5,6]}],"n":4,"c":"Representative message fork from Bob, testing endorsements","f":"bob_fork"} */
        let arena = Arena::new();
        let selector = FakeNonContinuousWitnessSelector::new(4);
        let (v4, v5, v6, v7, v8, v9, v10, v11, v12, v13);
        simple_messages!(0, &selector, arena [1, 0, true => v4;]);
        simple_messages!(0, &selector, arena [[=> v4; ] => 2, 0, true => v5;]);
        simple_messages!(0, &selector, arena [[=> v5; ] => 3, 0, true => v6;]);
        simple_messages!(0, &selector, arena [[=> v6; ] => 0, 0, true => v7;]);
        simple_messages!(0, &selector, arena [[=> v7; ] => 1, 0, true => v8;]);
        simple_messages!(0, &selector, arena [[=> v7; ] => 1, 0, true => v9;]);
        simple_messages!(0, &selector, arena [[=> v8; ] => 2, 0, true => v10;]);
        simple_messages!(0, &selector, arena [[=> v8; => v9; ] => 0, 0, true => v11;]);
        simple_messages!(0, &selector, arena [[=> v9; ] => 3, 0, true => v12;]);
        simple_messages!(0, &selector, arena [[=> v9; => v10; ] => 2, 0, true => v13;]);

        assert_eq!(v4.computed_epoch, 0);
        assert_eq!(v4.computed_is_representative, false);
        assert_eq!(v4.computed_is_kickout, false);
        assert_eq!(v5.computed_epoch, 0);
        assert_eq!(v5.computed_is_representative, false);
        assert_eq!(v5.computed_is_kickout, false);
        assert_eq!(v6.computed_epoch, 0);
        assert_eq!(v6.computed_is_representative, false);
        assert_eq!(v6.computed_is_kickout, false);
        assert_eq!(v7.computed_epoch, 0);
        assert_eq!(v7.computed_is_representative, true);
        assert_eq!(v7.computed_is_kickout, false);
        assert_eq!(v8.computed_epoch, 1);
        assert_eq!(v8.computed_is_representative, true);
        assert_eq!(v8.computed_is_kickout, false);
        assert_eq!(v9.computed_epoch, 1);
        assert_eq!(v9.computed_is_representative, true);
        assert_eq!(v9.computed_is_kickout, false);
        assert_eq!(v10.computed_epoch, 1);
        assert_eq!(v10.computed_is_representative, false);
        assert_eq!(v10.computed_is_kickout, false);
        assert_eq!(v11.computed_epoch, 1);
        assert_eq!(v11.computed_is_representative, false);
        assert_eq!(v11.computed_is_kickout, false);
        assert_eq!(v12.computed_epoch, 1);
        assert_eq!(v12.computed_is_representative, false);
        assert_eq!(v12.computed_is_kickout, false);
        assert_eq!(v13.computed_epoch, 1);
        assert_eq!(v13.computed_is_representative, false);
        assert_eq!(v13.computed_is_kickout, false);
    }

    #[test]
    fn generated_used_to_panic() {
        /* This used to panic */

        /* {"s":[{"owner":1,"parents":[]},{"owner":0,"parents":[0]},{"owner":2,"parents":[1]},{"owner":3,"parents":[2]},{"owner":1,"parents":[2]},{"owner":2,"parents":[3,4]}],"n":4,"c":"This used to panic","f":"used_to_panic"} */
        let arena = Arena::new();
        let selector = FakeNonContinuousWitnessSelector::new(4);
        let (v0, v1, v2, v3, v4, v5);
        simple_messages!(0, &selector, arena [1, 0, true => v0;]);
        simple_messages!(0, &selector, arena [[=> v0; ] => 0, 0, true => v1;]);
        simple_messages!(0, &selector, arena [[=> v1; ] => 2, 0, true => v2;]);
        simple_messages!(0, &selector, arena [[=> v2; ] => 3, 0, true => v3;]);
        simple_messages!(0, &selector, arena [[=> v2; ] => 1, 0, true => v4;]);
        simple_messages!(0, &selector, arena [[=> v3; => v4; ] => 2, 0, true => v5;]);

        assert_eq!(v0.computed_epoch, 0);
        assert_eq!(v0.computed_is_representative, false);
        assert_eq!(v0.computed_is_kickout, false);
        assert_eq!(v1.computed_epoch, 0);
        assert_eq!(v1.computed_is_representative, true);
        assert_eq!(v1.computed_is_kickout, false);
        assert_eq!(v2.computed_epoch, 0);
        assert_eq!(v2.computed_is_representative, false);
        assert_eq!(v2.computed_is_kickout, false);
        assert_eq!(v3.computed_epoch, 0);
        assert_eq!(v3.computed_is_representative, false);
        assert_eq!(v3.computed_is_kickout, false);
        assert_eq!(v4.computed_epoch, 1);
        assert_eq!(v4.computed_is_representative, true);
        assert_eq!(v4.computed_is_kickout, false);
        assert_eq!(v5.computed_epoch, 1);
        assert_eq!(v5.computed_is_representative, false);
        assert_eq!(v5.computed_is_kickout, false);
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
        simple_messages!(0, &selector, arena [1, 0, true => v0;]);
        simple_messages!(0, &selector, arena [[=> v0; ] => 0, 0, true => v1;]);
        simple_messages!(0, &selector, arena [[=> v1; ] => 2, 0, true => v2;]);
        simple_messages!(0, &selector, arena [[=> v2; ] => 3, 0, true => v3;]);
        simple_messages!(0, &selector, arena [[=> v2; ] => 1, 0, true => v4;]);
        simple_messages!(0, &selector, arena [[=> v3; => v4; ] => 2, 0, true => v5;]);
        simple_messages!(0, &selector, arena [[=> v4; ] => 0, 0, true => v6;]);
        simple_messages!(0, &selector, arena [[=> v5; ] => 1, 0, true => v7;]);
        simple_messages!(0, &selector, arena [[=> v6; => v7; ] => 0, 0, true => v8;]);
        simple_messages!(0, &selector, arena [[=> v8; ] => 3, 0, true => v9;]);
        simple_messages!(0, &selector, arena [[=> v9; ] => 3, 0, true => v10;]);
        simple_messages!(0, &selector, arena [[=> v10; ] => 1, 0, true => v11;]);
        simple_messages!(0, &selector, arena [[=> v11; ] => 3, 0, true => v12;]);
        simple_messages!(0, &selector, arena [[=> v11; ] => 2, 0, true => v13;]);
        simple_messages!(0, &selector, arena [[=> v12; => v13; ] => 3, 0, true => v14;]);
        simple_messages!(0, &selector, arena [[=> v12; => v13; ] => 1, 0, true => v15;]);
        simple_messages!(0, &selector, arena [[=> v15; ] => 2, 0, true => v16;]);
        simple_messages!(0, &selector, arena [[=> v14; => v16; ] => 1, 0, true => v17;]);
        simple_messages!(0, &selector, arena [[=> v14; => v16; ] => 2, 0, true => v18;]);
        simple_messages!(0, &selector, arena [[=> v14; => v16; ] => 3, 0, true => v19;]);
        simple_messages!(0, &selector, arena [[=> v17; => v18; => v19; ] => 1, 0, true => v20;]);
        simple_messages!(0, &selector, arena [[=> v20; ] => 2, 0, true => v21;]);
        simple_messages!(0, &selector, arena [[=> v20; ] => 3, 0, true => v22;]);
        simple_messages!(0, &selector, arena [[=> v21; => v22; ] => 1, 0, true => v23;]);

        assert_eq!(v0.computed_epoch, 0);
        assert_eq!(v0.computed_is_representative, false);
        assert_eq!(v0.computed_is_kickout, false);
        assert_eq!(v1.computed_epoch, 0);
        assert_eq!(v1.computed_is_representative, true);
        assert_eq!(v1.computed_is_kickout, false);
        assert_eq!(v2.computed_epoch, 0);
        assert_eq!(v2.computed_is_representative, false);
        assert_eq!(v2.computed_is_kickout, false);
        assert_eq!(v3.computed_epoch, 0);
        assert_eq!(v3.computed_is_representative, false);
        assert_eq!(v3.computed_is_kickout, false);
        assert_eq!(v4.computed_epoch, 1);
        assert_eq!(v4.computed_is_representative, true);
        assert_eq!(v4.computed_is_kickout, false);
        assert_eq!(v5.computed_epoch, 1);
        assert_eq!(v5.computed_is_representative, false);
        assert_eq!(v5.computed_is_kickout, false);
        assert_eq!(v6.computed_epoch, 1);
        assert_eq!(v6.computed_is_representative, false);
        assert_eq!(v6.computed_is_kickout, false);
        assert_eq!(v7.computed_epoch, 1);
        assert_eq!(v7.computed_is_representative, false);
        assert_eq!(v7.computed_is_kickout, false);
        assert_eq!(v8.computed_epoch, 2);
        assert_eq!(v8.computed_is_representative, false);
        assert_eq!(v8.computed_is_kickout, false);
        assert_eq!(v9.computed_epoch, 1);
        assert_eq!(v9.computed_is_representative, false);
        assert_eq!(v9.computed_is_kickout, false);
        assert_eq!(v10.computed_epoch, 2);
        assert_eq!(v10.computed_is_representative, false);
        assert_eq!(v10.computed_is_kickout, false);
        assert_eq!(v11.computed_epoch, 2);
        assert_eq!(v11.computed_is_representative, false);
        assert_eq!(v11.computed_is_kickout, false);
        assert_eq!(v12.computed_epoch, 3);
        assert_eq!(v12.computed_is_representative, false);
        assert_eq!(v12.computed_is_kickout, true);
        assert_eq!(v13.computed_epoch, 2);
        assert_eq!(v13.computed_is_representative, true);
        assert_eq!(v13.computed_is_kickout, false);
        assert_eq!(v14.computed_epoch, 3);
        assert_eq!(v14.computed_is_representative, true);
        assert_eq!(v14.computed_is_kickout, false);
        assert_eq!(v15.computed_epoch, 3);
        assert_eq!(v15.computed_is_representative, false);
        assert_eq!(v15.computed_is_kickout, false);
        assert_eq!(v16.computed_epoch, 3);
        assert_eq!(v16.computed_is_representative, false);
        assert_eq!(v16.computed_is_kickout, false);
        assert_eq!(v17.computed_epoch, 4);
        assert_eq!(v17.computed_is_representative, false);
        assert_eq!(v17.computed_is_kickout, false);
        assert_eq!(v18.computed_epoch, 4);
        assert_eq!(v18.computed_is_representative, false);
        assert_eq!(v18.computed_is_kickout, false);
        assert_eq!(v19.computed_epoch, 4);
        assert_eq!(v19.computed_is_representative, false);
        assert_eq!(v19.computed_is_kickout, false);
        assert_eq!(v20.computed_epoch, 5);
        assert_eq!(v20.computed_is_representative, false);
        assert_eq!(v20.computed_is_kickout, true);
        assert_eq!(v21.computed_epoch, 5);
        assert_eq!(v21.computed_is_representative, false);
        assert_eq!(v21.computed_is_kickout, false);
        assert_eq!(v22.computed_epoch, 5);
        assert_eq!(v22.computed_is_representative, false);
        assert_eq!(v22.computed_is_kickout, false);
        assert_eq!(v23.computed_epoch, 6);
        // TODO: uncomment this one
        //assert_eq!(v23.computed_is_representative, true);
        assert_eq!(v23.computed_is_kickout, false);
    }

}
