
#[test]
fn test_zero_balance_account_upgrade() {
    let epoch_length = 5;
    let mut genesis = Genesis::test(vec!["test0".parse().unwrap(), "test1".parse().unwrap()], 1);
    genesis.config.epoch_length = epoch_length;
    genesis.config.protocol_version = ProtocolFeature::ZeroBalanceAccount.protocol_version() - 1;
    let mut env = TestEnv::builder(ChainGenesis::test())
        .runtime_adapters(create_nightshade_runtimes(&genesis, 1))
        .build();
    let genesis_block = env.clients[0].chain.get_block_by_height(0).unwrap();
