const { Account, SimpleKeyStoreSigner, InMemoryKeyStore, KeyPair, LocalNodeConnection, NearClient, Near } = require('../');
const fs = require('fs');

const aliceAccountName = 'alice.near';
const aliceKey = new KeyPair(
    '22skMptHjFWNyuEWY22ftn2AbLPSYpmYwGJRGwpNHbTV',
    '2wyRcSwSuHtRVmkMCGjPwnzZmQLeXLzLLyED1NDMt4BjnKgQL6tF85yBx6Jr26D2dUNeC716RBoTxntVHsegogYw'
);
const test_key_store = new InMemoryKeyStore();
const simple_key_store_signer = new SimpleKeyStoreSigner(test_key_store);
test_key_store.setKey(aliceAccountName, aliceKey);
const localNodeConnection = new LocalNodeConnection('http://localhost:3030');
const nearClient = new NearClient(simple_key_store_signer, localNodeConnection);
const account = new Account(nearClient);
const nearjs = new Near(nearClient);

test('test creating default config', async () => {
    // Make sure createDefaultConfig doesn't crash.
    Near.createDefaultConfig();
});

test('view pre-defined account works and returns correct name', async () => {
    // We do not want to check the other properties of this account since we create other accounts
    // using this account as the originator
    const viewAccountResponse = await account.viewAccount(aliceAccountName);
    expect(viewAccountResponse.account_id).toEqual(aliceAccountName);
});

test('create account and then view account returns the created account', async () => {
    const newAccountName = await generateUniqueString('create.account.test');
    const newAccountPublicKey = '9AhWenZ3JddamBoyMqnTbp7yVbRuvqAv3zwfrWgfVRJE';
    const createAccountResponse = await account.createAccount(newAccountName, newAccountPublicKey, 1, aliceAccountName);
    await nearjs.waitForTransactionResult(createAccountResponse);
    const expectedAccount = {
        nonce: 0,
        account_id: newAccountName,
        amount: 1,
        code_hash: 'GKot5hBsd81kMupNCXHaqbhv3huEbxAFMLnpcX2hniwn',
        stake: 0,
    };
    const result = await account.viewAccount(newAccountName);
    expect(result).toEqual(expectedAccount);
});

test('create account with a new key and then view account returns the created account', async () => {
    const newAccountName = await generateUniqueString('create.randomkey.test');
    const amount = 2;
    const aliceAccountBeforeCreation = await account.viewAccount(aliceAccountName);
    const createAccountResponse = await account.createAccountWithRandomKey(
        newAccountName,
        amount,
        aliceAccountName);
    await nearjs.waitForTransactionResult(createAccountResponse);
    expect(createAccountResponse['key']).not.toBeFalsy();
    const expectedAccount = {
        nonce: 0,
        account_id: newAccountName,
        amount: amount,
        code_hash: 'GKot5hBsd81kMupNCXHaqbhv3huEbxAFMLnpcX2hniwn',
        stake: 0,
    };
    const result = await account.viewAccount(newAccountName);
    expect(result).toEqual(expectedAccount);
    const aliceAccountAfterCreation = await account.viewAccount(aliceAccountName);
    expect(aliceAccountAfterCreation.amount).toBe(aliceAccountBeforeCreation.amount - amount);
});

describe('with deployed contract', () => {
    let contract;
    let oldLog;
    let logs;
    let contractName = 'test_contract_' + Date.now();

    beforeAll(async () => {
        // See README.md for details about this contract source code location.
        const keyWithRandomSeed = await KeyPair.fromRandomSeed();
        const createAccountResponse = await account.createAccount(
            contractName,
            keyWithRandomSeed.getPublicKey(),
            10,
            aliceAccountName);
        await nearjs.waitForTransactionResult(createAccountResponse);
        test_key_store.setKey(contractName, keyWithRandomSeed);
        const data = [...fs.readFileSync('../tests/hello.wasm')];
        await nearjs.waitForTransactionResult(
            await nearjs.deployContract(contractName, data));
        contract = await nearjs.loadContract(contractName, {
            sender: aliceAccountName,
            viewMethods: ['getAllKeys'],
            changeMethods: ['generateLogs', 'triggerAssert']
        });
    });

    beforeEach(async () => {
        oldLog = console.log;
        logs = [];
        console.log = function() {
            logs.push(Array.from(arguments).join(' '));
        };
    });

    afterEach(async () => {
        console.log = oldLog;
    });

    test('make function calls', async () => {
        const args = {
            'name': 'trex'
        };
        const viewFunctionResult = await nearjs.callViewFunction(
            contractName,
            'hello', // this is the function defined in hello.wasm file that we are calling
            args);
        expect(viewFunctionResult).toEqual('hello trex');

        var setCallValue = await generateUniqueString('setCallPrefix');
        const setArgs = {
            'value': setCallValue
        };
        const scheduleResult = await nearjs.scheduleFunctionCall(
            0,
            aliceAccountName,
            contractName,
            'setValue', // this is the function defined in hello.wasm file that we are calling
            setArgs);
        expect(scheduleResult.hash).not.toBeFalsy();
        await nearjs.waitForTransactionResult(scheduleResult);
        const getValueResult = await nearjs.callViewFunction(
            contractName,
            'getValue', // this is the function defined in hello.wasm file that we are calling
            {});
        expect(getValueResult).toEqual(setCallValue);

        // test that load contract works and we can make calls afterwards
        const options = {
            viewMethods: ['hello', 'getValue'],
            changeMethods: ['setValue'],
            sender: aliceAccountName
        };
        const contract = await nearjs.loadContract(contractName, options);
        const helloResult = await contract.hello(args);
        expect(helloResult).toEqual('hello trex');
        var setCallValue2 = await generateUniqueString('setCallPrefix');
        const setArgs2 = {
            'value': setCallValue2
        };
        const setValueResult = await contract.setValue(setArgs2);
        expect(setValueResult.status).toBe('Completed');
        const getValueResult2 = await contract.getValue();
        expect(getValueResult2).toEqual(setCallValue2);
    });

    test('can get logs from method result', async () => {
        await contract.generateLogs();
        expect(logs).toEqual([`[${contractName}]: LOG: log1`, `[${contractName}]: LOG: log2`]);
    });

    test('can get assert message from method result', async () => {
        await expect(contract.triggerAssert()).rejects.toThrow(/Transaction .+ failed.+expected to fail/);
        expect(logs.length).toBe(3);
        expect(logs[0]).toEqual(`[${contractName}]: LOG: log before assert`);
        expect(logs[1]).toMatch(new RegExp(`^\\[${contractName}\\]: ABORT: "expected to fail" filename: "main.ts" line: \\d+ col: \\d+$`));
        expect(logs[2]).toEqual(`[${contractName}]: Runtime error: wasm async call execution failed with error: Wasmer(CallError(Runtime(User { msg: "Error: AssertFailed" })))`);
    });
});

// Generate some unique string with a given prefix using the alice nonce. 
const generateUniqueString = async (prefix) => {
    const viewAccountResponse = await account.viewAccount(aliceAccountName);
    return prefix + viewAccountResponse.nonce;
};
