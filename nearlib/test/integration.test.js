const SimpleKeyStoreSigner = require('../signing/simple_key_store_signer.js');
const InMemoryKeyStore = require('../signing/in_memory_key_store.js');
const KeyPair = require('../signing/key_pair.js');
const LocalNodeConnection = require('../local_node_connection')
const NearClient = require('../nearclient');
const Account = require('../account');
const Near = require('../near');
const fs = require('fs');


const aliceAccountName = 'alice.near';
const aliceKey = new KeyPair(
    "22skMptHjFWNyuEWY22ftn2AbLPSYpmYwGJRGwpNHbTV",
    "2wyRcSwSuHtRVmkMCGjPwnzZmQLeXLzLLyED1NDMt4BjnKgQL6tF85yBx6Jr26D2dUNeC716RBoTxntVHsegogYw"
);
const test_key_store = new InMemoryKeyStore();
const simple_key_store_signer = new SimpleKeyStoreSigner(test_key_store);
test_key_store.setKey(aliceAccountName, aliceKey);
const localNodeConnection = new LocalNodeConnection("http://localhost:3030");
const nearClient = new NearClient(simple_key_store_signer, localNodeConnection);
const account = new Account(nearClient);
const nearjs = new Near(nearClient);
const TEST_MAX_RETRIES = 10;
const TRANSACTION_COMPLETE_MAX_RETRIES = 100;


test('view pre-defined account works and returns correct name', async () => {
    // We do not want to check the other properties of this account since we create other accounts
    // using this account as the originator
    const viewAccountResponse = await account.viewAccount(aliceAccountName);
    expect(viewAccountResponse.account_id).toEqual(aliceAccountName);
});

test('create account and then view account returns the created account', async () => {
    const newAccountName = await generateUniqueString("create.account.test");
    const newAccountPublicKey = '9AhWenZ3JddamBoyMqnTbp7yVbRuvqAv3zwfrWgfVRJE';
    const createAccountResponse = await account.createAccount(newAccountName, newAccountPublicKey, 1, aliceAccountName);
    const expctedAccount = {
        nonce: 0,
        account_id: newAccountName,
        amount: 1,
        code_hash: 'GKot5hBsd81kMupNCXHaqbhv3huEbxAFMLnpcX2hniwn',
        stake: 0,
    };

    const viewAccountFunc = async () => {
        return await account.viewAccount(newAccountName);
    };
    const checkConditionFunc = (result) => {
        expect(result).toEqual(expctedAccount);
        return true;
    }
    await callUntilConditionIsMet(
        viewAccountFunc,
        checkConditionFunc, 
        "Call view account until result matches expected value");
});

test('create account with a new key and then view account returns the created account', async () => {
    const newAccountName = await generateUniqueString("create.randomkey.test");
    const amount = 2;
    const aliceAccountBeforeCreation = await account.viewAccount(aliceAccountName);
    const createAccountResponse = await account.createAccountWithRandomKey(
        newAccountName,
        amount,
        aliceAccountName);
    expect(createAccountResponse["key"]).not.toBeFalsy();
    const expctedAccount = {
        nonce: 0,
        account_id: newAccountName,
        amount: amount,
        code_hash: 'GKot5hBsd81kMupNCXHaqbhv3huEbxAFMLnpcX2hniwn',
        stake: 0,
    };

    const viewAccountFunc = async () => {
        return await account.viewAccount(newAccountName);
    };
    const checkConditionFunc = (result) => {
        expect(result).toEqual(expctedAccount);
        return true;
    }
    await callUntilConditionIsMet(
        viewAccountFunc,
        checkConditionFunc, 
        "Call view account until result matches expected value");
    const aliceAccountAfterCreation = await account.viewAccount(aliceAccountName);
    expect(aliceAccountAfterCreation.amount).toBe(aliceAccountBeforeCreation.amount - amount);
});

test('deploy contract and make function calls', async () => {
    // See README.md for details about this contract source code location.
    const data = [...fs.readFileSync('../tests/hello.wasm')];
    const initialAccount = await account.viewAccount(aliceAccountName);
    const deployResult = await nearjs.deployContract(
        aliceAccountName,
        "test_contract",
        data,
        "FTEov54o3JFxgnrouLNo2uferbvkU7fHDJvt7ohJNpZY");
    await waitForContractToDeploy(deployResult);
    const args = {
        "name": "trex"
    };
    const viewFunctionResult = await nearjs.callViewFunction(
        aliceAccountName,
        "test_contract",
        "hello", // this is the function defined in hello.wasm file that we are calling
        args);
    expect(viewFunctionResult).toEqual("hello trex");

    var setCallValue = await generateUniqueString("setCallPrefix");
    const accountBeforeScheduleCall = await account.viewAccount(aliceAccountName);
    const setArgs = {
        "value": setCallValue
    };
    const scheduleResult = await nearjs.scheduleFunctionCall(
        0,
        aliceAccountName,
        "test_contract",
        "setValue", // this is the function defined in hello.wasm file that we are calling
        setArgs);
    expect(scheduleResult.hash).not.toBeFalsy();
    await callUntilConditionIsMet(
        async () => { return await nearClient.getTransactionStatus(scheduleResult.hash); },
        (response) => { return response['status'] == 'Completed' },
        "Call get transaction status until transaction is completed",
        TRANSACTION_COMPLETE_MAX_RETRIES
    );
    const secondViewFunctionResult = await nearjs.callViewFunction(
        aliceAccountName,
        "test_contract",
        "getValue", // this is the function defined in hello.wasm file that we are calling
        {});
    expect(secondViewFunctionResult).toEqual(setCallValue);
});

const callUntilConditionIsMet = async (functToPoll, condition, description, maxRetries = TEST_MAX_RETRIES) => {
    for (let i = 0; i < maxRetries; i++) {
        try {
            const response = await functToPoll();
            if (condition(response)) {
                console.log("Success " + description + " in " + (i + 1) + " attempts.");
                return response;
            }
        } catch (e) {
            if (i == TEST_MAX_RETRIES - 1) {
                fail('exceeded number of retries for ' + description + ". Last error " + e.toString());
            }
        }
    }
};

const waitForContractToDeploy = async (deployResult) => {
    await callUntilConditionIsMet(
        async () => { return await nearClient.getTransactionStatus(deployResult.hash); },
        (response) => { return response['status'] == 'Completed' },
        "Call account status until contract is deployed",
        TRANSACTION_COMPLETE_MAX_RETRIES
    );
};

const waitForNonceToIncrease = async (initialAccount) => {
    await callUntilConditionIsMet(
        async () => { return await account.viewAccount(initialAccount['account_id']); },
        (response) => { return response['nonce'] != initialAccount['nonce'] },
        "Call view account until nonce increases"
    );
};

// Generate some unique string with a given prefix using the alice nonce. 
const generateUniqueString = async (prefix) => {
    const viewAccountResponse = await account.viewAccount(aliceAccountName);
    return prefix + viewAccountResponse.nonce;
};
