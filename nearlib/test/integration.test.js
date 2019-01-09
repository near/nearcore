const InMemoryKeyStore = require('../test-tools/in_memory_key_store.js');
const LocalNodeConnection = require('../local_node_connection')
const NearClient = require('../nearclient');
const Account = require('../account');
const Near = require('../near');
const fs = require('fs');


const aliceAccountName = 'alice.near';
const aliceKey = {
    public_key: "FTEov54o3JFxgnrouLNo2uferbvkU7fHDJvt7ohJNpZY",
    secret_key: "N3LfWXp5ag8eKSTu9yvksvN8VriNJqJT72StfE6471N8ef4qCfXT668jkuBdchMJVcrcUysriM8vN1ShfS8bJRY"
};
const test_key_store = new InMemoryKeyStore();
test_key_store.setKey(aliceAccountName, aliceKey);
const localNodeConnection = new LocalNodeConnection("http://localhost:3030");
const nearClient = new NearClient(test_key_store, localNodeConnection);
const account = new Account(nearClient);
const nearjs = new Near(nearClient);
const TEST_MAX_RETRIES = 10;


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
    const createAccountResponse = await account.createAccountWithRandomKey(
        newAccountName,
        2,
        aliceAccountName);
    expect(createAccountResponse["key"]).not.toBeFalsy();
    console.log(createAccountResponse["key"])
    const expctedAccount = {
        nonce: 0,
        account_id: newAccountName,
        amount: 2,
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

test('deploy contract and make function calls', async () => {
    // Contract is currently living here https://studio.nearprotocol.com/?f=Wbe7Zvd
    const data = [...fs.readFileSync('../tests/hello.wasm')];  
    const initialAccount = await account.viewAccount(aliceAccountName);
    const deployResult = await nearjs.deployContract(
        aliceAccountName,
        "test_contract",
        data,
        "FTEov54o3JFxgnrouLNo2uferbvkU7fHDJvt7ohJNpZY");
    waitForNonceToIncrease(initialAccount);
    const args = {
        "name": "trex"
    };
    const viewFunctionResult = await nearjs.callViewFunction(
        aliceAccountName,
        "test_contract",
        "near_func_hello", // this is the function defined in hello.wasm file that we are calling
        args);
    expect(viewFunctionResult).toEqual("hello trex");

    var setCallValue = await generateUniqueString("setCallPrefix");
    const accountBeforeScheduleCall = await account.viewAccount(aliceAccountName);
    const setArgs = {
        "value": setCallValue
    }
    const scheduleResult = await nearjs.scheduleFunctionCall(
        0,
        aliceAccountName,
        "test_contract",
        "near_func_setValue", // this is the function defined in hello.wasm file that we are calling
        setArgs);
    const callViewFunctionGetValue = async () => { 
        return await nearjs.callViewFunction(
            aliceAccountName,
            "test_contract",
            "near_func_getValue", // this is the function defined in hello.wasm file that we are calling
            {});
    };
    const checkResult = (result) => {
        expect(result).toEqual(setCallValue);
        return true;
    }
    await callUntilConditionIsMet(
        callViewFunctionGetValue,
        checkResult,
        "Call getValue until result is equal to expected value");
});

const callUntilConditionIsMet = async (functToPoll, condition, description) => {
    for (let i = 0; i < TEST_MAX_RETRIES; i++) {
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
