const InMemoryKeyStore = require('../test-tools/in_memory_key_store.js');
const LocalNodeConnection = require('../local_node_connection')
const NearClient = require('../nearclient');
const Account = require('../account');
const Near = require('../near');
const fs = require('fs');


const aliceAccountName = 'alice.near';
const aliceKey = {
    public_key: "9AhWenZ3JddamBoyMqnTbp7yVbRuvqAv3zwfrWgfVRJE",
    secret_key: "2hoLMP9X2Vsvib2t4F1fkZHpFd6fHLr5q7eqGroRoNqdBKcPja2jCrmxW9uGBLXdTnbtZYibWe4NoFtB4Bk7LWg6"
};
const test_key_store = new InMemoryKeyStore();
test_key_store.setKey(aliceAccountName, aliceKey);
const localNodeConnection = new LocalNodeConnection();
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
    const newAccountName = await generateUniqueAccountId("create.account.test");
    const newAccountPublicKey = '9AhWenZ3JddamBoyMqnTbp7yVbRuvqAv3zwfrWgfVRJE';
    const createAccountResponse = await account.createAccount(newAccountName, newAccountPublicKey, 1, aliceAccountName);
    const expctedAccount = {
        nonce: 0,
        account_id: newAccountName,
        amount: 1,
        code_hash: 'GKot5hBsd81kMupNCXHaqbhv3huEbxAFMLnpcX2hniwn',
        stake: 0,
    };

    // try to read the account a few times to wait for the transaction to go through
    for (var viewAttempt = 0; viewAttempt < TEST_MAX_RETRIES; viewAttempt++) {
        try {
            const viewAccountResponse = await account.viewAccount(newAccountName);
            expect(viewAccountResponse).toEqual(expctedAccount);
            return; // success!
        } catch (_) {}
    }
    // exceeded retries. fail. 
    fail('exceeded number of retries for viewing account');
});

test('deploy contract and make function calls', async () => {
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
});

const waitForNonceToIncrease = async (initialAccount) => {
    for (let i = 0; i < TEST_MAX_RETRIES; i++) {
        try {
            const viewAccountResponse = await account.viewAccount(initialAccount['account_id']);
            if (viewAccountResponse['nonce'] != initialAccount['nonce']) { return; }
        } catch (_) {
            // Errors are expected, not logging to avoid spam
        }
    }
    fail('exceeded number of retries for viewing account ' + i);
}

const generateUniqueAccountId = async (prefix) => {
    const viewAccountResponse = await account.viewAccount(aliceAccountName);
    return prefix + viewAccountResponse.nonce;
}