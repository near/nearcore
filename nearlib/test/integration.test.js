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
const TEST_MAX_RETRIES = 10;
const TRANSACTION_COMPLETE_MAX_RETRIES = 100;

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
    await waitForTransactionToComplete(createAccountResponse);
    const expctedAccount = {
        nonce: 0,
        account_id: newAccountName,
        amount: 1,
        code_hash: 'GKot5hBsd81kMupNCXHaqbhv3huEbxAFMLnpcX2hniwn',
        stake: 0,
    };
    const result = await account.viewAccount(newAccountName);
    expect(result).toEqual(expctedAccount);
});

test('create account with a new key and then view account returns the created account', async () => {
    const newAccountName = await generateUniqueString('create.randomkey.test');
    const amount = 2;
    const aliceAccountBeforeCreation = await account.viewAccount(aliceAccountName);
    const createAccountResponse = await account.createAccountWithRandomKey(
        newAccountName,
        amount,
        aliceAccountName);
    await waitForTransactionToComplete(createAccountResponse);
    expect(createAccountResponse['key']).not.toBeFalsy();
    const expctedAccount = {
        nonce: 0,
        account_id: newAccountName,
        amount: amount,
        code_hash: 'GKot5hBsd81kMupNCXHaqbhv3huEbxAFMLnpcX2hniwn',
        stake: 0,
    };
    const result = await account.viewAccount(newAccountName);
    expect(result).toEqual(expctedAccount);
    const aliceAccountAfterCreation = await account.viewAccount(aliceAccountName);
    expect(aliceAccountAfterCreation.amount).toBe(aliceAccountBeforeCreation.amount - amount);
});

describe('with deployed contract', () => {
    let contract;
    let oldLog;
    let logs;
    beforeAll(async () => {
        // See README.md for details about this contract source code location.
        const data = [...fs.readFileSync('../tests/hello.wasm')];
        const deployResult = await nearjs.deployContract(
            aliceAccountName,
            'test_contract',
            data);
        await waitForContractToDeploy(deployResult);
        contract = await nearjs.loadContract('test_contract', {
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
            aliceAccountName,
            'test_contract',
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
            'test_contract',
            'setValue', // this is the function defined in hello.wasm file that we are calling
            setArgs);
        expect(scheduleResult.hash).not.toBeFalsy();
        await waitForTransactionToComplete(scheduleResult);
        const secondViewFunctionResult = await nearjs.callViewFunction(
            aliceAccountName,
            'test_contract',
            'getValue', // this is the function defined in hello.wasm file that we are calling
            {});
        expect(secondViewFunctionResult).toEqual(setCallValue);
    });

    test('can get logs from method result', async () => {
        await contract.generateLogs();
        expect(logs).toEqual(['[test_contract]: LOG: log1', '[test_contract]: LOG: log2']);
    });

    test('can get assert message from method result', async () => {
        await expect(contract.triggerAssert()).rejects.toThrow(/Transaction .+ failed.+expected to fail/);
        expect(logs).toEqual(["[test_contract]: LOG: log before assert",
            "[test_contract]: ABORT: \"expected to fail\" filename: \"main.ts\" line: 35 col: 2"]);
    });
});

const callUntilConditionIsMet = async (functToPoll, condition, description, maxRetries = TEST_MAX_RETRIES) => {
    for (let i = 0; i < maxRetries; i++) {
        try {
            const response = await functToPoll();
            if (condition(response)) {
                console.log('Success ' + description + ' in ' + (i + 1) + ' attempts.');
                return response;
            }
        } catch (e) {
            if (i == TEST_MAX_RETRIES - 1) {
                fail('exceeded number of retries for ' + description + '. Last error ' + e.toString());
            }
        }
        await sleep(500);
    }
    fail('exceeded number of retries for ' + description);
};

const waitForTransactionToComplete = async (submitTransactionResult) => {
    expect(submitTransactionResult.hash).not.toBeFalsy();
    console.log('Waiting for transaction', submitTransactionResult.hash);
    await callUntilConditionIsMet(
        async () => { return await nearjs.getTransactionStatus(submitTransactionResult.hash); },
        (response) => {
            if (response.result.status == 'Completed') {
                console.log('Transaction ' + submitTransactionResult.hash + ' completed');
                return true;
            } else {
                return false;
            }
        },
        'Call get transaction status until transaction is completed',
        TRANSACTION_COMPLETE_MAX_RETRIES
    );
};

const waitForContractToDeploy = async (deployResult) => {
    await callUntilConditionIsMet(
        async () => { return await nearjs.getTransactionStatus(deployResult.hash); },
        (response) => { return response['result']['status'] == 'Completed'; },
        'Call account status until contract is deployed',
        TRANSACTION_COMPLETE_MAX_RETRIES
    );
};

function sleep(time) {
    return new Promise(function (resolve) {
        setTimeout(resolve, time);
    });
}

// Generate some unique string with a given prefix using the alice nonce. 
const generateUniqueString = async (prefix) => {
    const viewAccountResponse = await account.viewAccount(aliceAccountName);
    return prefix + viewAccountResponse.nonce;
};
