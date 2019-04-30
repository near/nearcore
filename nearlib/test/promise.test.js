const { Account, KeyPair, InMemoryKeyStore } = require('../');
const  { aliceAccountName, createFakeStorage } = require('./test-utils');
const dev = require('../dev');
const fs = require('fs');
let nearjs;
let account;
let mainTestAccountName;
let keyStore;
let storage;

beforeAll(async () => {
    keyStore = new InMemoryKeyStore('somenetwork');
    storage = createFakeStorage();
    nearjs = await dev.connect({
        nodeUrl: 'http://localhost:3030',
        useDevAccount: true,
        deps: { keyStore, storage },
        network: 'somenetwork'
    });

    account = new Account(nearjs.nearClient);

    mainTestAccountName = 'dev_acc_' + Math.random();
    const keyWithRandomSeed = KeyPair.fromRandomSeed();
    const createAccountResponse = await account.createAccount(
        mainTestAccountName,
        keyWithRandomSeed.getPublicKey(),
        1000,
        aliceAccountName);
    await nearjs.waitForTransactionResult(createAccountResponse);
    await keyStore.setKey(mainTestAccountName, keyWithRandomSeed);
});

describe('with promises', () => { 
    let contract, contract1, contract2;
    let oldLog;
    let logs;
    let contractName = 'test_' + Date.now();
    let contractName1 = 'test_' + Math.random();
    let contractName2 = 'test_' + Math.random();

    jasmine.DEFAULT_TIMEOUT_INTERVAL = 20000;

    const deploy = async (contractName) => {
        const keyWithRandomSeed = KeyPair.fromRandomSeed();
        const createAccountResponse = await account.createAccount(
            contractName,
            keyWithRandomSeed.getPublicKey(),
            1,
            mainTestAccountName);
        await nearjs.waitForTransactionResult(createAccountResponse);
        keyStore.setKey(contractName, keyWithRandomSeed);
        const data = [...fs.readFileSync('../tests/hello.wasm')];
        await nearjs.waitForTransactionResult(
            await nearjs.deployContract(contractName, data));
        return await nearjs.loadContract(contractName, {
            sender: mainTestAccountName,
            viewMethods: ['getLastResult'],
            changeMethods: ['callPromise']
        });
    };

    beforeAll(async () => {
        // See README.md for details about this contract source code location.
        contract = await deploy(contractName);
        contract1 = await deploy(contractName1);
        contract2 = await deploy(contractName2);
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

    // -> means async call 
    // => means callback

    test('single promise, no callback (A->B)', async () => {
        const result = await contract.callPromise({args: {
            receiver: contractName1,
            methodName: 'callbackWithName',
            args: null,
            additionalMana: 0,
            callback: null,
            callbackArgs: null,
            callbackAdditionalMana: 0,
        }});
        expect(result.status).toBe('Completed');
        const lastResult = await contract1.getLastResult();
        expect(lastResult).toEqual({
            rs: [],
            n: contractName1,
        });
    });

    test('single promise with callback (A->B=>A)', async () => {
        const result = await contract.callPromise({args: {
            receiver: contractName1,
            methodName: 'callbackWithName',
            args: null,
            additionalMana: 0,
            callback: 'callbackWithName',
            callbackArgs: null,
            callbackAdditionalMana: 0,
        }});
        expect(result.status).toBe('Completed');
        const lastResult1 = await contract1.getLastResult();
        expect(lastResult1).toEqual({
            rs: [],
            n: contractName1,
        });
        const lastResult = await contract.getLastResult();
        expect(lastResult).toEqual({
            rs: [{
                ok: true,
                r: lastResult1,
            }],
            n: contractName,
        });
    });

    test('two promises, no callbacks (A->B->C)', async () => {
        const result = await contract.callPromise({args: {
            receiver: contractName1,
            methodName: 'callPromise',
            args: {
                receiver: contractName2,
                methodName: 'callbackWithName',
                args: null,
                additionalMana: 0,
                callback: null,
                callbackArgs: null,
                callbackAdditionalMana: 0,
            },
            additionalMana: 1,
            callback: null,
            callbackArgs: null,
            callbackAdditionalMana: 0,
        }});
        expect(result.status).toBe('Completed');
        const lastResult2 = await contract2.getLastResult();
        expect(lastResult2).toEqual({
            rs: [],
            n: contractName2,
        });
    });

    test('two promises, with two callbacks (A->B->C=>B=>A)', async () => {
        const result = await contract.callPromise({args: {
            receiver: contractName1,
            methodName: 'callPromise',
            args: {
                receiver: contractName2,
                methodName: 'callbackWithName',
                args: null,
                additionalMana: 0,
                callback: 'callbackWithName',
                callbackArgs: null,
                callbackAdditionalMana: 0,
            },
            additionalMana: 2,
            callback: 'callbackWithName',
            callbackArgs: null,
            callbackAdditionalMana: 0,
        }});
        expect(result.status).toBe('Completed');
        const lastResult2 = await contract2.getLastResult();
        expect(lastResult2).toEqual({
            rs: [],
            n: contractName2,
        });
        const lastResult1 = await contract1.getLastResult();
        expect(lastResult1).toEqual({
            rs: [{
                ok: true,
                r: lastResult2,
            }],
            n: contractName1,
        });
        const lastResult = await contract.getLastResult();
        expect(lastResult).toEqual({
            rs: [{
                ok: true,
                r: lastResult1,
            }],
            n: contractName,
        });
    });

    test('cross contract call with callbacks (A->B->A=>B=>A)', async () => {
        const result = await contract.callPromise({args: {
            receiver: contractName1,
            methodName: 'callPromise',
            args: {
                receiver: contractName,
                methodName: 'callbackWithName',
                args: null,
                additionalMana: 0,
                callback: 'callbackWithName',
                callbackArgs: null,
                callbackAdditionalMana: 0,
            },
            additionalMana: 2,
            callback: 'callbackWithName',
            callbackArgs: null,
            callbackAdditionalMana: 0,
        }});
        expect(result.status).toBe('Completed');
        const lastResult1 = await contract1.getLastResult();
        expect(lastResult1).toEqual({
            rs: [{
                ok: true,
                r: {
                    rs: [],
                    n: contractName,
                },
            }],
            n: contractName1,
        });
        const lastResult = await contract.getLastResult();
        expect(lastResult).toEqual({
            rs: [{
                ok: true,
                r: lastResult1,
            }],
            n: contractName,
        });
    });

    test('2 promises with 1 skipped callbacks (A->B->C=>A)', async () => {
        const result = await contract.callPromise({args: {
            receiver: contractName1,
            methodName: 'callPromise',
            args: {
                receiver: contractName2,
                methodName: 'callbackWithName',
                args: null,
                additionalMana: 0,
                callback: null,
                callbackArgs: null,
                callbackAdditionalMana: 0,
            },
            additionalMana: 1,
            callback: 'callbackWithName',
            callbackArgs: null,
            callbackAdditionalMana: 0,
        }});
        expect(result.status).toBe('Completed');
        const lastResult2 = await contract2.getLastResult();
        expect(lastResult2).toEqual({
            rs: [],
            n: contractName2,
        });
        const lastResult = await contract.getLastResult();
        expect(lastResult).toEqual({
            rs: [{
                ok: true,
                r: lastResult2,
            }],
            n: contractName,
        });
    });

    test('single promise with callback using deposit (empty method name) (A->B=>A)', async () => {
        const result = await contract.callPromise({args: {
            receiver: contractName1,
            methodName: '',  // Deposit (no execution)
            args: null,
            additionalMana: 0,
            callback: 'callbackWithName',
            callbackArgs: null,
            callbackAdditionalMana: 0,
        }});
        expect(result.status).toBe('Completed');
        const lastResult = await contract.getLastResult();
        expect(lastResult).toEqual({
            rs: [{
                ok: true,
                r: null,
            }],
            n: contractName,
        });
    });

    test('2 promises with 1 skipped callbacks using deposit (empty method name) (A->B->C=>A)', async () => {
        const result = await contract.callPromise({args: {
            receiver: contractName1,
            methodName: 'callPromise',
            args: {
                receiver: contractName2,
                methodName: '',  // Deposit (no execution)
                args: null,
                additionalMana: 0,
                callback: null,
                callbackArgs: null,
                callbackAdditionalMana: 0,
            },
            additionalMana: 1,
            callback: 'callbackWithName',
            callbackArgs: null,
            callbackAdditionalMana: 0,
        }});
        expect(result.status).toBe('Completed');
        const lastResult = await contract.getLastResult();
        expect(lastResult).toEqual({
            rs: [{
                ok: true,
                r: null,
            }],
            n: contractName,
        });
    });
});
