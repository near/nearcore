const BrowserLocalStorageKeystore = require('../signing/browser_local_storage_key_store.js');

const  { createFakeStorage } = require('./test-utils');

const KeyPair = require('../signing/key_pair.js');

const NETWORK_ID_SINGLE_KEY = 'singlekeynetworkid';
const ACCOUNT_ID_SINGLE_KEY = 'singlekeyaccountid';
const KEYPAIR_SINGLE_KEY = new KeyPair('public', 'secret');

describe('Browser keystore', () => {
    let keyStore;

    beforeAll(async () => {
        keyStore = new BrowserLocalStorageKeystore(NETWORK_ID_SINGLE_KEY, createFakeStorage());
        await keyStore.setKey(ACCOUNT_ID_SINGLE_KEY, KEYPAIR_SINGLE_KEY);
    });

    test('Get account id from empty keystore', async () => {
        const key = await keyStore.getKey('someaccount', 'somenetowrk');
        expect(key).toBeNull();  
    });
    
    test('Get account id from a network with single key', async () => {
        const key = await keyStore.getKey(ACCOUNT_ID_SINGLE_KEY);
        expect(key).toEqual(KEYPAIR_SINGLE_KEY);
    });

    test('Add two keys to network and retrieve them', async () => {
        const networkId = 'twoKeyNetwork';
        const newNetworkKeystore = new BrowserLocalStorageKeystore(networkId, createFakeStorage());
        const accountId1 = 'acc1';
        const accountId2 = 'acc2';
        const key1Expected = new KeyPair('p1', 's1');
        const key2Expected = new KeyPair('p2', 's2');
        await newNetworkKeystore.setKey(accountId1, key1Expected);
        await newNetworkKeystore.setKey(accountId2, key2Expected);
        const key1 = await newNetworkKeystore.getKey(accountId1);
        const key2 = await newNetworkKeystore.getKey(accountId2);
        expect(key1).toEqual(key1Expected);
        expect(key2).toEqual(key2Expected);
    });

    test('Set key from json and get key', async () => {
        const networkId = 'setFromJson';
        const keystore = new BrowserLocalStorageKeystore(networkId, createFakeStorage());
        const accountId = 'acc1';
        const expectedKey = new KeyPair('p1', 's1');
        const json = {
            account_id : accountId,
            public_key: expectedKey.getPublicKey(),
            secret_key: expectedKey.getSecretKey(),
            network_id: networkId
        };
        await keystore.setKeyFromJson(json);
        const key = await keystore.getKey(accountId);
        expect(key).toEqual(expectedKey);
    });

    test('Set key from json wrong network', async () => {
        const networkId = 'setFromJson';
        const keystore = new BrowserLocalStorageKeystore(networkId, createFakeStorage());
        const accountId = 'acc1';
        const expectedKey = new KeyPair('p1', 's1');
        const json = {
            account_id : accountId,
            public_key: expectedKey.getPublicKey(),
            secret_key: expectedKey.getSecretKey(),
            network_id: 'other network'
        };
        try {
            await keystore.setKeyFromJson(json);
            fail('setting key for wrong network should have failed');
        } catch (e) {
            expect(e).toEqual(new Error('Setting key for a wrong network'));
        }
    });
});