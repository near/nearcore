const UnencryptedFileSystemKeyStore = require('../signing/unencrypted_file_system_keystore.js');

const KeyPair = require('../signing/key_pair.js');

const NETWORK_ID_SINGLE_KEY = 'singlekeynetworkid';
const ACCOUNT_ID_SINGLE_KEY = 'singlekeyaccountid';
const KEYPAIR_SINGLE_KEY = new KeyPair('public', 'secret');

describe('Unencrypted file system keystore', () => {
    let keyStore;

    beforeAll(async () => {
        keyStore = new UnencryptedFileSystemKeyStore('../tests', NETWORK_ID_SINGLE_KEY);
        await keyStore.setKey(ACCOUNT_ID_SINGLE_KEY, KEYPAIR_SINGLE_KEY);
    });

    test('Get all keys with empty network returns empty list', async () => {
        const emptyList = await new UnencryptedFileSystemKeyStore('../tests', 'emptynetowrk').getAccountIds();
        expect(emptyList).toEqual([]);
    });  
    
    test('Get all keys with single key in keystore', async () => {
        const accountIds = await keyStore.getAccountIds();
        expect(accountIds).toEqual([ACCOUNT_ID_SINGLE_KEY]);
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
        const newNetworkKeystore = new UnencryptedFileSystemKeyStore('../tests', networkId);
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
        const accountIds = await newNetworkKeystore.getAccountIds();
        expect(accountIds).toEqual([accountId1, accountId2]);
    });
});