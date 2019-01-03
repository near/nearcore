const InMemoryKeyStore = require('../test-tools/in_memory_key_store.js');
const Account = require('../account');
const aliceAccountName = 'alice.near';
const aliceKey = {
    public_key: "9AhWenZ3JddamBoyMqnTbp7yVbRuvqAv3zwfrWgfVRJE",
    secret_key: "2hoLMP9X2Vsvib2t4F1fkZHpFd6fHLr5q7eqGroRoNqdBKcPja2jCrmxW9uGBLXdTnbtZYibWe4NoFtB4Bk7LWg6"
};
const test_key_store = new InMemoryKeyStore();
test_key_store.setKey(aliceAccountName, aliceKey);
const account = new Account(test_key_store);
const TEST_MAX_RETRIES = 5;


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

    // try to read the account a few times to wait for the transaction to go through
    for (var viewAttempt = 0; viewAttempt < TEST_MAX_RETRIES; viewAttempt++) {
        try {
            const viewAccountResponse = await account.viewAccount(newAccountName);
            const expctedAccount = {
                nonce: 0,
                account_id: newAccountName,
                amount: 1,
                code_hash: 'GKot5hBsd81kMupNCXHaqbhv3huEbxAFMLnpcX2hniwn',
                stake: 0,
            };

            expect(viewAccountResponse).toEqual(expctedAccount);
            return; // success!
        } catch (_) {}
    }
    // exceeded retries. fail. 
    fail('exceeded number of retries for viewing account');
});

const generateUniqueAccountId = async (prefix) => {
    const viewAccountResponse = await account.viewAccount(aliceAccountName);
    return prefix + viewAccountResponse.nonce;
}