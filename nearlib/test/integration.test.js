const account = require('../account');
const uuidV4 = require('uuid/v4');

const aliceAccountName = 'alice.near';
const MAX_RETRIES = 3;

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
    for (var viewAttempt = 0; viewAttempt < 1; viewAttempt++) {
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
        // exceeded retries. fail.
        fail('exceeded number of retries for viewing account');
    }
});

const generateUniqueAccountId = async (prefix) => {
    const viewAccountResponse = await account.viewAccount(aliceAccountName);
    return prefix + viewAccountResponse.nonce;
}