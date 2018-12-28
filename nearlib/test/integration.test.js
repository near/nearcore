const account = require('../account');

const aliceAccount = {
    account_id: 'alice.near',
    amount: 100,
    stake: 0,
    nonce: 0,
    code_hash: '2Kkfab2QQ3GK7DSiuzcRF2H4vNSPKFfE9gkCDkPGumtV'
};

test('view account', async () => {
    const viewAccountResponse = await account.viewAccount("alice.near");
    expect(viewAccountResponse).toEqual(aliceAccount);
});