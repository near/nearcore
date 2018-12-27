const account = require('../account');

const aliceAccount = {
    account_id: '3x9az88Dkbxa6tkKByxqEn7jBTJCJCD4dVvou49L24ET',
    amount: 100,
    stake: 0,
    nonce: 0,
    code_hash: '2Kkfab2QQ3GK7DSiuzcRF2H4vNSPKFfE9gkCDkPGumtV'
};

test('returns 1', () => {
    // Sanity check
    expect(account.returnOne()).toBe(1);
});

test('view account', () => {
    account.viewAccount("alice").then(
        (result) =>  expect(result).toEqual(aliceAccount));//expect(result.toBe(aliceAccount)));
   // expect(viewAccountResponse.toBe({}));
});