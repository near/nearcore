const superagent = require('superagent');
const ed25519 = require('ed25519');
const bs58 = require('bs58');

const MAX_RETRIES = 3;
var keyStore;


/**
 * Builder method to specify a keystore.
 */
exports.withKeystore = keyStore => {
    this.keyStore = keyStore;
    return this;
}


/**
 * Creates a new account with a given name and key,
 */
exports.createAccount = async (newAccountId, publicKey, amount, originatorAccountId) => {
    const createAccountParams = {
        originator: originatorAccountId,
        new_account_id: newAccountId,
        amount: amount,
        public_key: publicKey,
    };

    const transactionResponse = await submitTransaction("create_account", createAccountParams);
    return transactionResponse;
};

/**
 * Retrieves account data by plain-text account id. 
 */
exports.viewAccount = async account_id => {
    return await viewAccount(account_id);
};


const submitTransaction = async (method, args) => {
    // TODO: Make sender param names consistent
    // TODO: https://github.com/nearprotocol/nearcore/issues/287
    const senderKeys = ['sender_account_id', 'originator_account_id', 'originator_id', 'sender', 'originator'];
    const sender = senderKeys.map(key => args[key]).find(it => !!it)
    const nonce = await getNonce(sender);
    for (let i = 0; i < MAX_RETRIES; i++) {
        const response = await request(method, Object.assign({}, args, { nonce }));
        const transaction = response.body;
        const signedTransaction = await signTransaction(transaction, sender);
        const submitResponse = await request('submit_transaction', signedTransaction);
    }
    return { nonce: nonce };
}

const getNonce = async account_id => {
    return (await viewAccount(account_id)).nonce + 1;
}

const viewAccount = async account_id => {
    const viewAccountResponse = await request('view_account', {
        account_id: account_id,
    });
    return viewAccountResponse;
}

const signTransaction = async (transaction, sender) => {
    const stringifiedTxn = JSON.stringify(transaction);
    const encodedKey = this.keyStore.getKey(sender).secret_key;
    const key = bs58.decode(encodedKey);
    const signature = [...ed25519.Sign(new Buffer(stringifiedTxn, 'utf8'), key)];
    const response = { 
        body: transaction,
        signature: signature
    };
    return response;
};

const request = async (methodName, params) => {
    try {
        // TODO: (issue 320) make this configurable and testable
        const response = await superagent
            .post(`http://localhost:3030/${methodName}`)
            .use(require('superagent-logger'))
            .send(params);
        return JSON.parse(response.text);
    } catch(e) {
        console.error("error calling rpc ", e);
        throw e;
    }
};
