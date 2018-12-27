
/**
 * Creates a new account with a random new key.
 */
exports.createRandomAccount = async function() {
    // TODO: implement
};

/**
 * Retrieves account data by plain-text account id. 
 */
exports.viewAccount = async account_id => {
    return await viewAccount(await accountHash(account_id));
};

// TODO: this is to sanity check that tests are working. Remove soon.
exports.returnOne = function() {
    return 1;
}


// TODO: move impl to a diff file.

const bs58 = require('bs58');
const crypto = require('crypto');
const superagent = require('superagent');


const viewAccount = async account_id => {
    const viewAccountResponse = await request('view_account', {
        account_id: account_id,
    });
    return viewAccountResponse;
}

// TODO: remove once the no-hash lands
const sha256 = data => {
    const hash = crypto.createHash('sha256');
    hash.update(data, 'utf8');
    return hash.digest();
}

const accountHash = async str => {
    return bs58.encode(sha256(str));
};

const request = async (methodName, params) => {
    try {
        // TODO: make this configurable and testable
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
