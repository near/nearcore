
/**
 * Creates a new account with a random new key.
 */
exports.createRandomAccount = async function() {
    // Not implemented yet
};

/**
 * Retrieves account data by plain-text account id. 
 */
exports.viewAccount = async account_id => {
    return await viewAccount(account_id);
};


// Implementation
const superagent = require('superagent');

const viewAccount = async account_id => {
    const viewAccountResponse = await request('view_account', {
        account_id: account_id,
    });
    return viewAccountResponse;
}

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
