const nearlib = require('./');
const sendJson = require('./internal/send-json');

const storageAccountIdKey = 'dev_near_user';

// This key will only be available on dev/test environments. Do not rely on it for anything that runs on mainnet.
const devKey = new nearlib.KeyPair(
    '22skMptHjFWNyuEWY22ftn2AbLPSYpmYwGJRGwpNHbTV',
    '2wyRcSwSuHtRVmkMCGjPwnzZmQLeXLzLLyED1NDMt4BjnKgQL6tF85yBx6Jr26D2dUNeC716RBoTxntVHsegogYw'
);
const devAccountName = 'alice.near';

module.exports = {
    getConfig: async function() {
        return JSON.parse(decodeURIComponent(getCookie('fiddleConfig'))) || {};
    },
    /**
     * Create a connection which can perform operations on behalf of a given account.
     * @param {object} options object to pass named parameters.
     * @param {Object} options.nodeUrl specifies node url. accountId specifies account id. key_pair is the key pair for account
     * @param {boolean} options.useDevAccount specify to use development account to create accounts / deploy contracts. Should be used only on TestNet.
     * @param {string} options.accountId account ID to use.
     */
    connect: async function(options = {}) {
        if (options.useDevAccount) {
            options.accountId = devAccountName;
            options.key = devKey;
        }
        const keyStore = (options.deps && options.deps.keyStore) || new nearlib.BrowserLocalStorageKeystore();
        const nodeUrl = options.nodeUrl || (await this.getConfig()).nodeUrl || 'http://localhost:3030';
        const nearClient = new nearlib.NearClient(
            new nearlib.SimpleKeyStoreSigner(keyStore), new nearlib.LocalNodeConnection(nodeUrl));
        this.near = new nearlib.Near(nearClient);
        if (options.accountId) {
            keyStore.setKey(options.accountId, options.key);
        } else {
            await this.getOrCreateDevUser(options.deps);
        }
        return this.near;
    },
    getOrCreateDevUser: async function (deps = {}) {
        const storage = deps.storage || window.localStorage;
        let tempUserAccountId = storage.getItem(storageAccountIdKey);
        const localKeyStore = deps.keyStore || new nearlib.BrowserLocalStorageKeystore();
        const accountKey = await localKeyStore.getKey(tempUserAccountId);
        if (tempUserAccountId && accountKey) {
            // Make sure the user actually exists with valid keys and recreate it if it doesn't
            const accountLib = new nearlib.Account(this.near.nearClient);
            try {
                await accountLib.viewAccount(tempUserAccountId);
                return tempUserAccountId;
            } catch (e) {
                console.log('Error looking up temp account', e);
                // Something went wrong! Recreate user by continuing the flow
            }
        } else {
            tempUserAccountId = 'devuser' + Date.now();
        }
        const keypair = await nearlib.KeyPair.fromRandomSeed();
        const nearConfig = await this.getConfig();
        const createAccount = deps.createAccount ? deps.createAccount :
            async (accountId, newAccountPublicKey) =>
                createAccountWithContractHelper(nearConfig, accountId, newAccountPublicKey);
        await createAccount(tempUserAccountId, keypair.getPublicKey());
        localKeyStore.setKey(tempUserAccountId, keypair);
        storage.setItem(storageAccountIdKey, tempUserAccountId);
        return tempUserAccountId;
    },
    get myAccountId() {
        return window.localStorage.getItem(storageAccountIdKey);
    }
};

async function createAccountWithContractHelper(nearConfig, newAccountId, publicKey) {
    return await sendJson('POST', `${nearConfig.baseUrl}/account`, {
        newAccountId: newAccountId,
        newAccountPublicKey: publicKey
    });
}

function getCookie(name) {
    var v = document.cookie.match('(^|;) ?' + name + '=([^;]*)(;|$)');
    return v ? v[2] : null;
}
