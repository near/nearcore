const nearlib = require('./');
const sendJson = require('./internal/send-json');

const localStorageAccountIdKey = 'dev_near_user';

// This key will only be available on dev/test environments. Do not rely on it for anything that runs on mainnet.
const devKey = new nearlib.KeyPair(
    '22skMptHjFWNyuEWY22ftn2AbLPSYpmYwGJRGwpNHbTV',
    '2wyRcSwSuHtRVmkMCGjPwnzZmQLeXLzLLyED1NDMt4BjnKgQL6tF85yBx6Jr26D2dUNeC716RBoTxntVHsegogYw'
);
const devAccountName = 'alice.near';

module.exports = {
    getConfig: async function() {
        return JSON.parse(decodeURIComponent(getCookie('fiddleConfig')));
    },
    /**
     * Create a connection which can perform operations on behalf of a given account.
     * @param {object} options object to pass named parameters.
     * @param {Object} options.nodeUrl specifies node url. accountId specifies account id. key_pair is the key pair for account
     * @param {boolean} options.useDevAccount specify to use development account to create accounts / deploy contracts. Should be used only on TestNet.
     * @param {string} options.accountId account ID to use.
     */
    connect: async function(options) {
        if (options.useDevAccount) {
            options.accountId = devAccountName;
            options.key = devKey;
        }
        const keyStore = (options.deps && options.deps.keyStore) || new nearlib.BrowserLocalStorageKeystore();
        const nodeUrl = options.nodeUrl || this.getConfig().nodeUrl || 'http://localhost:3030';
        const nearClient = new nearlib.NearClient(
            new nearlib.SimpleKeyStoreSigner(keyStore), new nearlib.LocalNodeConnection(nodeUrl));
        const near = new nearlib.Near(nearClient);
        if (options.accountId) {
            keyStore.setKey(options.accountId, options.key);
        } else {
            this.getOrCreateDevUser(near, keyStore);
        }
        return near;
    },
    getOrCreateDevUser: async function (near, keyStore = null) {
        let tempUserAccountId = window.localStorage.getItem(localStorageAccountIdKey);
        if (tempUserAccountId) {
            // Make sure the user actually exists and recreate it if it doesn't
            const accountLib = new nearlib.Account(near.nearClient);
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
        await sendJson('POST', `${nearConfig.baseUrl}/account`, {
            newAccountId: tempUserAccountId,
            newAccountPublicKey: keypair.getPublicKey()
        });
        const localKeyStore = keyStore || new nearlib.BrowserLocalStorageKeystore();
        localKeyStore.setKey(tempUserAccountId, keypair);
        window.localStorage.setItem(localStorageAccountIdKey, tempUserAccountId);
        return tempUserAccountId;
    },
    get myAccountId() {
        return window.localStorage.getItem(localStorageAccountIdKey);
    }
};

function getCookie(name) {
    var v = document.cookie.match('(^|;) ?' + name + '=([^;]*)(;|$)');
    return v ? v[2] : null;
}
