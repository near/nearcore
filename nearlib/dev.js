const nearlib = require('./');
const sendJson = require('./internal/send-json');

const storageAccountIdKey = 'dev_near_user';

// This key will only be available on dev/test environments. Do not rely on it for anything that runs on mainnet.
const devKey = new nearlib.KeyPair(
    '22skMptHjFWNyuEWY22ftn2AbLPSYpmYwGJRGwpNHbTV',
    '2wyRcSwSuHtRVmkMCGjPwnzZmQLeXLzLLyED1NDMt4BjnKgQL6tF85yBx6Jr26D2dUNeC716RBoTxntVHsegogYw'
);
const devAccountName = 'alice.near';
const localNodeUrl = 'http://localhost:3030';

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
        this.deps = options.deps || {};
        this.deps.keyStore = this.deps.keyStore || new nearlib.BrowserLocalStorageKeystore();
        this.deps.storage = this.deps.storage || window.localStorage;
        const nodeUrl = options.nodeUrl || (await this.getConfig()).nodeUrl || localNodeUrl;
        const nearClient = new nearlib.NearClient(
            new nearlib.SimpleKeyStoreSigner(this.deps.keyStore), new nearlib.LocalNodeConnection(nodeUrl));
        this.near = new nearlib.Near(nearClient);
        if (options.accountId) {
            this.deps.keyStore.setKey(options.accountId, options.key);
        } else {
            await this.getOrCreateDevUser();
        }
        return this.near;
    },
    getOrCreateDevUser: async function () {
        let tempUserAccountId = this.deps.storage.getItem(storageAccountIdKey);
        const accountKey = await this.deps.keyStore.getKey(tempUserAccountId);
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
        const createAccount = this.deps.createAccount ? this.deps.createAccount :
            async (accountId, newAccountPublicKey) =>
                this.createAccountWithContractHelper(await this.getConfig(), accountId, newAccountPublicKey);
        await createAccount.bind(this, tempUserAccountId, keypair.getPublicKey())();
        this.deps.keyStore.setKey(tempUserAccountId, keypair);
        this.deps.storage.setItem(storageAccountIdKey, tempUserAccountId);
        return tempUserAccountId;
    },
    get myAccountId() {
        return window.localStorage.getItem(storageAccountIdKey);
    },
    getNear: function () {
        return this.near;
    },
    /**
     * Function to create an account on local node. This will not work on non-dev environments.
     */
    createAccountWithLocalNodeConnection: async function (newAccountName, newAccountPublicKey) {
        const account = new nearlib.Account(this.near.nearClient);
        this.deps.keyStore.setKey(devAccountName, devKey); // need to have dev account in key store to use this.
        const createAccountResponse = await account.createAccount(newAccountName, newAccountPublicKey, 1, devAccountName);
        await this.near.waitForTransactionResult(createAccountResponse);
    },
    createAccountWithContractHelper: async function (nearConfig, newAccountId, publicKey) {
        return await sendJson('POST', `${nearConfig.baseUrl}/account`, {
            newAccountId: newAccountId,
            newAccountPublicKey: publicKey
        });
    }
};

function getCookie(name) {
    var v = document.cookie.match('(^|;) ?' + name + '=([^;]*)(;|$)');
    return v ? v[2] : null;
}
