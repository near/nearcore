const Near = require('./near');
const NearClient = require('./nearclient');
const Account = require('./account');
const SimpleKeyStoreSigner = require('./signing/simple_key_store_signer');
const BrowserLocalStorageKeystore = require('./signing/browser_local_storage_key_store');
const LocalNodeConnection = require('./local_node_connection');
const KeyPair = require('./signing/key_pair');
const sendJson = require('./internal/send-json');

const storageAccountIdKey = 'dev_near_user';

// This key will only be available on dev/test environments. Do not rely on it for anything that runs on mainnet.
const devKey = new KeyPair(
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
     * @param {string} options.networkId id associated with this network, for key management purposes.
     */
    connect: async function(options = {}) {
        // construct full options objects based on params, and fill in with defaults.
        this.options = Object.assign({deps: {}}, options);
        this.deps = this.options.deps;
        if (this.options.useDevAccount) {
            this.options.accountId = devAccountName;
            this.options.key = devKey;
        }
        this.options.helperUrl = this.options.helperUrl || this.options.baseUrl;
        if (!this.deps.createAccount) {
            if (this.options.helperUrl) {
                this.deps.createAccount = this.createAccountWithContractHelper.bind(this);
            } else {
                this.deps.createAccount = this.createAccountWithLocalNodeConnection.bind(this);
            }
        }
        this.options.networkId = this.options.networkId || 'localhost';
        this.options.nodeUrl = this.options.nodeUrl || (await this.getConfig()).nodeUrl || localNodeUrl;
        this.deps.keyStore = this.deps.keyStore || new BrowserLocalStorageKeystore(this.options.networkId);
        this.deps.storage = this.deps.storage || window.localStorage;

        const nearClient = new NearClient(
            new SimpleKeyStoreSigner(this.deps.keyStore), new LocalNodeConnection(this.options.nodeUrl));
        this.near = new Near(nearClient);
        if (this.options.accountId && this.options.key) {
            this.deps.keyStore.setKey(this.options.accountId, this.options.key);
        }
        if (!this.options.accountId) {
            await this.getOrCreateDevUser();
        }
        return this.near;
    },
    getOrCreateDevUser: async function () {
        let tempUserAccountId = this.deps.storage.getItem(storageAccountIdKey);
        const accountKey = await this.deps.keyStore.getKey(tempUserAccountId);
        if (tempUserAccountId && accountKey) {
            // Make sure the user actually exists with valid keys and recreate it if it doesn't
            const accountLib = new Account(this.near.nearClient);
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
        const keypair = KeyPair.fromRandomSeed();
        const createAccount = this.deps.createAccount ? this.deps.createAccount :
            async (accountId, newAccountPublicKey) =>
                this.createAccountWithContractHelper(await this.getConfig(), accountId, newAccountPublicKey);
        await createAccount.bind(this, tempUserAccountId, keypair.getPublicKey())();
        this.deps.keyStore.setKey(tempUserAccountId, keypair);
        this.deps.storage.setItem(storageAccountIdKey, tempUserAccountId);
        return tempUserAccountId;
    },
    get myAccountId() {
        return this.deps.storage.getItem(storageAccountIdKey);
    },
    /**
     * Function to create an account on local node. This will not work on non-dev environments.
     */
    createAccountWithLocalNodeConnection: async function (newAccountName, newAccountPublicKey) {
        const account = new Account(this.near.nearClient);
        this.deps.keyStore.setKey(devAccountName, devKey); // need to have dev account in key store to use this.
        const createAccountResponse = await account.createAccount(newAccountName, newAccountPublicKey, 1, devAccountName);
        await this.near.waitForTransactionResult(createAccountResponse);
    },
    /**
     * Function to create an account on near-hosted devnet using contract helper. This will not work on non-dev environments.
     */
    createAccountWithContractHelper: async function (newAccountId, publicKey) {
        return await sendJson('POST', `${this.options.helperUrl}/account`, {
            newAccountId: newAccountId,
            newAccountPublicKey: publicKey
        });
    }
};

function getCookie(name) {
    var v = document.cookie.match('(^|;) ?' + name + '=([^;]*)(;|$)');
    return v ? v[2] : null;
}
