const nearlib = require('./');
const sendJson = require('./internal/send-json');

const localStorageAccountIdKey = 'dev_near_user';

// This key will only be available on dev/test environments. Do not rely on it for anything that runs on mainnet.
const aliceKey = new nearlib.KeyPair(
    '22skMptHjFWNyuEWY22ftn2AbLPSYpmYwGJRGwpNHbTV',
    '2wyRcSwSuHtRVmkMCGjPwnzZmQLeXLzLLyED1NDMt4BjnKgQL6tF85yBx6Jr26D2dUNeC716RBoTxntVHsegogYw'
);
const aliceAccountName = "alice.near";

module.exports = {
    getConfig: async function() {
        return JSON.parse(decodeURIComponent(getCookie('fiddleConfig')));
    },
    connect: async function(nodeUrl) {
        const studioConfig = await this.getConfig();
        const near = nearlib.Near.createDefaultConfig(nodeUrl || studioConfig.nodeUrl);
        await this.getOrCreateDevUser(near);
        return near;
    },
    /**
     * Create a connection which can perform operations on behalf of alice.near. This is an
     * account that only exists on dev enviornments, so this should never be called on mainnet.
     * @param {string} nodeUrl
     */
    connectWithAlice: async function(nodeUrl) {
        return this.connectWithAccount(nodeUrl, aliceAccountName, aliceKey);
    },
    /**
     * Create a connection which can perform operations on behalf of a given account.
     * @param {string} nodeUrl
     * @param {string} accountId
     * @param {KeyPair} key
     */
    connectWithAccount: async function(nodeUrl, accountId, key) {
        const studioConfig = await this.getConfig();
        const keyStore = new nearlib.InMemoryKeyStore();
        keyStore.setKey(accountId, key);
        const nearClient = new nearlib.NearClient(
            new nearlib.SimpleKeyStoreSigner(keyStore), new nearlib.LocalNodeConnection(nodeUrl || studioConfig.nodeUrl));
        return new nearlib.Near(nearClient);
    },
    getOrCreateDevUser: async function (near) {
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
        const keyStore = new nearlib.BrowserLocalStorageKeystore();
        keyStore.setKey(tempUserAccountId, keypair);
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