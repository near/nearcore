const nearlib = require('./');
const sendJson = require('./internal/send-json');

const localStorageAccountIdKey = 'dev_near_user';
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