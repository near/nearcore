const nearlib = require('./');
const localStorageAccountIdKey = 'dev_near_user';
module.exports = {
    getConfig: async function() {
        return JSON.parse(decodeURIComponent(getCookie('fiddleConfig')));
    },
    initDefault: async function() {
        const studioConfig = await this.getConfig();
        return nearlib.Near.createDefaultConfig(studioConfig.nodeUrl);
    }, 
    getOrCreateDevUser: async function () {
        let tempUserAccountId = window.localStorage.getItem(localStorageAccountIdKey);
        if (tempUserAccountId) {
            return tempUserAccountId;
        }

        tempUserAccountId = 'devuser' + Date.now();
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

async function sendJson(method, url, json) {
    const response = await fetch(url, {
        credentials: 'include',
        method: method,
        body: JSON.stringify(json),
        headers: new Headers({ 'Content-type': 'application/json; charset=utf-8' })
    });
    if (!response.ok) {
        throw new Error(await response.text());
    }
    if (response.status === 204) {
        // No Content
        return null;
    }
    return await response.json();
}

function getCookie(name) {
    var v = document.cookie.match('(^|;) ?' + name + '=([^;]*)(;|$)');
    return v ? v[2] : null;
}