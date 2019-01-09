/**
 * Stores keys in the browser local storage. This allows to retain keys between
 * browser sessions. Local storage likes to work with strings so we store public and private key separately.
 */

const LOCAL_STORAGE_SECRET_KEY_SUFFIX = "_public";
const LOCAL_STORAGE_PUBLIC_KEY_SUFFIX = "_secret";

class BrowserLocalStorageKeystore {
    constructor() {}

    static storageKeyForPublicKey(accountId) {
        return accountId + LOCAL_STORAGE_PUBLIC_KEY_SUFFIX;
    };

    static storageKeyForSecretKey(accountId) {
        return accountId + LOCAL_STORAGE_SECRET_KEY_SUFFIX;
    }

    async setKey(accountId, key) {
        window.localStorage.setItem(
            BrowserLocalStorageKeystore.storageKeyForPublicKey(accountId), key["public_key"]);
        window.localStorage.setItem(
            BrowserLocalStorageKeystore.storageKeyForSecretKey(accountId), key["secret_key"]);
    };

    async getKey(accountId) {
        return {
            public_key: window.localStorage.getItem(
                BrowserLocalStorageKeystore.storageKeyForPublicKey(accountId)),
            secret_key: window.localStorage.getItem(
                BrowserLocalStorageKeystore.storageKeyForSecretKey(accountId))
        };
    };
};

module.exports = BrowserLocalStorageKeystore;