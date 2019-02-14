/**
 * Stores keys in the browser local storage. This allows to retain keys between
 * browser sessions. Local storage likes to work with strings so we store public and private key separately.
 */
const KeyPair = require('./key_pair');

const LOCAL_STORAGE_SECRET_KEY_SUFFIX = '_secretkey';
const LOCAL_STORAGE_PUBLIC_KEY_SUFFIX = '_publickey';


class BrowserLocalStorageKeystore {
    constructor() {}

    static storageKeyForPublicKey(accountId) {
        return accountId + LOCAL_STORAGE_PUBLIC_KEY_SUFFIX;
    }

    static storageKeyForSecretKey(accountId) {
        return accountId + LOCAL_STORAGE_SECRET_KEY_SUFFIX;
    }

    /**
     * Save the key in local storage. 
     * @param {string} accountId 
     * @param {KeyPair} key 
     */
    async setKey(accountId, key) {
        window.localStorage.setItem(
            BrowserLocalStorageKeystore.storageKeyForPublicKey(accountId), key.getPublicKey());
        window.localStorage.setItem(
            BrowserLocalStorageKeystore.storageKeyForSecretKey(accountId), key.getSecretKey());
    }

    /**
     * Get the key from local storage and return as KeyPair object.
     * @param {string} accountId 
     */
    async getKey(accountId) {
        return new KeyPair(
            window.localStorage.getItem(
                BrowserLocalStorageKeystore.storageKeyForPublicKey(accountId)),
            window.localStorage.getItem(
                BrowserLocalStorageKeystore.storageKeyForSecretKey(accountId))
        );
    }

    static getAccounts() {
        return Object.keys(window.localStorage).map(function(key) {
            if (key.endsWith('_public')) {
                return key.substr(0, key.length() - 7);
            }
        });
    }
}

module.exports = BrowserLocalStorageKeystore;