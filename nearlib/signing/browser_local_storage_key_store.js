/**
 * Stores keys in the browser local storage. This allows to retain keys between
 * browser sessions. Local storage likes to work with strings so we store public and private key separately.
 */
const KeyPair = require('./key_pair');
const AccountInfo = require('./account_info');

const LOCAL_STORAGE_SECRET_KEY_SUFFIX = '_secretkey';
const LOCAL_STORAGE_PUBLIC_KEY_SUFFIX = '_publickey';


class BrowserLocalStorageKeystore {
    constructor(networkId = 'unknown', localStorage = window.localStorage) {
        this.networkId = networkId;
        this.localStorage = localStorage;
    }

    static storageKeyForPublicKey(accountId) {
        return accountId + '_' + this.networkId + LOCAL_STORAGE_PUBLIC_KEY_SUFFIX;
    }

    static storageKeyForSecretKey(accountId) {
        return accountId + '_' + this.networkId + LOCAL_STORAGE_SECRET_KEY_SUFFIX;
    }

    /**
     * Save the key in local storage. 
     * @param {string} accountId 
     * @param {KeyPair} key 
     */
    async setKey(accountId, key) {
        this.localStorage.setItem(
            BrowserLocalStorageKeystore.storageKeyForPublicKey(accountId), key.getPublicKey());
        this.localStorage.setItem(
            BrowserLocalStorageKeystore.storageKeyForSecretKey(accountId), key.getSecretKey());
    }

    async setKeyFromJson(json) {
        const accountInfo =  AccountInfo.fromJson(json);
        if (this.networkId != accountInfo.networkId) {
            throw new Error('Setting key for a wrong network');
        }
        this.setKey(accountInfo.accountId, accountInfo.keyPair);
    }

    /**
     * Get the key from local storage and return as KeyPair object.
     * @param {string} accountId 
     */
    async getKey(accountId) {
        const publicKey = this.localStorage.getItem(
            BrowserLocalStorageKeystore.storageKeyForPublicKey(accountId));
        const secretKey = this.localStorage.getItem(
            BrowserLocalStorageKeystore.storageKeyForSecretKey(accountId));
        if (!publicKey || !secretKey) {
            return null;
        }
        return new KeyPair(publicKey, secretKey);
    }

    static getAccounts() {
        return Object.keys(this.localStorage).map(function(key) {
            if (key.endsWith('_public')) {
                return key.substr(0, key.length() - 7);
            }
        });
    }
}

module.exports = BrowserLocalStorageKeystore;