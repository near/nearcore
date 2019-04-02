const fs = require('fs');
const keyDir = './neardev';
const KeyPair = require('nearlib/signing/key_pair')
const {promisify} = require('util');

/**
 * Unencrypted file system key store.
 */
class UnencryptedFileSystemKeyStore {
    constructor(keyDir) {
        this.keyDir = keyDir;
    }

    /**
     * Set a key for an account with a given id on a given network.
     * @param {string} accountId 
     * @param {string} keypair 
     * @param {string} networkId 
     */
    async setKey(accountId, keypair, networkId) {
        if (!await promisify(fs.exists)(keyDir)){
            await promisify(fs.mkdir)(keyDir);
        }
        const keyFileContent = {
            public_key: keypair.getPublicKey(),
            secret_key: keypair.getSecretKey(),
            account_id: accountId
        };
        await promisify(fs.writeFile)(this.getKeyFilePath(networkId, accountId), JSON.stringify(keyFileContent));
    }

    /**
     * Get a single key for an account on a given network.
     * @param {string} accountId 
     * @param {string} networkId 
     */
    async getKey(accountId, networkId) {
        // Find keys/account id
        if (!await promisify(fs.exists)(this.getKeyFilePath(networkId, accountId))) {
            throw 'Key lookup failed. Please make sure you set up an account.';
        }
        const rawKey = await this.getRawKey(networkId, accountId);
        if (!rawKey.public_key || !rawKey.secret_key || !rawKey.account_id) {
            throw 'Deployment failed. neardev/devkey.json format problem. Please make sure file contains public_key, secret_key, and account_id".';
        }
        if (rawKey.account_id != accountId) {
            throw 'Deployment failed. Keystore contains data for wrong account.';
        }
        const result = new KeyPair(rawKey.public_key, rawKey.secret_key);
        return result;
    }

    /**
     * Returns all account ids for a particular network.
     */
    async getAccountIds(networkId) {
        if (!await promisify(fs.exists)(keyDir)) {
            return [];
        }
        const result = [];
        const dir = await promisify(fs.readdir)(keyDir);
        for (let i = 0; i < dir.length; i++) {
            if (dir[i].startsWith(networkId + "_")) {
                result.push(dir[i].substring(networkId.length + 1));
            }
        }
        return result;
    }

    async clear() {
        this.keys = {};
    }

    /**
     * Returns the key file path. The convention is to store the keys in file {networkId}.json
     * @param {string} networkId 
     */
    getKeyFilePath(networkId, accountId) {
        return keyDir + "/" + networkId + "_" + accountId;
    }

    async getRawKey(networkId, accountId) {
        return JSON.parse(await promisify(fs.readFile)(this.getKeyFilePath(networkId, accountId)));
    }
}

module.exports = UnencryptedFileSystemKeyStore;