const fs = require('fs');
const keyDir = './neardev';
const AccountInfo = require('./account_info');
const {promisify} = require('util');

/**
 * Unencrypted file system key store.
 */
class UnencryptedFileSystemKeyStore {
    constructor(keyDir, networkId) {
        this.keyDir = keyDir;
        this.networkId = networkId;
    }

    /**
     * Set a key for an account with a given id on a given network.
     * @param {string} accountId 
     * @param {string} keypair 
     */
    async setKey(accountId, keypair) {
        if (!await promisify(fs.exists)(keyDir)){
            await promisify(fs.mkdir)(keyDir);
        }
        const accountInfo = new AccountInfo(accountId, keypair, this.networkId);
        const keyFileContent = accountInfo.toJSON();
        await promisify(fs.writeFile)(this.getKeyFilePath(accountId), JSON.stringify(keyFileContent));
    }

    /**
     * Get a single key for an account on a given network.
     * @param {string} accountId 
     */
    async getKey(accountId) {
        // Find keys/account id
        if (!await promisify(fs.exists)(this.getKeyFilePath(accountId))) {
            return null;
        }
        const json = await this.getRawKey(accountId);
        return AccountInfo.fromJson(json).keyPair;
    }

    /**
     * Returns all account ids for a particular network.
     */
    async getAccountIds() {
        if (!await promisify(fs.exists)(keyDir)) {
            return [];
        }
        const result = [];
        const dir = await promisify(fs.readdir)(keyDir);
        for (let i = 0; i < dir.length; i++) {
            if (dir[i].startsWith(this.networkId + '_')) {
                result.push(dir[i].substring(this.networkId.length + 1));
            }
        }
        return result;
    }

    async clear() {
        this.keys = {};
    }

    /**
     * Returns the key file path. The convention is to store the keys in file {networkId}.json
     */
    getKeyFilePath(accountId) {
        return keyDir + '/' + this.networkId + '_' + accountId;
    }

    async getRawKey(accountId) {
        return JSON.parse(await promisify(fs.readFile)(this.getKeyFilePath(accountId)));
    }
}

module.exports = UnencryptedFileSystemKeyStore;