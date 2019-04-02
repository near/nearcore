/**
 * Simple in-memory keystore for testing purposes.
 */
class InMemoryKeyStore {
    constructor() {
        this.keys = {};
    }

    async setKey(accountId, key, networkId) {
        this.keys[accountId + "_" + networkId] = key;
    }

    async getKey(accountId, networkId) {
        return this.keys[accountId  + "_" + networkId];
    }

    async clear() {
        this.keys = {};
    }
}

module.exports = InMemoryKeyStore;