/**
 * Simple in-memory keystore for testing purposes.
 */
class InMemoryKeyStore {
    constructor() {
        this.keys = {};
    }

    async setKey(accountId, key) {
        this.keys[accountId] = key;
    }

    async getKey(accountId) {
        return this.keys[accountId];
    }
}

module.exports = InMemoryKeyStore;