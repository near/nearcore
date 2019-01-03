/**
 * Simple in-memory keystore for testing purposes.
 */

class InMemoryKeyStore {

    constructor() {
        this.keys = {};
    }

    async setKey(accontId, key) {
        this.keys[accontId] = key;
    }

    async getKey(accontId) {
        return this.keys[accontId];
    }
};

module.exports = InMemoryKeyStore;