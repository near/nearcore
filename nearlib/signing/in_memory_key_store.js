/**
 * Simple in-memory keystore for testing purposes.
 */

class InMemoryKeyStore {

    constructor() {
        this.keys = {};
    }

    async setKeyPair(accontId, key) {
        this.keys[accontId] = key;
    }

    async getKeyPair(accontId) {
        return this.keys[accontId];
    }
};

module.exports = InMemoryKeyStore;