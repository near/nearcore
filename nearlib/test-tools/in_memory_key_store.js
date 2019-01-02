/**
 * Simple in-memory keystore for testing purposes.
 */
var keys = {};

exports.setKey = (account_id, key) => {
    keys[account_id] = key;
}

exports.getKey = account_id => {
    return keys[account_id];
}