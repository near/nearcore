const aliceAccountName = 'alice.near';
// every new account has this codehash
const newAccountCodeHash = 'GKot5hBsd81kMupNCXHaqbhv3huEbxAFMLnpcX2hniwn';
const storageAccountIdKey = 'dev_near_user';

const createFakeStorage = function() {
    let store = {};
    return {
        getItem: function(key) {
            return store[key];
        },
        setItem: function(key, value) {
            store[key] = value.toString();
        },
        clear: function() {
            store = {};
        },
        removeItem: function(key) {
            delete store[key];
        }
    };
};

function sleep(time) {
    return new Promise(function (resolve) {
        setTimeout(resolve, time);
    });
}

module.exports = { aliceAccountName, newAccountCodeHash, storageAccountIdKey, createFakeStorage, sleep };
