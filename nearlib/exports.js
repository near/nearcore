const Near = require('./near');
const NearClient = require('./nearclient');
const Account = require('./account');
const BrowserLocalStorageKeystore = require('./signing/browser_local_storage_keystore');
const LocalNodeConnection = require('./local_node_connection');
const KeyPair = require('./signing/key_pair');


var nearLib = window.nearLib || {};

nearLib.Near = Near;
nearLib.NearClient = NearClient;
nearLib.Account = Account;
nearLib.BrowserLocalStorageKeystore = BrowserLocalStorageKeystore;
nearLib.LocalNodeConnection = LocalNodeConnection;
nearLib.KeyPair = KeyPair;

window.nearLib = nearLib;
