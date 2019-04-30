/**
 * Access Key based signer that uses Wallet to authorize app on the account and receive the access key.
 */

const KeyPair = require('./signing/key_pair');
const BrowserLocalStorageKeystore = require('./signing/browser_local_storage_key_store');
const SimpleKeyStoreSigner = require('./signing/simple_key_store_signer');

const LOGIN_WALLET_URL_SUFFIX = '/login_v2/';
const LOCAL_STORAGE_KEY_SUFFIX = '_wallet_access_key';

/**
 * Access Key based signer that uses Wallet to authorize app on the account and receive the access key.
 * @example
 * // if importing WalletAccessKey directly
 * const walletAccount = new WalletAccessKey(contractName, walletBaseUrl)
 * // if importing in all of nearLib and calling from variable
 * const walletAccount = new nearlib.WalletAccessKey(contractName, walletBaseUrl)
 * // To access this signer globally
 * window.walletAccount = new nearlib.WalletAccessKey(config.contractName, walletBaseUrl);
 * // To provide custom signer where the keys would be stored
 * window.walletAccount = new nearlib.WalletAccessKey(config.contractName, walletBaseUrl, customSigner);
 */
class WalletAccessKey {
    constructor(appKeyPrefix, walletBaseUrl = 'https://wallet.nearprotocol.com', signer = null) {
        this._walletBaseUrl = walletBaseUrl;
        this._authDataKey = appKeyPrefix + LOCAL_STORAGE_KEY_SUFFIX;
        this._signer = signer || (new SimpleKeyStoreSigner(new BrowserLocalStorageKeystore()));

        this._authData = JSON.parse(window.localStorage.getItem(this._authDataKey) || '{}');

        if (!this.isSignedIn()) {
            this._tryInitFromUrl();
        }
    }

    /**
     * Returns true, if this WalletAccount is authorized with the wallet.
     * @example
     * walletAccount.isSignedIn();
     */
    isSignedIn() {
        return !!this._authData.accountId;
    }

    /**
     * Returns authorized Account ID.
     * @example
     * walletAccount.getAccountId();
     */
    getAccountId() {
        return this._authData.accountId || '';
    }

    /**
     * Redirects current page to the wallet authentication page.
     * @param {string} contractId contract ID of the application
     * @param {string} title name of the application
     * @param {string} successUrl url to redirect on success
     * @param {string} failureUrl url to redirect on failure
     */
    requestSignIn(contractId, title, successUrl, failureUrl) {
        this._authData.key = KeyPair.fromRandomSeed();
        const currentUrl = new URL(window.location.href);
        let newUrl = new URL(this._walletBaseUrl + LOGIN_WALLET_URL_SUFFIX);
        newUrl.searchParams.set('title', title);
        newUrl.searchParams.set('contract_id', contractId);
        newUrl.searchParams.set('public_key', this._authData.key.getPublicKey());
        newUrl.searchParams.set('success_url', successUrl || currentUrl.href);
        newUrl.searchParams.set('failure_url', failureUrl || currentUrl.href);
        newUrl.searchParams.set('app_url', currentUrl.origin);
        window.location.replace(newUrl.toString());
    }
    /**
     * Sign out from the current account
     * @example
     * walletAccount.signOut();
     */
    signOut() {
        if (this._authData.accountId) {
            this._signer.keyStore.setKey(this.getAccountId(), null).catch(console.error);
            this._authData = {};
            window.localStorage.removeItem(this._authDataKey);
        }
    }

    _saveAuthData() {
        window.localStorage.setItem(this._authDataKey, JSON.stringify(this._authData));
    }

    _tryInitFromUrl() {
        if (this._authData.key) {
            let currentUrl = new URL(window.location.href);
            let publicKey = currentUrl.searchParams.get('public_key') || '';
            let accountId = currentUrl.searchParams.get('account_id') || '';
            if (accountId && publicKey === this._authData.key.getPublicKey()) {
                this._signer.keyStore.setKey(accountId, this._authData.key);
                this._authData = {
                    accountId,
                    publicKey,
                };
                this._saveAuthData();
            }
        }
    }

    /**
     * Sign a buffer. If the key for originator is not present,
     * this operation will fail.
     * @param {Uint8Array} buffer
     * @param {string} originator
     */
    async signBuffer(buffer, originator) {
        if (!this.isSignedIn() || originator !== this.getAccountId()) {
            throw 'Unauthorized account_id ' + originator;
        }
        return await this._signer.signBuffer(buffer, originator);
    }

}

module.exports = WalletAccessKey;
