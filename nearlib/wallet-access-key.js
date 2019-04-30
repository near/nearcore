/**
 * Wallet based account and signer that uses external wallet through the iframe to signs transactions.
 */

const { sha256 } = require('js-sha256');

const LOGIN_WALLET_URL_SUFFIX = '/login/';
const LOCAL_STORAGE_KEY_SUFFIX = '_wallet_access_key';

/**
 * Wallet based account and signer that uses external wallet through the iframe to sign transactions.
 * @example 
 * // if importing WalletAccount directly
 * const walletAccount = new WalletAccount(contractName, walletBaseUrl)
 * // if importing in all of nearLib and calling from variable 
 * const walletAccount = new nearlib.WalletAccount(contractName, walletBaseUrl)
 * // To access wallet globally use:
 * window.walletAccount = new nearlib.WalletAccount(config.contractName, walletBaseUrl);
 */
class WalletAccessKey {
    constructor(appKeyPrefix, simpleSigner, walletBaseUrl = 'https://wallet.nearprotocol.com') {
        this._walletBaseUrl = walletBaseUrl;
        this._authDataKey = appKeyPrefix + LOCAL_STORAGE_KEY_SUFFIX;
        this._simpleSigner = simpleSigner;

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
     * @param {string} contract_id contract ID of the application
     * @param {string} title name of the application
     * @param {string} success_url url to redirect on success
     * @param {string} failure_url url to redirect on failure
     * @example
     *   walletAccount.requestSignIn(
     *     myContractId,
     *     title,
     *     onSuccessHref,
     *     onFailureHref);
     */
    requestSignIn(contract_id, title, success_url, failure_url) {
        this._authData.key = await KeyPair.fromRandomSeed();
        const currentUrl = new URL(window.location.href);
        let newUrl = new URL(this._walletBaseUrl + LOGIN_WALLET_URL_SUFFIX);
        newUrl.searchParams.set('title', title);
        newUrl.searchParams.set('contract_id', contract_id);
        newUrl.searchParams.set('success_url', success_url || currentUrl.href);
        newUrl.searchParams.set('failure_url', failure_url || currentUrl.href);
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
            this._simpleSigner.keyStore.setKey(this.getAccountId(), null).catch(console.error);
            this._authData = {};
            window.localStorage.removeItem(this._authDataKey);
        }
    }

    _saveAuthData() {
        window.localStorage.setItem(this._authDataKey, JSON.stringify(this._authData));
    }

    _tryInitFromUrl() {
        let currentUrl = new URL(window.location.href);
        let publicKey = currentUrl.searchParams.get('public_key') || '';
        let accountId = currentUrl.searchParams.get('account_id') || '';
        if (accountId && publicKey === this._authData.key.getPublicKey()) {
            this._simpleSigner.keyStore.setKey(accountId, this._authData.key);
            this._authData = {
                accountId,
                publicKey,
            };
            this._saveAuthData();
        }
    }

    /**
     * Sign a transaction body. If the key for senderAccountId is not present,
     * this operation will fail.
     * @param {Uint8Array} buffer
     * @param {string} originator
     */
    async signBuffer(buffer, originator) {
        if (!this.isSignedIn() || originator !== this.getAccountId()) {
            throw 'Unauthorized account_id ' + originator;
        }
        return await this._simpleSigner.signBuffer(buffer, originator);
    }

}

module.exports = WalletAccessKey;
