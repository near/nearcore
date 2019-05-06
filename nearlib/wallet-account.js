/**
 * Wallet based account and signer that uses external wallet through the iframe to sign transactions.
 */

const { sha256 } = require('js-sha256');
const { FunctionCallTransaction } = require('./protos');

const EMBED_WALLET_URL_SUFFIX = '/embed/';
const LOGIN_WALLET_URL_SUFFIX = '/login/';
const RANDOM_ALPHABET = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';
const REQUEST_ID_LENGTH = 32;

const LOCAL_STORAGE_KEY_SUFFIX = '_wallet_auth_key';

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
class WalletAccount {

    constructor(appKeyPrefix, walletBaseUrl = 'https://wallet.nearprotocol.com') {
        this._walletBaseUrl = walletBaseUrl;
        this._authDataKey = appKeyPrefix + LOCAL_STORAGE_KEY_SUFFIX;

        this._initHtmlElements();
        this._signatureRequests = {};
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
        this._authData = {};
        window.localStorage.removeItem(this._authDataKey);
    }

    _tryInitFromUrl() {
        let currentUrl = new URL(window.location.href);
        let authToken = currentUrl.searchParams.get('auth_token') || '';
        let accountId = currentUrl.searchParams.get('account_id') || '';
        if (!!authToken && !!accountId) {
            this._authData = {
                authToken,
                accountId,
            };
            window.localStorage.setItem(this._authDataKey, JSON.stringify(this._authData));
        }
    }

    _initHtmlElements() {
        // Wallet iframe
        const iframe = document.createElement('iframe');
        iframe.style = 'display: none;';
        iframe.src = this._walletBaseUrl + EMBED_WALLET_URL_SUFFIX;
        document.body.appendChild(iframe);
        this._walletWindow = iframe.contentWindow;

        // Message Event
        window.addEventListener('message', this.receiveMessage.bind(this), false);
    }

    receiveMessage(event) {
        if (!this._walletBaseUrl.startsWith(event.origin)) {
            // Only processing wallet messages.
            console.log('Wallet account ignoring message from ' + event.origin);
            return;
        }
        let data;
        try {
            data = JSON.parse(event.data);
        } catch (e) {
            console.error('Can\'t parse the result', event.data, e);
            return;
        }
        const request_id = data.request_id || '';
        if (!(request_id in this._signatureRequests)) {
            console.error('Request ID' + request_id + ' was not found');
            return;
        }
        let signatureRequest = this._signatureRequests[request_id];
        delete this._signatureRequests[request_id];

        if (data.success) {
            signatureRequest.resolve(data.result);
        } else {
            signatureRequest.reject(data.error);
        }
    }

    _randomRequestId() {
        var result = '';

        for (var i = 0; i < REQUEST_ID_LENGTH; i++) {
            result += RANDOM_ALPHABET.charAt(Math.floor(Math.random() * RANDOM_ALPHABET.length));
        }

        return result;
    }

    _remoteSign(hash, methodName, args) {
        // TODO(#482): Add timeout.
        return new Promise((resolve, reject) => {
            const request_id = this._randomRequestId();
            this._signatureRequests[request_id] = {
                request_id,
                resolve,
                reject,
            };
            this._walletWindow.postMessage(JSON.stringify({
                action: 'sign_transaction',
                token: this._authData.authToken,
                method_name: methodName,
                args: args || {},
                hash,
                request_id,
            }), this._walletBaseUrl);
        });
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
        const body = FunctionCallTransaction.decode(buffer);
        let methodName = Buffer.from(body.methodName).toString();
        let args = JSON.parse(Buffer.from(body.args).toString());
        let signature = await this._remoteSign(sha256.array(buffer), methodName, args);
        return {
            signature,
        };
    }

}

module.exports = WalletAccount;
