/**
 * Wallet based signer that uses external wallet through the iframe to signs transactions.
 */
const EMBED_WALLET_URL_SUFFIX = '/embed/';
const RANDOM_ALPHABET = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';
const REQUEST_ID_LENGTH = 32;

class WalletBasedSigner {

    constructor(authToken, walletBaseUrl = 'https://wallet.nearprotocol.com') {
        this._authToken = authToken;
        this._walletBaseUrl = walletBaseUrl;

        this._initHtmlElements();
        this._signatureRequests = {};
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
        if (event.origin != this._walletBaseUrl) {
            // Only processing wallet messages.
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
        return new Promise((resolve, reject) => {
            const request_id = this._randomRequestId();
            this._signatureRequests[request_id] = {
                request_id,
                resolve,
                reject,
            };
            this._walletWindow.postMessage(JSON.stringify({
                action: 'sign_transaction',
                token: this._authToken,
                method_name: methodName,
                args: args || {},
                hash,
                request_id,
            }), this._walletBaseUrl);
        });
    }

    /**
     * Sign a transaction. If the key for senderAccountId is not present, this operation
     * will fail.
     * @param {object} tx Transaction details
     * @param {string} senderAccountId
     */
    async signTransaction(tx, senderAccountId) {
        const hash = tx.hash;
        let methodName = Buffer.from(tx.body.FunctionCall.method_name).toString();
        let args = JSON.parse(Buffer.from(tx.body.FunctionCall.args).toString());
        let signature = await this._remoteSign(hash, methodName, args);
        return signature;
    }

}

module.exports = WalletBasedSigner;
