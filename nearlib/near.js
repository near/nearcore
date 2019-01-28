const bs58 = require('bs58');

const NearClient = require('./nearclient');
const BrowserLocalStorageKeystore = require('./signing/browser_local_storage_keystore');
const LocalNodeConnection = require('./local_node_connection');
const {
    DeployContractTransaction, FunctionCallTransaction, SignedTransaction
} = require('./protos');

/*
 * This is javascript library for interacting with blockchain.
 */
class Near {
    constructor(nearClient) {
        this.nearClient = nearClient;
    }

    /**
     * Default setup for browser
     */
    static createDefaultConfig(nodeUrl = "http://localhost:3030") {
        return new Near(new NearClient(
            new BrowserLocalStorageKeystore(),
            new LocalNodeConnection(nodeUrl)
        ));
    };

    /**
     * Calls a view function. Returns the same value that the function returns.
     */
    async callViewFunction(sender, contractAccountId, methodName, args) {
        if (!args) {
            args = {};
        }
        const serializedArgs = Array.from(Buffer.from(JSON.stringify(args)));
        const response = await this.nearClient.request('call_view_function', {
            originator: sender,
            contract_account_id: contractAccountId,
            method_name: methodName,
            args: serializedArgs
        });
        const json = JSON.parse(Buffer.from(response.result).toString());
        return json.result;
    };

    /**
     * Schedules an asynchronous function call.
     */
    async scheduleFunctionCall(amount, originator, contractId, methodName, args) {
        if (!args) {
            args = {};
        }
        args = Uint8Array(Buffer.from(JSON.stringify(args)));
        const nonce = await this.nearClient.getNonce();
        const functionCall = FunctionCallTransaction.deploy({
            nonce,
            originator,
            contractId,
            methodName,
            args,
            amount,
        });
        const buffer = FunctionCallTransaction.encode(functionCall).finish();
        const signature = await this.nearClient.signer.signTransactionBody(
            buffer,
            originator,
        );

        const signedTransaction = SignedTransaction.create({
            functionCall,
            signature,
        });
        return await this.nearClient.submitTransaction(signedTransaction);
    };

    /**
     * Deploys a contract.
     */
    async deployContract(originator, contractId, wasmByteArray, publicKey) {
        const nonce = await this.nearClient.getNonce();
        publicKey = bs58.decode(publicKey);
        const deployContract = DeployContractTransaction.deploy({
            nonce,
            originator,
            contractId,
            wasmByteArray,
            publicKey
        });
        const buffer = DeployContractTransaction.encode(deployContract).finish();
        const signature = await this.nearClient.signer.signTransactionBody(
            buffer,
            originator,
        );

        const signedTransaction = SignedTransaction.create({
            deployContract,
            signature,
        });
        return await this.nearClient.submitTransaction(signedTransaction);
    }
};

module.exports = Near; 

