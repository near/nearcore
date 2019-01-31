/*eslint-disable block-scoped-var, id-length, no-control-regex, no-magic-numbers, no-prototype-builtins, no-redeclare, no-shadow, no-var, sort-vars*/
'use strict';

var $protobuf = require('protobufjs/minimal');

// Common aliases
var $Reader = $protobuf.Reader, $Writer = $protobuf.Writer, $util = $protobuf.util;

// Exported root namespace
var $root = $protobuf.roots['default'] || ($protobuf.roots['default'] = {});

$root.CreateAccountTransaction = (function() {

    /**
     * Properties of a CreateAccountTransaction.
     * @exports ICreateAccountTransaction
     * @interface ICreateAccountTransaction
     * @property {number|Long|null} [nonce] CreateAccountTransaction nonce
     * @property {string|null} [originator] CreateAccountTransaction originator
     * @property {string|null} [newAccountId] CreateAccountTransaction newAccountId
     * @property {number|Long|null} [amount] CreateAccountTransaction amount
     * @property {Uint8Array|null} [publicKey] CreateAccountTransaction publicKey
     */

    /**
     * Constructs a new CreateAccountTransaction.
     * @exports CreateAccountTransaction
     * @classdesc Represents a CreateAccountTransaction.
     * @implements ICreateAccountTransaction
     * @constructor
     * @param {ICreateAccountTransaction=} [properties] Properties to set
     */
    function CreateAccountTransaction(properties) {
        if (properties)
            for (var keys = Object.keys(properties), i = 0; i < keys.length; ++i)
                if (properties[keys[i]] != null)
                    this[keys[i]] = properties[keys[i]];
    }

    /**
     * CreateAccountTransaction nonce.
     * @member {number|Long} nonce
     * @memberof CreateAccountTransaction
     * @instance
     */
    CreateAccountTransaction.prototype.nonce = $util.Long ? $util.Long.fromBits(0,0,true) : 0;

    /**
     * CreateAccountTransaction originator.
     * @member {string} originator
     * @memberof CreateAccountTransaction
     * @instance
     */
    CreateAccountTransaction.prototype.originator = '';

    /**
     * CreateAccountTransaction newAccountId.
     * @member {string} newAccountId
     * @memberof CreateAccountTransaction
     * @instance
     */
    CreateAccountTransaction.prototype.newAccountId = '';

    /**
     * CreateAccountTransaction amount.
     * @member {number|Long} amount
     * @memberof CreateAccountTransaction
     * @instance
     */
    CreateAccountTransaction.prototype.amount = $util.Long ? $util.Long.fromBits(0,0,true) : 0;

    /**
     * CreateAccountTransaction publicKey.
     * @member {Uint8Array} publicKey
     * @memberof CreateAccountTransaction
     * @instance
     */
    CreateAccountTransaction.prototype.publicKey = $util.newBuffer([]);

    /**
     * Creates a new CreateAccountTransaction instance using the specified properties.
     * @function create
     * @memberof CreateAccountTransaction
     * @static
     * @param {ICreateAccountTransaction=} [properties] Properties to set
     * @returns {CreateAccountTransaction} CreateAccountTransaction instance
     */
    CreateAccountTransaction.create = function create(properties) {
        return new CreateAccountTransaction(properties);
    };

    /**
     * Encodes the specified CreateAccountTransaction message. Does not implicitly {@link CreateAccountTransaction.verify|verify} messages.
     * @function encode
     * @memberof CreateAccountTransaction
     * @static
     * @param {ICreateAccountTransaction} message CreateAccountTransaction message or plain object to encode
     * @param {$protobuf.Writer} [writer] Writer to encode to
     * @returns {$protobuf.Writer} Writer
     */
    CreateAccountTransaction.encode = function encode(message, writer) {
        if (!writer)
            writer = $Writer.create();
        if (message.nonce != null && message.hasOwnProperty('nonce'))
            writer.uint32(/* id 1, wireType 0 =*/8).uint64(message.nonce);
        if (message.originator != null && message.hasOwnProperty('originator'))
            writer.uint32(/* id 2, wireType 2 =*/18).string(message.originator);
        if (message.newAccountId != null && message.hasOwnProperty('newAccountId'))
            writer.uint32(/* id 3, wireType 2 =*/26).string(message.newAccountId);
        if (message.amount != null && message.hasOwnProperty('amount'))
            writer.uint32(/* id 4, wireType 0 =*/32).uint64(message.amount);
        if (message.publicKey != null && message.hasOwnProperty('publicKey'))
            writer.uint32(/* id 5, wireType 2 =*/42).bytes(message.publicKey);
        return writer;
    };

    /**
     * Encodes the specified CreateAccountTransaction message, length delimited. Does not implicitly {@link CreateAccountTransaction.verify|verify} messages.
     * @function encodeDelimited
     * @memberof CreateAccountTransaction
     * @static
     * @param {ICreateAccountTransaction} message CreateAccountTransaction message or plain object to encode
     * @param {$protobuf.Writer} [writer] Writer to encode to
     * @returns {$protobuf.Writer} Writer
     */
    CreateAccountTransaction.encodeDelimited = function encodeDelimited(message, writer) {
        return this.encode(message, writer).ldelim();
    };

    /**
     * Decodes a CreateAccountTransaction message from the specified reader or buffer.
     * @function decode
     * @memberof CreateAccountTransaction
     * @static
     * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
     * @param {number} [length] Message length if known beforehand
     * @returns {CreateAccountTransaction} CreateAccountTransaction
     * @throws {Error} If the payload is not a reader or valid buffer
     * @throws {$protobuf.util.ProtocolError} If required fields are missing
     */
    CreateAccountTransaction.decode = function decode(reader, length) {
        if (!(reader instanceof $Reader))
            reader = $Reader.create(reader);
        var end = length === undefined ? reader.len : reader.pos + length, message = new $root.CreateAccountTransaction();
        while (reader.pos < end) {
            var tag = reader.uint32();
            switch (tag >>> 3) {
            case 1:
                message.nonce = reader.uint64();
                break;
            case 2:
                message.originator = reader.string();
                break;
            case 3:
                message.newAccountId = reader.string();
                break;
            case 4:
                message.amount = reader.uint64();
                break;
            case 5:
                message.publicKey = reader.bytes();
                break;
            default:
                reader.skipType(tag & 7);
                break;
            }
        }
        return message;
    };

    /**
     * Decodes a CreateAccountTransaction message from the specified reader or buffer, length delimited.
     * @function decodeDelimited
     * @memberof CreateAccountTransaction
     * @static
     * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
     * @returns {CreateAccountTransaction} CreateAccountTransaction
     * @throws {Error} If the payload is not a reader or valid buffer
     * @throws {$protobuf.util.ProtocolError} If required fields are missing
     */
    CreateAccountTransaction.decodeDelimited = function decodeDelimited(reader) {
        if (!(reader instanceof $Reader))
            reader = new $Reader(reader);
        return this.decode(reader, reader.uint32());
    };

    /**
     * Verifies a CreateAccountTransaction message.
     * @function verify
     * @memberof CreateAccountTransaction
     * @static
     * @param {Object.<string,*>} message Plain object to verify
     * @returns {string|null} `null` if valid, otherwise the reason why it is not
     */
    CreateAccountTransaction.verify = function verify(message) {
        if (typeof message !== 'object' || message === null)
            return 'object expected';
        if (message.nonce != null && message.hasOwnProperty('nonce'))
            if (!$util.isInteger(message.nonce) && !(message.nonce && $util.isInteger(message.nonce.low) && $util.isInteger(message.nonce.high)))
                return 'nonce: integer|Long expected';
        if (message.originator != null && message.hasOwnProperty('originator'))
            if (!$util.isString(message.originator))
                return 'originator: string expected';
        if (message.newAccountId != null && message.hasOwnProperty('newAccountId'))
            if (!$util.isString(message.newAccountId))
                return 'newAccountId: string expected';
        if (message.amount != null && message.hasOwnProperty('amount'))
            if (!$util.isInteger(message.amount) && !(message.amount && $util.isInteger(message.amount.low) && $util.isInteger(message.amount.high)))
                return 'amount: integer|Long expected';
        if (message.publicKey != null && message.hasOwnProperty('publicKey'))
            if (!(message.publicKey && typeof message.publicKey.length === 'number' || $util.isString(message.publicKey)))
                return 'publicKey: buffer expected';
        return null;
    };

    /**
     * Creates a CreateAccountTransaction message from a plain object. Also converts values to their respective internal types.
     * @function fromObject
     * @memberof CreateAccountTransaction
     * @static
     * @param {Object.<string,*>} object Plain object
     * @returns {CreateAccountTransaction} CreateAccountTransaction
     */
    CreateAccountTransaction.fromObject = function fromObject(object) {
        if (object instanceof $root.CreateAccountTransaction)
            return object;
        var message = new $root.CreateAccountTransaction();
        if (object.nonce != null)
            if ($util.Long)
                (message.nonce = $util.Long.fromValue(object.nonce)).unsigned = true;
            else if (typeof object.nonce === 'string')
                message.nonce = parseInt(object.nonce, 10);
            else if (typeof object.nonce === 'number')
                message.nonce = object.nonce;
            else if (typeof object.nonce === 'object')
                message.nonce = new $util.LongBits(object.nonce.low >>> 0, object.nonce.high >>> 0).toNumber(true);
        if (object.originator != null)
            message.originator = String(object.originator);
        if (object.newAccountId != null)
            message.newAccountId = String(object.newAccountId);
        if (object.amount != null)
            if ($util.Long)
                (message.amount = $util.Long.fromValue(object.amount)).unsigned = true;
            else if (typeof object.amount === 'string')
                message.amount = parseInt(object.amount, 10);
            else if (typeof object.amount === 'number')
                message.amount = object.amount;
            else if (typeof object.amount === 'object')
                message.amount = new $util.LongBits(object.amount.low >>> 0, object.amount.high >>> 0).toNumber(true);
        if (object.publicKey != null)
            if (typeof object.publicKey === 'string')
                $util.base64.decode(object.publicKey, message.publicKey = $util.newBuffer($util.base64.length(object.publicKey)), 0);
            else if (object.publicKey.length)
                message.publicKey = object.publicKey;
        return message;
    };

    /**
     * Creates a plain object from a CreateAccountTransaction message. Also converts values to other types if specified.
     * @function toObject
     * @memberof CreateAccountTransaction
     * @static
     * @param {CreateAccountTransaction} message CreateAccountTransaction
     * @param {$protobuf.IConversionOptions} [options] Conversion options
     * @returns {Object.<string,*>} Plain object
     */
    CreateAccountTransaction.toObject = function toObject(message, options) {
        if (!options)
            options = {};
        var object = {};
        if (options.defaults) {
            if ($util.Long) {
                var long = new $util.Long(0, 0, true);
                object.nonce = options.longs === String ? long.toString() : options.longs === Number ? long.toNumber() : long;
            } else
                object.nonce = options.longs === String ? '0' : 0;
            object.originator = '';
            object.newAccountId = '';
            if ($util.Long) {
                var long = new $util.Long(0, 0, true);
                object.amount = options.longs === String ? long.toString() : options.longs === Number ? long.toNumber() : long;
            } else
                object.amount = options.longs === String ? '0' : 0;
            if (options.bytes === String)
                object.publicKey = '';
            else {
                object.publicKey = [];
                if (options.bytes !== Array)
                    object.publicKey = $util.newBuffer(object.publicKey);
            }
        }
        if (message.nonce != null && message.hasOwnProperty('nonce'))
            if (typeof message.nonce === 'number')
                object.nonce = options.longs === String ? String(message.nonce) : message.nonce;
            else
                object.nonce = options.longs === String ? $util.Long.prototype.toString.call(message.nonce) : options.longs === Number ? new $util.LongBits(message.nonce.low >>> 0, message.nonce.high >>> 0).toNumber(true) : message.nonce;
        if (message.originator != null && message.hasOwnProperty('originator'))
            object.originator = message.originator;
        if (message.newAccountId != null && message.hasOwnProperty('newAccountId'))
            object.newAccountId = message.newAccountId;
        if (message.amount != null && message.hasOwnProperty('amount'))
            if (typeof message.amount === 'number')
                object.amount = options.longs === String ? String(message.amount) : message.amount;
            else
                object.amount = options.longs === String ? $util.Long.prototype.toString.call(message.amount) : options.longs === Number ? new $util.LongBits(message.amount.low >>> 0, message.amount.high >>> 0).toNumber(true) : message.amount;
        if (message.publicKey != null && message.hasOwnProperty('publicKey'))
            object.publicKey = options.bytes === String ? $util.base64.encode(message.publicKey, 0, message.publicKey.length) : options.bytes === Array ? Array.prototype.slice.call(message.publicKey) : message.publicKey;
        return object;
    };

    /**
     * Converts this CreateAccountTransaction to JSON.
     * @function toJSON
     * @memberof CreateAccountTransaction
     * @instance
     * @returns {Object.<string,*>} JSON object
     */
    CreateAccountTransaction.prototype.toJSON = function toJSON() {
        return this.constructor.toObject(this, $protobuf.util.toJSONOptions);
    };

    return CreateAccountTransaction;
})();

$root.DeployContractTransaction = (function() {

    /**
     * Properties of a DeployContractTransaction.
     * @exports IDeployContractTransaction
     * @interface IDeployContractTransaction
     * @property {number|Long|null} [nonce] DeployContractTransaction nonce
     * @property {string|null} [originator] DeployContractTransaction originator
     * @property {string|null} [contractId] DeployContractTransaction contractId
     * @property {Uint8Array|null} [wasmByteArray] DeployContractTransaction wasmByteArray
     * @property {Uint8Array|null} [publicKey] DeployContractTransaction publicKey
     */

    /**
     * Constructs a new DeployContractTransaction.
     * @exports DeployContractTransaction
     * @classdesc Represents a DeployContractTransaction.
     * @implements IDeployContractTransaction
     * @constructor
     * @param {IDeployContractTransaction=} [properties] Properties to set
     */
    function DeployContractTransaction(properties) {
        if (properties)
            for (var keys = Object.keys(properties), i = 0; i < keys.length; ++i)
                if (properties[keys[i]] != null)
                    this[keys[i]] = properties[keys[i]];
    }

    /**
     * DeployContractTransaction nonce.
     * @member {number|Long} nonce
     * @memberof DeployContractTransaction
     * @instance
     */
    DeployContractTransaction.prototype.nonce = $util.Long ? $util.Long.fromBits(0,0,true) : 0;

    /**
     * DeployContractTransaction originator.
     * @member {string} originator
     * @memberof DeployContractTransaction
     * @instance
     */
    DeployContractTransaction.prototype.originator = '';

    /**
     * DeployContractTransaction contractId.
     * @member {string} contractId
     * @memberof DeployContractTransaction
     * @instance
     */
    DeployContractTransaction.prototype.contractId = '';

    /**
     * DeployContractTransaction wasmByteArray.
     * @member {Uint8Array} wasmByteArray
     * @memberof DeployContractTransaction
     * @instance
     */
    DeployContractTransaction.prototype.wasmByteArray = $util.newBuffer([]);

    /**
     * DeployContractTransaction publicKey.
     * @member {Uint8Array} publicKey
     * @memberof DeployContractTransaction
     * @instance
     */
    DeployContractTransaction.prototype.publicKey = $util.newBuffer([]);

    /**
     * Creates a new DeployContractTransaction instance using the specified properties.
     * @function create
     * @memberof DeployContractTransaction
     * @static
     * @param {IDeployContractTransaction=} [properties] Properties to set
     * @returns {DeployContractTransaction} DeployContractTransaction instance
     */
    DeployContractTransaction.create = function create(properties) {
        return new DeployContractTransaction(properties);
    };

    /**
     * Encodes the specified DeployContractTransaction message. Does not implicitly {@link DeployContractTransaction.verify|verify} messages.
     * @function encode
     * @memberof DeployContractTransaction
     * @static
     * @param {IDeployContractTransaction} message DeployContractTransaction message or plain object to encode
     * @param {$protobuf.Writer} [writer] Writer to encode to
     * @returns {$protobuf.Writer} Writer
     */
    DeployContractTransaction.encode = function encode(message, writer) {
        if (!writer)
            writer = $Writer.create();
        if (message.nonce != null && message.hasOwnProperty('nonce'))
            writer.uint32(/* id 1, wireType 0 =*/8).uint64(message.nonce);
        if (message.originator != null && message.hasOwnProperty('originator'))
            writer.uint32(/* id 2, wireType 2 =*/18).string(message.originator);
        if (message.contractId != null && message.hasOwnProperty('contractId'))
            writer.uint32(/* id 3, wireType 2 =*/26).string(message.contractId);
        if (message.wasmByteArray != null && message.hasOwnProperty('wasmByteArray'))
            writer.uint32(/* id 4, wireType 2 =*/34).bytes(message.wasmByteArray);
        if (message.publicKey != null && message.hasOwnProperty('publicKey'))
            writer.uint32(/* id 5, wireType 2 =*/42).bytes(message.publicKey);
        return writer;
    };

    /**
     * Encodes the specified DeployContractTransaction message, length delimited. Does not implicitly {@link DeployContractTransaction.verify|verify} messages.
     * @function encodeDelimited
     * @memberof DeployContractTransaction
     * @static
     * @param {IDeployContractTransaction} message DeployContractTransaction message or plain object to encode
     * @param {$protobuf.Writer} [writer] Writer to encode to
     * @returns {$protobuf.Writer} Writer
     */
    DeployContractTransaction.encodeDelimited = function encodeDelimited(message, writer) {
        return this.encode(message, writer).ldelim();
    };

    /**
     * Decodes a DeployContractTransaction message from the specified reader or buffer.
     * @function decode
     * @memberof DeployContractTransaction
     * @static
     * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
     * @param {number} [length] Message length if known beforehand
     * @returns {DeployContractTransaction} DeployContractTransaction
     * @throws {Error} If the payload is not a reader or valid buffer
     * @throws {$protobuf.util.ProtocolError} If required fields are missing
     */
    DeployContractTransaction.decode = function decode(reader, length) {
        if (!(reader instanceof $Reader))
            reader = $Reader.create(reader);
        var end = length === undefined ? reader.len : reader.pos + length, message = new $root.DeployContractTransaction();
        while (reader.pos < end) {
            var tag = reader.uint32();
            switch (tag >>> 3) {
            case 1:
                message.nonce = reader.uint64();
                break;
            case 2:
                message.originator = reader.string();
                break;
            case 3:
                message.contractId = reader.string();
                break;
            case 4:
                message.wasmByteArray = reader.bytes();
                break;
            case 5:
                message.publicKey = reader.bytes();
                break;
            default:
                reader.skipType(tag & 7);
                break;
            }
        }
        return message;
    };

    /**
     * Decodes a DeployContractTransaction message from the specified reader or buffer, length delimited.
     * @function decodeDelimited
     * @memberof DeployContractTransaction
     * @static
     * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
     * @returns {DeployContractTransaction} DeployContractTransaction
     * @throws {Error} If the payload is not a reader or valid buffer
     * @throws {$protobuf.util.ProtocolError} If required fields are missing
     */
    DeployContractTransaction.decodeDelimited = function decodeDelimited(reader) {
        if (!(reader instanceof $Reader))
            reader = new $Reader(reader);
        return this.decode(reader, reader.uint32());
    };

    /**
     * Verifies a DeployContractTransaction message.
     * @function verify
     * @memberof DeployContractTransaction
     * @static
     * @param {Object.<string,*>} message Plain object to verify
     * @returns {string|null} `null` if valid, otherwise the reason why it is not
     */
    DeployContractTransaction.verify = function verify(message) {
        if (typeof message !== 'object' || message === null)
            return 'object expected';
        if (message.nonce != null && message.hasOwnProperty('nonce'))
            if (!$util.isInteger(message.nonce) && !(message.nonce && $util.isInteger(message.nonce.low) && $util.isInteger(message.nonce.high)))
                return 'nonce: integer|Long expected';
        if (message.originator != null && message.hasOwnProperty('originator'))
            if (!$util.isString(message.originator))
                return 'originator: string expected';
        if (message.contractId != null && message.hasOwnProperty('contractId'))
            if (!$util.isString(message.contractId))
                return 'contractId: string expected';
        if (message.wasmByteArray != null && message.hasOwnProperty('wasmByteArray'))
            if (!(message.wasmByteArray && typeof message.wasmByteArray.length === 'number' || $util.isString(message.wasmByteArray)))
                return 'wasmByteArray: buffer expected';
        if (message.publicKey != null && message.hasOwnProperty('publicKey'))
            if (!(message.publicKey && typeof message.publicKey.length === 'number' || $util.isString(message.publicKey)))
                return 'publicKey: buffer expected';
        return null;
    };

    /**
     * Creates a DeployContractTransaction message from a plain object. Also converts values to their respective internal types.
     * @function fromObject
     * @memberof DeployContractTransaction
     * @static
     * @param {Object.<string,*>} object Plain object
     * @returns {DeployContractTransaction} DeployContractTransaction
     */
    DeployContractTransaction.fromObject = function fromObject(object) {
        if (object instanceof $root.DeployContractTransaction)
            return object;
        var message = new $root.DeployContractTransaction();
        if (object.nonce != null)
            if ($util.Long)
                (message.nonce = $util.Long.fromValue(object.nonce)).unsigned = true;
            else if (typeof object.nonce === 'string')
                message.nonce = parseInt(object.nonce, 10);
            else if (typeof object.nonce === 'number')
                message.nonce = object.nonce;
            else if (typeof object.nonce === 'object')
                message.nonce = new $util.LongBits(object.nonce.low >>> 0, object.nonce.high >>> 0).toNumber(true);
        if (object.originator != null)
            message.originator = String(object.originator);
        if (object.contractId != null)
            message.contractId = String(object.contractId);
        if (object.wasmByteArray != null)
            if (typeof object.wasmByteArray === 'string')
                $util.base64.decode(object.wasmByteArray, message.wasmByteArray = $util.newBuffer($util.base64.length(object.wasmByteArray)), 0);
            else if (object.wasmByteArray.length)
                message.wasmByteArray = object.wasmByteArray;
        if (object.publicKey != null)
            if (typeof object.publicKey === 'string')
                $util.base64.decode(object.publicKey, message.publicKey = $util.newBuffer($util.base64.length(object.publicKey)), 0);
            else if (object.publicKey.length)
                message.publicKey = object.publicKey;
        return message;
    };

    /**
     * Creates a plain object from a DeployContractTransaction message. Also converts values to other types if specified.
     * @function toObject
     * @memberof DeployContractTransaction
     * @static
     * @param {DeployContractTransaction} message DeployContractTransaction
     * @param {$protobuf.IConversionOptions} [options] Conversion options
     * @returns {Object.<string,*>} Plain object
     */
    DeployContractTransaction.toObject = function toObject(message, options) {
        if (!options)
            options = {};
        var object = {};
        if (options.defaults) {
            if ($util.Long) {
                var long = new $util.Long(0, 0, true);
                object.nonce = options.longs === String ? long.toString() : options.longs === Number ? long.toNumber() : long;
            } else
                object.nonce = options.longs === String ? '0' : 0;
            object.originator = '';
            object.contractId = '';
            if (options.bytes === String)
                object.wasmByteArray = '';
            else {
                object.wasmByteArray = [];
                if (options.bytes !== Array)
                    object.wasmByteArray = $util.newBuffer(object.wasmByteArray);
            }
            if (options.bytes === String)
                object.publicKey = '';
            else {
                object.publicKey = [];
                if (options.bytes !== Array)
                    object.publicKey = $util.newBuffer(object.publicKey);
            }
        }
        if (message.nonce != null && message.hasOwnProperty('nonce'))
            if (typeof message.nonce === 'number')
                object.nonce = options.longs === String ? String(message.nonce) : message.nonce;
            else
                object.nonce = options.longs === String ? $util.Long.prototype.toString.call(message.nonce) : options.longs === Number ? new $util.LongBits(message.nonce.low >>> 0, message.nonce.high >>> 0).toNumber(true) : message.nonce;
        if (message.originator != null && message.hasOwnProperty('originator'))
            object.originator = message.originator;
        if (message.contractId != null && message.hasOwnProperty('contractId'))
            object.contractId = message.contractId;
        if (message.wasmByteArray != null && message.hasOwnProperty('wasmByteArray'))
            object.wasmByteArray = options.bytes === String ? $util.base64.encode(message.wasmByteArray, 0, message.wasmByteArray.length) : options.bytes === Array ? Array.prototype.slice.call(message.wasmByteArray) : message.wasmByteArray;
        if (message.publicKey != null && message.hasOwnProperty('publicKey'))
            object.publicKey = options.bytes === String ? $util.base64.encode(message.publicKey, 0, message.publicKey.length) : options.bytes === Array ? Array.prototype.slice.call(message.publicKey) : message.publicKey;
        return object;
    };

    /**
     * Converts this DeployContractTransaction to JSON.
     * @function toJSON
     * @memberof DeployContractTransaction
     * @instance
     * @returns {Object.<string,*>} JSON object
     */
    DeployContractTransaction.prototype.toJSON = function toJSON() {
        return this.constructor.toObject(this, $protobuf.util.toJSONOptions);
    };

    return DeployContractTransaction;
})();

$root.FunctionCallTransaction = (function() {

    /**
     * Properties of a FunctionCallTransaction.
     * @exports IFunctionCallTransaction
     * @interface IFunctionCallTransaction
     * @property {number|Long|null} [nonce] FunctionCallTransaction nonce
     * @property {string|null} [originator] FunctionCallTransaction originator
     * @property {string|null} [contractId] FunctionCallTransaction contractId
     * @property {Uint8Array|null} [methodName] FunctionCallTransaction methodName
     * @property {Uint8Array|null} [args] FunctionCallTransaction args
     * @property {number|Long|null} [amount] FunctionCallTransaction amount
     */

    /**
     * Constructs a new FunctionCallTransaction.
     * @exports FunctionCallTransaction
     * @classdesc Represents a FunctionCallTransaction.
     * @implements IFunctionCallTransaction
     * @constructor
     * @param {IFunctionCallTransaction=} [properties] Properties to set
     */
    function FunctionCallTransaction(properties) {
        if (properties)
            for (var keys = Object.keys(properties), i = 0; i < keys.length; ++i)
                if (properties[keys[i]] != null)
                    this[keys[i]] = properties[keys[i]];
    }

    /**
     * FunctionCallTransaction nonce.
     * @member {number|Long} nonce
     * @memberof FunctionCallTransaction
     * @instance
     */
    FunctionCallTransaction.prototype.nonce = $util.Long ? $util.Long.fromBits(0,0,true) : 0;

    /**
     * FunctionCallTransaction originator.
     * @member {string} originator
     * @memberof FunctionCallTransaction
     * @instance
     */
    FunctionCallTransaction.prototype.originator = '';

    /**
     * FunctionCallTransaction contractId.
     * @member {string} contractId
     * @memberof FunctionCallTransaction
     * @instance
     */
    FunctionCallTransaction.prototype.contractId = '';

    /**
     * FunctionCallTransaction methodName.
     * @member {Uint8Array} methodName
     * @memberof FunctionCallTransaction
     * @instance
     */
    FunctionCallTransaction.prototype.methodName = $util.newBuffer([]);

    /**
     * FunctionCallTransaction args.
     * @member {Uint8Array} args
     * @memberof FunctionCallTransaction
     * @instance
     */
    FunctionCallTransaction.prototype.args = $util.newBuffer([]);

    /**
     * FunctionCallTransaction amount.
     * @member {number|Long} amount
     * @memberof FunctionCallTransaction
     * @instance
     */
    FunctionCallTransaction.prototype.amount = $util.Long ? $util.Long.fromBits(0,0,true) : 0;

    /**
     * Creates a new FunctionCallTransaction instance using the specified properties.
     * @function create
     * @memberof FunctionCallTransaction
     * @static
     * @param {IFunctionCallTransaction=} [properties] Properties to set
     * @returns {FunctionCallTransaction} FunctionCallTransaction instance
     */
    FunctionCallTransaction.create = function create(properties) {
        return new FunctionCallTransaction(properties);
    };

    /**
     * Encodes the specified FunctionCallTransaction message. Does not implicitly {@link FunctionCallTransaction.verify|verify} messages.
     * @function encode
     * @memberof FunctionCallTransaction
     * @static
     * @param {IFunctionCallTransaction} message FunctionCallTransaction message or plain object to encode
     * @param {$protobuf.Writer} [writer] Writer to encode to
     * @returns {$protobuf.Writer} Writer
     */
    FunctionCallTransaction.encode = function encode(message, writer) {
        if (!writer)
            writer = $Writer.create();
        if (message.nonce != null && message.hasOwnProperty('nonce'))
            writer.uint32(/* id 1, wireType 0 =*/8).uint64(message.nonce);
        if (message.originator != null && message.hasOwnProperty('originator'))
            writer.uint32(/* id 2, wireType 2 =*/18).string(message.originator);
        if (message.contractId != null && message.hasOwnProperty('contractId'))
            writer.uint32(/* id 3, wireType 2 =*/26).string(message.contractId);
        if (message.methodName != null && message.hasOwnProperty('methodName'))
            writer.uint32(/* id 4, wireType 2 =*/34).bytes(message.methodName);
        if (message.args != null && message.hasOwnProperty('args'))
            writer.uint32(/* id 5, wireType 2 =*/42).bytes(message.args);
        if (message.amount != null && message.hasOwnProperty('amount'))
            writer.uint32(/* id 6, wireType 0 =*/48).uint64(message.amount);
        return writer;
    };

    /**
     * Encodes the specified FunctionCallTransaction message, length delimited. Does not implicitly {@link FunctionCallTransaction.verify|verify} messages.
     * @function encodeDelimited
     * @memberof FunctionCallTransaction
     * @static
     * @param {IFunctionCallTransaction} message FunctionCallTransaction message or plain object to encode
     * @param {$protobuf.Writer} [writer] Writer to encode to
     * @returns {$protobuf.Writer} Writer
     */
    FunctionCallTransaction.encodeDelimited = function encodeDelimited(message, writer) {
        return this.encode(message, writer).ldelim();
    };

    /**
     * Decodes a FunctionCallTransaction message from the specified reader or buffer.
     * @function decode
     * @memberof FunctionCallTransaction
     * @static
     * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
     * @param {number} [length] Message length if known beforehand
     * @returns {FunctionCallTransaction} FunctionCallTransaction
     * @throws {Error} If the payload is not a reader or valid buffer
     * @throws {$protobuf.util.ProtocolError} If required fields are missing
     */
    FunctionCallTransaction.decode = function decode(reader, length) {
        if (!(reader instanceof $Reader))
            reader = $Reader.create(reader);
        var end = length === undefined ? reader.len : reader.pos + length, message = new $root.FunctionCallTransaction();
        while (reader.pos < end) {
            var tag = reader.uint32();
            switch (tag >>> 3) {
            case 1:
                message.nonce = reader.uint64();
                break;
            case 2:
                message.originator = reader.string();
                break;
            case 3:
                message.contractId = reader.string();
                break;
            case 4:
                message.methodName = reader.bytes();
                break;
            case 5:
                message.args = reader.bytes();
                break;
            case 6:
                message.amount = reader.uint64();
                break;
            default:
                reader.skipType(tag & 7);
                break;
            }
        }
        return message;
    };

    /**
     * Decodes a FunctionCallTransaction message from the specified reader or buffer, length delimited.
     * @function decodeDelimited
     * @memberof FunctionCallTransaction
     * @static
     * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
     * @returns {FunctionCallTransaction} FunctionCallTransaction
     * @throws {Error} If the payload is not a reader or valid buffer
     * @throws {$protobuf.util.ProtocolError} If required fields are missing
     */
    FunctionCallTransaction.decodeDelimited = function decodeDelimited(reader) {
        if (!(reader instanceof $Reader))
            reader = new $Reader(reader);
        return this.decode(reader, reader.uint32());
    };

    /**
     * Verifies a FunctionCallTransaction message.
     * @function verify
     * @memberof FunctionCallTransaction
     * @static
     * @param {Object.<string,*>} message Plain object to verify
     * @returns {string|null} `null` if valid, otherwise the reason why it is not
     */
    FunctionCallTransaction.verify = function verify(message) {
        if (typeof message !== 'object' || message === null)
            return 'object expected';
        if (message.nonce != null && message.hasOwnProperty('nonce'))
            if (!$util.isInteger(message.nonce) && !(message.nonce && $util.isInteger(message.nonce.low) && $util.isInteger(message.nonce.high)))
                return 'nonce: integer|Long expected';
        if (message.originator != null && message.hasOwnProperty('originator'))
            if (!$util.isString(message.originator))
                return 'originator: string expected';
        if (message.contractId != null && message.hasOwnProperty('contractId'))
            if (!$util.isString(message.contractId))
                return 'contractId: string expected';
        if (message.methodName != null && message.hasOwnProperty('methodName'))
            if (!(message.methodName && typeof message.methodName.length === 'number' || $util.isString(message.methodName)))
                return 'methodName: buffer expected';
        if (message.args != null && message.hasOwnProperty('args'))
            if (!(message.args && typeof message.args.length === 'number' || $util.isString(message.args)))
                return 'args: buffer expected';
        if (message.amount != null && message.hasOwnProperty('amount'))
            if (!$util.isInteger(message.amount) && !(message.amount && $util.isInteger(message.amount.low) && $util.isInteger(message.amount.high)))
                return 'amount: integer|Long expected';
        return null;
    };

    /**
     * Creates a FunctionCallTransaction message from a plain object. Also converts values to their respective internal types.
     * @function fromObject
     * @memberof FunctionCallTransaction
     * @static
     * @param {Object.<string,*>} object Plain object
     * @returns {FunctionCallTransaction} FunctionCallTransaction
     */
    FunctionCallTransaction.fromObject = function fromObject(object) {
        if (object instanceof $root.FunctionCallTransaction)
            return object;
        var message = new $root.FunctionCallTransaction();
        if (object.nonce != null)
            if ($util.Long)
                (message.nonce = $util.Long.fromValue(object.nonce)).unsigned = true;
            else if (typeof object.nonce === 'string')
                message.nonce = parseInt(object.nonce, 10);
            else if (typeof object.nonce === 'number')
                message.nonce = object.nonce;
            else if (typeof object.nonce === 'object')
                message.nonce = new $util.LongBits(object.nonce.low >>> 0, object.nonce.high >>> 0).toNumber(true);
        if (object.originator != null)
            message.originator = String(object.originator);
        if (object.contractId != null)
            message.contractId = String(object.contractId);
        if (object.methodName != null)
            if (typeof object.methodName === 'string')
                $util.base64.decode(object.methodName, message.methodName = $util.newBuffer($util.base64.length(object.methodName)), 0);
            else if (object.methodName.length)
                message.methodName = object.methodName;
        if (object.args != null)
            if (typeof object.args === 'string')
                $util.base64.decode(object.args, message.args = $util.newBuffer($util.base64.length(object.args)), 0);
            else if (object.args.length)
                message.args = object.args;
        if (object.amount != null)
            if ($util.Long)
                (message.amount = $util.Long.fromValue(object.amount)).unsigned = true;
            else if (typeof object.amount === 'string')
                message.amount = parseInt(object.amount, 10);
            else if (typeof object.amount === 'number')
                message.amount = object.amount;
            else if (typeof object.amount === 'object')
                message.amount = new $util.LongBits(object.amount.low >>> 0, object.amount.high >>> 0).toNumber(true);
        return message;
    };

    /**
     * Creates a plain object from a FunctionCallTransaction message. Also converts values to other types if specified.
     * @function toObject
     * @memberof FunctionCallTransaction
     * @static
     * @param {FunctionCallTransaction} message FunctionCallTransaction
     * @param {$protobuf.IConversionOptions} [options] Conversion options
     * @returns {Object.<string,*>} Plain object
     */
    FunctionCallTransaction.toObject = function toObject(message, options) {
        if (!options)
            options = {};
        var object = {};
        if (options.defaults) {
            if ($util.Long) {
                var long = new $util.Long(0, 0, true);
                object.nonce = options.longs === String ? long.toString() : options.longs === Number ? long.toNumber() : long;
            } else
                object.nonce = options.longs === String ? '0' : 0;
            object.originator = '';
            object.contractId = '';
            if (options.bytes === String)
                object.methodName = '';
            else {
                object.methodName = [];
                if (options.bytes !== Array)
                    object.methodName = $util.newBuffer(object.methodName);
            }
            if (options.bytes === String)
                object.args = '';
            else {
                object.args = [];
                if (options.bytes !== Array)
                    object.args = $util.newBuffer(object.args);
            }
            if ($util.Long) {
                var long = new $util.Long(0, 0, true);
                object.amount = options.longs === String ? long.toString() : options.longs === Number ? long.toNumber() : long;
            } else
                object.amount = options.longs === String ? '0' : 0;
        }
        if (message.nonce != null && message.hasOwnProperty('nonce'))
            if (typeof message.nonce === 'number')
                object.nonce = options.longs === String ? String(message.nonce) : message.nonce;
            else
                object.nonce = options.longs === String ? $util.Long.prototype.toString.call(message.nonce) : options.longs === Number ? new $util.LongBits(message.nonce.low >>> 0, message.nonce.high >>> 0).toNumber(true) : message.nonce;
        if (message.originator != null && message.hasOwnProperty('originator'))
            object.originator = message.originator;
        if (message.contractId != null && message.hasOwnProperty('contractId'))
            object.contractId = message.contractId;
        if (message.methodName != null && message.hasOwnProperty('methodName'))
            object.methodName = options.bytes === String ? $util.base64.encode(message.methodName, 0, message.methodName.length) : options.bytes === Array ? Array.prototype.slice.call(message.methodName) : message.methodName;
        if (message.args != null && message.hasOwnProperty('args'))
            object.args = options.bytes === String ? $util.base64.encode(message.args, 0, message.args.length) : options.bytes === Array ? Array.prototype.slice.call(message.args) : message.args;
        if (message.amount != null && message.hasOwnProperty('amount'))
            if (typeof message.amount === 'number')
                object.amount = options.longs === String ? String(message.amount) : message.amount;
            else
                object.amount = options.longs === String ? $util.Long.prototype.toString.call(message.amount) : options.longs === Number ? new $util.LongBits(message.amount.low >>> 0, message.amount.high >>> 0).toNumber(true) : message.amount;
        return object;
    };

    /**
     * Converts this FunctionCallTransaction to JSON.
     * @function toJSON
     * @memberof FunctionCallTransaction
     * @instance
     * @returns {Object.<string,*>} JSON object
     */
    FunctionCallTransaction.prototype.toJSON = function toJSON() {
        return this.constructor.toObject(this, $protobuf.util.toJSONOptions);
    };

    return FunctionCallTransaction;
})();

$root.SendMoneyTransaction = (function() {

    /**
     * Properties of a SendMoneyTransaction.
     * @exports ISendMoneyTransaction
     * @interface ISendMoneyTransaction
     * @property {number|Long|null} [nonce] SendMoneyTransaction nonce
     * @property {string|null} [originator] SendMoneyTransaction originator
     * @property {string|null} [receiver] SendMoneyTransaction receiver
     * @property {number|Long|null} [amount] SendMoneyTransaction amount
     */

    /**
     * Constructs a new SendMoneyTransaction.
     * @exports SendMoneyTransaction
     * @classdesc Represents a SendMoneyTransaction.
     * @implements ISendMoneyTransaction
     * @constructor
     * @param {ISendMoneyTransaction=} [properties] Properties to set
     */
    function SendMoneyTransaction(properties) {
        if (properties)
            for (var keys = Object.keys(properties), i = 0; i < keys.length; ++i)
                if (properties[keys[i]] != null)
                    this[keys[i]] = properties[keys[i]];
    }

    /**
     * SendMoneyTransaction nonce.
     * @member {number|Long} nonce
     * @memberof SendMoneyTransaction
     * @instance
     */
    SendMoneyTransaction.prototype.nonce = $util.Long ? $util.Long.fromBits(0,0,true) : 0;

    /**
     * SendMoneyTransaction originator.
     * @member {string} originator
     * @memberof SendMoneyTransaction
     * @instance
     */
    SendMoneyTransaction.prototype.originator = '';

    /**
     * SendMoneyTransaction receiver.
     * @member {string} receiver
     * @memberof SendMoneyTransaction
     * @instance
     */
    SendMoneyTransaction.prototype.receiver = '';

    /**
     * SendMoneyTransaction amount.
     * @member {number|Long} amount
     * @memberof SendMoneyTransaction
     * @instance
     */
    SendMoneyTransaction.prototype.amount = $util.Long ? $util.Long.fromBits(0,0,true) : 0;

    /**
     * Creates a new SendMoneyTransaction instance using the specified properties.
     * @function create
     * @memberof SendMoneyTransaction
     * @static
     * @param {ISendMoneyTransaction=} [properties] Properties to set
     * @returns {SendMoneyTransaction} SendMoneyTransaction instance
     */
    SendMoneyTransaction.create = function create(properties) {
        return new SendMoneyTransaction(properties);
    };

    /**
     * Encodes the specified SendMoneyTransaction message. Does not implicitly {@link SendMoneyTransaction.verify|verify} messages.
     * @function encode
     * @memberof SendMoneyTransaction
     * @static
     * @param {ISendMoneyTransaction} message SendMoneyTransaction message or plain object to encode
     * @param {$protobuf.Writer} [writer] Writer to encode to
     * @returns {$protobuf.Writer} Writer
     */
    SendMoneyTransaction.encode = function encode(message, writer) {
        if (!writer)
            writer = $Writer.create();
        if (message.nonce != null && message.hasOwnProperty('nonce'))
            writer.uint32(/* id 1, wireType 0 =*/8).uint64(message.nonce);
        if (message.originator != null && message.hasOwnProperty('originator'))
            writer.uint32(/* id 2, wireType 2 =*/18).string(message.originator);
        if (message.receiver != null && message.hasOwnProperty('receiver'))
            writer.uint32(/* id 3, wireType 2 =*/26).string(message.receiver);
        if (message.amount != null && message.hasOwnProperty('amount'))
            writer.uint32(/* id 4, wireType 0 =*/32).uint64(message.amount);
        return writer;
    };

    /**
     * Encodes the specified SendMoneyTransaction message, length delimited. Does not implicitly {@link SendMoneyTransaction.verify|verify} messages.
     * @function encodeDelimited
     * @memberof SendMoneyTransaction
     * @static
     * @param {ISendMoneyTransaction} message SendMoneyTransaction message or plain object to encode
     * @param {$protobuf.Writer} [writer] Writer to encode to
     * @returns {$protobuf.Writer} Writer
     */
    SendMoneyTransaction.encodeDelimited = function encodeDelimited(message, writer) {
        return this.encode(message, writer).ldelim();
    };

    /**
     * Decodes a SendMoneyTransaction message from the specified reader or buffer.
     * @function decode
     * @memberof SendMoneyTransaction
     * @static
     * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
     * @param {number} [length] Message length if known beforehand
     * @returns {SendMoneyTransaction} SendMoneyTransaction
     * @throws {Error} If the payload is not a reader or valid buffer
     * @throws {$protobuf.util.ProtocolError} If required fields are missing
     */
    SendMoneyTransaction.decode = function decode(reader, length) {
        if (!(reader instanceof $Reader))
            reader = $Reader.create(reader);
        var end = length === undefined ? reader.len : reader.pos + length, message = new $root.SendMoneyTransaction();
        while (reader.pos < end) {
            var tag = reader.uint32();
            switch (tag >>> 3) {
            case 1:
                message.nonce = reader.uint64();
                break;
            case 2:
                message.originator = reader.string();
                break;
            case 3:
                message.receiver = reader.string();
                break;
            case 4:
                message.amount = reader.uint64();
                break;
            default:
                reader.skipType(tag & 7);
                break;
            }
        }
        return message;
    };

    /**
     * Decodes a SendMoneyTransaction message from the specified reader or buffer, length delimited.
     * @function decodeDelimited
     * @memberof SendMoneyTransaction
     * @static
     * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
     * @returns {SendMoneyTransaction} SendMoneyTransaction
     * @throws {Error} If the payload is not a reader or valid buffer
     * @throws {$protobuf.util.ProtocolError} If required fields are missing
     */
    SendMoneyTransaction.decodeDelimited = function decodeDelimited(reader) {
        if (!(reader instanceof $Reader))
            reader = new $Reader(reader);
        return this.decode(reader, reader.uint32());
    };

    /**
     * Verifies a SendMoneyTransaction message.
     * @function verify
     * @memberof SendMoneyTransaction
     * @static
     * @param {Object.<string,*>} message Plain object to verify
     * @returns {string|null} `null` if valid, otherwise the reason why it is not
     */
    SendMoneyTransaction.verify = function verify(message) {
        if (typeof message !== 'object' || message === null)
            return 'object expected';
        if (message.nonce != null && message.hasOwnProperty('nonce'))
            if (!$util.isInteger(message.nonce) && !(message.nonce && $util.isInteger(message.nonce.low) && $util.isInteger(message.nonce.high)))
                return 'nonce: integer|Long expected';
        if (message.originator != null && message.hasOwnProperty('originator'))
            if (!$util.isString(message.originator))
                return 'originator: string expected';
        if (message.receiver != null && message.hasOwnProperty('receiver'))
            if (!$util.isString(message.receiver))
                return 'receiver: string expected';
        if (message.amount != null && message.hasOwnProperty('amount'))
            if (!$util.isInteger(message.amount) && !(message.amount && $util.isInteger(message.amount.low) && $util.isInteger(message.amount.high)))
                return 'amount: integer|Long expected';
        return null;
    };

    /**
     * Creates a SendMoneyTransaction message from a plain object. Also converts values to their respective internal types.
     * @function fromObject
     * @memberof SendMoneyTransaction
     * @static
     * @param {Object.<string,*>} object Plain object
     * @returns {SendMoneyTransaction} SendMoneyTransaction
     */
    SendMoneyTransaction.fromObject = function fromObject(object) {
        if (object instanceof $root.SendMoneyTransaction)
            return object;
        var message = new $root.SendMoneyTransaction();
        if (object.nonce != null)
            if ($util.Long)
                (message.nonce = $util.Long.fromValue(object.nonce)).unsigned = true;
            else if (typeof object.nonce === 'string')
                message.nonce = parseInt(object.nonce, 10);
            else if (typeof object.nonce === 'number')
                message.nonce = object.nonce;
            else if (typeof object.nonce === 'object')
                message.nonce = new $util.LongBits(object.nonce.low >>> 0, object.nonce.high >>> 0).toNumber(true);
        if (object.originator != null)
            message.originator = String(object.originator);
        if (object.receiver != null)
            message.receiver = String(object.receiver);
        if (object.amount != null)
            if ($util.Long)
                (message.amount = $util.Long.fromValue(object.amount)).unsigned = true;
            else if (typeof object.amount === 'string')
                message.amount = parseInt(object.amount, 10);
            else if (typeof object.amount === 'number')
                message.amount = object.amount;
            else if (typeof object.amount === 'object')
                message.amount = new $util.LongBits(object.amount.low >>> 0, object.amount.high >>> 0).toNumber(true);
        return message;
    };

    /**
     * Creates a plain object from a SendMoneyTransaction message. Also converts values to other types if specified.
     * @function toObject
     * @memberof SendMoneyTransaction
     * @static
     * @param {SendMoneyTransaction} message SendMoneyTransaction
     * @param {$protobuf.IConversionOptions} [options] Conversion options
     * @returns {Object.<string,*>} Plain object
     */
    SendMoneyTransaction.toObject = function toObject(message, options) {
        if (!options)
            options = {};
        var object = {};
        if (options.defaults) {
            if ($util.Long) {
                var long = new $util.Long(0, 0, true);
                object.nonce = options.longs === String ? long.toString() : options.longs === Number ? long.toNumber() : long;
            } else
                object.nonce = options.longs === String ? '0' : 0;
            object.originator = '';
            object.receiver = '';
            if ($util.Long) {
                var long = new $util.Long(0, 0, true);
                object.amount = options.longs === String ? long.toString() : options.longs === Number ? long.toNumber() : long;
            } else
                object.amount = options.longs === String ? '0' : 0;
        }
        if (message.nonce != null && message.hasOwnProperty('nonce'))
            if (typeof message.nonce === 'number')
                object.nonce = options.longs === String ? String(message.nonce) : message.nonce;
            else
                object.nonce = options.longs === String ? $util.Long.prototype.toString.call(message.nonce) : options.longs === Number ? new $util.LongBits(message.nonce.low >>> 0, message.nonce.high >>> 0).toNumber(true) : message.nonce;
        if (message.originator != null && message.hasOwnProperty('originator'))
            object.originator = message.originator;
        if (message.receiver != null && message.hasOwnProperty('receiver'))
            object.receiver = message.receiver;
        if (message.amount != null && message.hasOwnProperty('amount'))
            if (typeof message.amount === 'number')
                object.amount = options.longs === String ? String(message.amount) : message.amount;
            else
                object.amount = options.longs === String ? $util.Long.prototype.toString.call(message.amount) : options.longs === Number ? new $util.LongBits(message.amount.low >>> 0, message.amount.high >>> 0).toNumber(true) : message.amount;
        return object;
    };

    /**
     * Converts this SendMoneyTransaction to JSON.
     * @function toJSON
     * @memberof SendMoneyTransaction
     * @instance
     * @returns {Object.<string,*>} JSON object
     */
    SendMoneyTransaction.prototype.toJSON = function toJSON() {
        return this.constructor.toObject(this, $protobuf.util.toJSONOptions);
    };

    return SendMoneyTransaction;
})();

$root.StakeTransaction = (function() {

    /**
     * Properties of a StakeTransaction.
     * @exports IStakeTransaction
     * @interface IStakeTransaction
     * @property {number|Long|null} [nonce] StakeTransaction nonce
     * @property {string|null} [originator] StakeTransaction originator
     * @property {number|Long|null} [amount] StakeTransaction amount
     */

    /**
     * Constructs a new StakeTransaction.
     * @exports StakeTransaction
     * @classdesc Represents a StakeTransaction.
     * @implements IStakeTransaction
     * @constructor
     * @param {IStakeTransaction=} [properties] Properties to set
     */
    function StakeTransaction(properties) {
        if (properties)
            for (var keys = Object.keys(properties), i = 0; i < keys.length; ++i)
                if (properties[keys[i]] != null)
                    this[keys[i]] = properties[keys[i]];
    }

    /**
     * StakeTransaction nonce.
     * @member {number|Long} nonce
     * @memberof StakeTransaction
     * @instance
     */
    StakeTransaction.prototype.nonce = $util.Long ? $util.Long.fromBits(0,0,true) : 0;

    /**
     * StakeTransaction originator.
     * @member {string} originator
     * @memberof StakeTransaction
     * @instance
     */
    StakeTransaction.prototype.originator = '';

    /**
     * StakeTransaction amount.
     * @member {number|Long} amount
     * @memberof StakeTransaction
     * @instance
     */
    StakeTransaction.prototype.amount = $util.Long ? $util.Long.fromBits(0,0,true) : 0;

    /**
     * Creates a new StakeTransaction instance using the specified properties.
     * @function create
     * @memberof StakeTransaction
     * @static
     * @param {IStakeTransaction=} [properties] Properties to set
     * @returns {StakeTransaction} StakeTransaction instance
     */
    StakeTransaction.create = function create(properties) {
        return new StakeTransaction(properties);
    };

    /**
     * Encodes the specified StakeTransaction message. Does not implicitly {@link StakeTransaction.verify|verify} messages.
     * @function encode
     * @memberof StakeTransaction
     * @static
     * @param {IStakeTransaction} message StakeTransaction message or plain object to encode
     * @param {$protobuf.Writer} [writer] Writer to encode to
     * @returns {$protobuf.Writer} Writer
     */
    StakeTransaction.encode = function encode(message, writer) {
        if (!writer)
            writer = $Writer.create();
        if (message.nonce != null && message.hasOwnProperty('nonce'))
            writer.uint32(/* id 1, wireType 0 =*/8).uint64(message.nonce);
        if (message.originator != null && message.hasOwnProperty('originator'))
            writer.uint32(/* id 2, wireType 2 =*/18).string(message.originator);
        if (message.amount != null && message.hasOwnProperty('amount'))
            writer.uint32(/* id 3, wireType 0 =*/24).uint64(message.amount);
        return writer;
    };

    /**
     * Encodes the specified StakeTransaction message, length delimited. Does not implicitly {@link StakeTransaction.verify|verify} messages.
     * @function encodeDelimited
     * @memberof StakeTransaction
     * @static
     * @param {IStakeTransaction} message StakeTransaction message or plain object to encode
     * @param {$protobuf.Writer} [writer] Writer to encode to
     * @returns {$protobuf.Writer} Writer
     */
    StakeTransaction.encodeDelimited = function encodeDelimited(message, writer) {
        return this.encode(message, writer).ldelim();
    };

    /**
     * Decodes a StakeTransaction message from the specified reader or buffer.
     * @function decode
     * @memberof StakeTransaction
     * @static
     * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
     * @param {number} [length] Message length if known beforehand
     * @returns {StakeTransaction} StakeTransaction
     * @throws {Error} If the payload is not a reader or valid buffer
     * @throws {$protobuf.util.ProtocolError} If required fields are missing
     */
    StakeTransaction.decode = function decode(reader, length) {
        if (!(reader instanceof $Reader))
            reader = $Reader.create(reader);
        var end = length === undefined ? reader.len : reader.pos + length, message = new $root.StakeTransaction();
        while (reader.pos < end) {
            var tag = reader.uint32();
            switch (tag >>> 3) {
            case 1:
                message.nonce = reader.uint64();
                break;
            case 2:
                message.originator = reader.string();
                break;
            case 3:
                message.amount = reader.uint64();
                break;
            default:
                reader.skipType(tag & 7);
                break;
            }
        }
        return message;
    };

    /**
     * Decodes a StakeTransaction message from the specified reader or buffer, length delimited.
     * @function decodeDelimited
     * @memberof StakeTransaction
     * @static
     * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
     * @returns {StakeTransaction} StakeTransaction
     * @throws {Error} If the payload is not a reader or valid buffer
     * @throws {$protobuf.util.ProtocolError} If required fields are missing
     */
    StakeTransaction.decodeDelimited = function decodeDelimited(reader) {
        if (!(reader instanceof $Reader))
            reader = new $Reader(reader);
        return this.decode(reader, reader.uint32());
    };

    /**
     * Verifies a StakeTransaction message.
     * @function verify
     * @memberof StakeTransaction
     * @static
     * @param {Object.<string,*>} message Plain object to verify
     * @returns {string|null} `null` if valid, otherwise the reason why it is not
     */
    StakeTransaction.verify = function verify(message) {
        if (typeof message !== 'object' || message === null)
            return 'object expected';
        if (message.nonce != null && message.hasOwnProperty('nonce'))
            if (!$util.isInteger(message.nonce) && !(message.nonce && $util.isInteger(message.nonce.low) && $util.isInteger(message.nonce.high)))
                return 'nonce: integer|Long expected';
        if (message.originator != null && message.hasOwnProperty('originator'))
            if (!$util.isString(message.originator))
                return 'originator: string expected';
        if (message.amount != null && message.hasOwnProperty('amount'))
            if (!$util.isInteger(message.amount) && !(message.amount && $util.isInteger(message.amount.low) && $util.isInteger(message.amount.high)))
                return 'amount: integer|Long expected';
        return null;
    };

    /**
     * Creates a StakeTransaction message from a plain object. Also converts values to their respective internal types.
     * @function fromObject
     * @memberof StakeTransaction
     * @static
     * @param {Object.<string,*>} object Plain object
     * @returns {StakeTransaction} StakeTransaction
     */
    StakeTransaction.fromObject = function fromObject(object) {
        if (object instanceof $root.StakeTransaction)
            return object;
        var message = new $root.StakeTransaction();
        if (object.nonce != null)
            if ($util.Long)
                (message.nonce = $util.Long.fromValue(object.nonce)).unsigned = true;
            else if (typeof object.nonce === 'string')
                message.nonce = parseInt(object.nonce, 10);
            else if (typeof object.nonce === 'number')
                message.nonce = object.nonce;
            else if (typeof object.nonce === 'object')
                message.nonce = new $util.LongBits(object.nonce.low >>> 0, object.nonce.high >>> 0).toNumber(true);
        if (object.originator != null)
            message.originator = String(object.originator);
        if (object.amount != null)
            if ($util.Long)
                (message.amount = $util.Long.fromValue(object.amount)).unsigned = true;
            else if (typeof object.amount === 'string')
                message.amount = parseInt(object.amount, 10);
            else if (typeof object.amount === 'number')
                message.amount = object.amount;
            else if (typeof object.amount === 'object')
                message.amount = new $util.LongBits(object.amount.low >>> 0, object.amount.high >>> 0).toNumber(true);
        return message;
    };

    /**
     * Creates a plain object from a StakeTransaction message. Also converts values to other types if specified.
     * @function toObject
     * @memberof StakeTransaction
     * @static
     * @param {StakeTransaction} message StakeTransaction
     * @param {$protobuf.IConversionOptions} [options] Conversion options
     * @returns {Object.<string,*>} Plain object
     */
    StakeTransaction.toObject = function toObject(message, options) {
        if (!options)
            options = {};
        var object = {};
        if (options.defaults) {
            if ($util.Long) {
                var long = new $util.Long(0, 0, true);
                object.nonce = options.longs === String ? long.toString() : options.longs === Number ? long.toNumber() : long;
            } else
                object.nonce = options.longs === String ? '0' : 0;
            object.originator = '';
            if ($util.Long) {
                var long = new $util.Long(0, 0, true);
                object.amount = options.longs === String ? long.toString() : options.longs === Number ? long.toNumber() : long;
            } else
                object.amount = options.longs === String ? '0' : 0;
        }
        if (message.nonce != null && message.hasOwnProperty('nonce'))
            if (typeof message.nonce === 'number')
                object.nonce = options.longs === String ? String(message.nonce) : message.nonce;
            else
                object.nonce = options.longs === String ? $util.Long.prototype.toString.call(message.nonce) : options.longs === Number ? new $util.LongBits(message.nonce.low >>> 0, message.nonce.high >>> 0).toNumber(true) : message.nonce;
        if (message.originator != null && message.hasOwnProperty('originator'))
            object.originator = message.originator;
        if (message.amount != null && message.hasOwnProperty('amount'))
            if (typeof message.amount === 'number')
                object.amount = options.longs === String ? String(message.amount) : message.amount;
            else
                object.amount = options.longs === String ? $util.Long.prototype.toString.call(message.amount) : options.longs === Number ? new $util.LongBits(message.amount.low >>> 0, message.amount.high >>> 0).toNumber(true) : message.amount;
        return object;
    };

    /**
     * Converts this StakeTransaction to JSON.
     * @function toJSON
     * @memberof StakeTransaction
     * @instance
     * @returns {Object.<string,*>} JSON object
     */
    StakeTransaction.prototype.toJSON = function toJSON() {
        return this.constructor.toObject(this, $protobuf.util.toJSONOptions);
    };

    return StakeTransaction;
})();

$root.SwapKeyTransaction = (function() {

    /**
     * Properties of a SwapKeyTransaction.
     * @exports ISwapKeyTransaction
     * @interface ISwapKeyTransaction
     * @property {number|Long|null} [nonce] SwapKeyTransaction nonce
     * @property {string|null} [originator] SwapKeyTransaction originator
     * @property {Uint8Array|null} [curKey] SwapKeyTransaction curKey
     * @property {Uint8Array|null} [newKey] SwapKeyTransaction newKey
     */

    /**
     * Constructs a new SwapKeyTransaction.
     * @exports SwapKeyTransaction
     * @classdesc Represents a SwapKeyTransaction.
     * @implements ISwapKeyTransaction
     * @constructor
     * @param {ISwapKeyTransaction=} [properties] Properties to set
     */
    function SwapKeyTransaction(properties) {
        if (properties)
            for (var keys = Object.keys(properties), i = 0; i < keys.length; ++i)
                if (properties[keys[i]] != null)
                    this[keys[i]] = properties[keys[i]];
    }

    /**
     * SwapKeyTransaction nonce.
     * @member {number|Long} nonce
     * @memberof SwapKeyTransaction
     * @instance
     */
    SwapKeyTransaction.prototype.nonce = $util.Long ? $util.Long.fromBits(0,0,true) : 0;

    /**
     * SwapKeyTransaction originator.
     * @member {string} originator
     * @memberof SwapKeyTransaction
     * @instance
     */
    SwapKeyTransaction.prototype.originator = '';

    /**
     * SwapKeyTransaction curKey.
     * @member {Uint8Array} curKey
     * @memberof SwapKeyTransaction
     * @instance
     */
    SwapKeyTransaction.prototype.curKey = $util.newBuffer([]);

    /**
     * SwapKeyTransaction newKey.
     * @member {Uint8Array} newKey
     * @memberof SwapKeyTransaction
     * @instance
     */
    SwapKeyTransaction.prototype.newKey = $util.newBuffer([]);

    /**
     * Creates a new SwapKeyTransaction instance using the specified properties.
     * @function create
     * @memberof SwapKeyTransaction
     * @static
     * @param {ISwapKeyTransaction=} [properties] Properties to set
     * @returns {SwapKeyTransaction} SwapKeyTransaction instance
     */
    SwapKeyTransaction.create = function create(properties) {
        return new SwapKeyTransaction(properties);
    };

    /**
     * Encodes the specified SwapKeyTransaction message. Does not implicitly {@link SwapKeyTransaction.verify|verify} messages.
     * @function encode
     * @memberof SwapKeyTransaction
     * @static
     * @param {ISwapKeyTransaction} message SwapKeyTransaction message or plain object to encode
     * @param {$protobuf.Writer} [writer] Writer to encode to
     * @returns {$protobuf.Writer} Writer
     */
    SwapKeyTransaction.encode = function encode(message, writer) {
        if (!writer)
            writer = $Writer.create();
        if (message.nonce != null && message.hasOwnProperty('nonce'))
            writer.uint32(/* id 1, wireType 0 =*/8).uint64(message.nonce);
        if (message.originator != null && message.hasOwnProperty('originator'))
            writer.uint32(/* id 2, wireType 2 =*/18).string(message.originator);
        if (message.curKey != null && message.hasOwnProperty('curKey'))
            writer.uint32(/* id 3, wireType 2 =*/26).bytes(message.curKey);
        if (message.newKey != null && message.hasOwnProperty('newKey'))
            writer.uint32(/* id 4, wireType 2 =*/34).bytes(message.newKey);
        return writer;
    };

    /**
     * Encodes the specified SwapKeyTransaction message, length delimited. Does not implicitly {@link SwapKeyTransaction.verify|verify} messages.
     * @function encodeDelimited
     * @memberof SwapKeyTransaction
     * @static
     * @param {ISwapKeyTransaction} message SwapKeyTransaction message or plain object to encode
     * @param {$protobuf.Writer} [writer] Writer to encode to
     * @returns {$protobuf.Writer} Writer
     */
    SwapKeyTransaction.encodeDelimited = function encodeDelimited(message, writer) {
        return this.encode(message, writer).ldelim();
    };

    /**
     * Decodes a SwapKeyTransaction message from the specified reader or buffer.
     * @function decode
     * @memberof SwapKeyTransaction
     * @static
     * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
     * @param {number} [length] Message length if known beforehand
     * @returns {SwapKeyTransaction} SwapKeyTransaction
     * @throws {Error} If the payload is not a reader or valid buffer
     * @throws {$protobuf.util.ProtocolError} If required fields are missing
     */
    SwapKeyTransaction.decode = function decode(reader, length) {
        if (!(reader instanceof $Reader))
            reader = $Reader.create(reader);
        var end = length === undefined ? reader.len : reader.pos + length, message = new $root.SwapKeyTransaction();
        while (reader.pos < end) {
            var tag = reader.uint32();
            switch (tag >>> 3) {
            case 1:
                message.nonce = reader.uint64();
                break;
            case 2:
                message.originator = reader.string();
                break;
            case 3:
                message.curKey = reader.bytes();
                break;
            case 4:
                message.newKey = reader.bytes();
                break;
            default:
                reader.skipType(tag & 7);
                break;
            }
        }
        return message;
    };

    /**
     * Decodes a SwapKeyTransaction message from the specified reader or buffer, length delimited.
     * @function decodeDelimited
     * @memberof SwapKeyTransaction
     * @static
     * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
     * @returns {SwapKeyTransaction} SwapKeyTransaction
     * @throws {Error} If the payload is not a reader or valid buffer
     * @throws {$protobuf.util.ProtocolError} If required fields are missing
     */
    SwapKeyTransaction.decodeDelimited = function decodeDelimited(reader) {
        if (!(reader instanceof $Reader))
            reader = new $Reader(reader);
        return this.decode(reader, reader.uint32());
    };

    /**
     * Verifies a SwapKeyTransaction message.
     * @function verify
     * @memberof SwapKeyTransaction
     * @static
     * @param {Object.<string,*>} message Plain object to verify
     * @returns {string|null} `null` if valid, otherwise the reason why it is not
     */
    SwapKeyTransaction.verify = function verify(message) {
        if (typeof message !== 'object' || message === null)
            return 'object expected';
        if (message.nonce != null && message.hasOwnProperty('nonce'))
            if (!$util.isInteger(message.nonce) && !(message.nonce && $util.isInteger(message.nonce.low) && $util.isInteger(message.nonce.high)))
                return 'nonce: integer|Long expected';
        if (message.originator != null && message.hasOwnProperty('originator'))
            if (!$util.isString(message.originator))
                return 'originator: string expected';
        if (message.curKey != null && message.hasOwnProperty('curKey'))
            if (!(message.curKey && typeof message.curKey.length === 'number' || $util.isString(message.curKey)))
                return 'curKey: buffer expected';
        if (message.newKey != null && message.hasOwnProperty('newKey'))
            if (!(message.newKey && typeof message.newKey.length === 'number' || $util.isString(message.newKey)))
                return 'newKey: buffer expected';
        return null;
    };

    /**
     * Creates a SwapKeyTransaction message from a plain object. Also converts values to their respective internal types.
     * @function fromObject
     * @memberof SwapKeyTransaction
     * @static
     * @param {Object.<string,*>} object Plain object
     * @returns {SwapKeyTransaction} SwapKeyTransaction
     */
    SwapKeyTransaction.fromObject = function fromObject(object) {
        if (object instanceof $root.SwapKeyTransaction)
            return object;
        var message = new $root.SwapKeyTransaction();
        if (object.nonce != null)
            if ($util.Long)
                (message.nonce = $util.Long.fromValue(object.nonce)).unsigned = true;
            else if (typeof object.nonce === 'string')
                message.nonce = parseInt(object.nonce, 10);
            else if (typeof object.nonce === 'number')
                message.nonce = object.nonce;
            else if (typeof object.nonce === 'object')
                message.nonce = new $util.LongBits(object.nonce.low >>> 0, object.nonce.high >>> 0).toNumber(true);
        if (object.originator != null)
            message.originator = String(object.originator);
        if (object.curKey != null)
            if (typeof object.curKey === 'string')
                $util.base64.decode(object.curKey, message.curKey = $util.newBuffer($util.base64.length(object.curKey)), 0);
            else if (object.curKey.length)
                message.curKey = object.curKey;
        if (object.newKey != null)
            if (typeof object.newKey === 'string')
                $util.base64.decode(object.newKey, message.newKey = $util.newBuffer($util.base64.length(object.newKey)), 0);
            else if (object.newKey.length)
                message.newKey = object.newKey;
        return message;
    };

    /**
     * Creates a plain object from a SwapKeyTransaction message. Also converts values to other types if specified.
     * @function toObject
     * @memberof SwapKeyTransaction
     * @static
     * @param {SwapKeyTransaction} message SwapKeyTransaction
     * @param {$protobuf.IConversionOptions} [options] Conversion options
     * @returns {Object.<string,*>} Plain object
     */
    SwapKeyTransaction.toObject = function toObject(message, options) {
        if (!options)
            options = {};
        var object = {};
        if (options.defaults) {
            if ($util.Long) {
                var long = new $util.Long(0, 0, true);
                object.nonce = options.longs === String ? long.toString() : options.longs === Number ? long.toNumber() : long;
            } else
                object.nonce = options.longs === String ? '0' : 0;
            object.originator = '';
            if (options.bytes === String)
                object.curKey = '';
            else {
                object.curKey = [];
                if (options.bytes !== Array)
                    object.curKey = $util.newBuffer(object.curKey);
            }
            if (options.bytes === String)
                object.newKey = '';
            else {
                object.newKey = [];
                if (options.bytes !== Array)
                    object.newKey = $util.newBuffer(object.newKey);
            }
        }
        if (message.nonce != null && message.hasOwnProperty('nonce'))
            if (typeof message.nonce === 'number')
                object.nonce = options.longs === String ? String(message.nonce) : message.nonce;
            else
                object.nonce = options.longs === String ? $util.Long.prototype.toString.call(message.nonce) : options.longs === Number ? new $util.LongBits(message.nonce.low >>> 0, message.nonce.high >>> 0).toNumber(true) : message.nonce;
        if (message.originator != null && message.hasOwnProperty('originator'))
            object.originator = message.originator;
        if (message.curKey != null && message.hasOwnProperty('curKey'))
            object.curKey = options.bytes === String ? $util.base64.encode(message.curKey, 0, message.curKey.length) : options.bytes === Array ? Array.prototype.slice.call(message.curKey) : message.curKey;
        if (message.newKey != null && message.hasOwnProperty('newKey'))
            object.newKey = options.bytes === String ? $util.base64.encode(message.newKey, 0, message.newKey.length) : options.bytes === Array ? Array.prototype.slice.call(message.newKey) : message.newKey;
        return object;
    };

    /**
     * Converts this SwapKeyTransaction to JSON.
     * @function toJSON
     * @memberof SwapKeyTransaction
     * @instance
     * @returns {Object.<string,*>} JSON object
     */
    SwapKeyTransaction.prototype.toJSON = function toJSON() {
        return this.constructor.toObject(this, $protobuf.util.toJSONOptions);
    };

    return SwapKeyTransaction;
})();

$root.SignedTransaction = (function() {

    /**
     * Properties of a SignedTransaction.
     * @exports ISignedTransaction
     * @interface ISignedTransaction
     * @property {Uint8Array|null} [signature] SignedTransaction signature
     * @property {ICreateAccountTransaction|null} [createAccount] SignedTransaction createAccount
     * @property {IDeployContractTransaction|null} [deployContract] SignedTransaction deployContract
     * @property {IFunctionCallTransaction|null} [functionCall] SignedTransaction functionCall
     * @property {ISendMoneyTransaction|null} [sendMoney] SignedTransaction sendMoney
     * @property {IStakeTransaction|null} [stake] SignedTransaction stake
     * @property {ISwapKeyTransaction|null} [swapKey] SignedTransaction swapKey
     */

    /**
     * Constructs a new SignedTransaction.
     * @exports SignedTransaction
     * @classdesc Represents a SignedTransaction.
     * @implements ISignedTransaction
     * @constructor
     * @param {ISignedTransaction=} [properties] Properties to set
     */
    function SignedTransaction(properties) {
        if (properties)
            for (var keys = Object.keys(properties), i = 0; i < keys.length; ++i)
                if (properties[keys[i]] != null)
                    this[keys[i]] = properties[keys[i]];
    }

    /**
     * SignedTransaction signature.
     * @member {Uint8Array} signature
     * @memberof SignedTransaction
     * @instance
     */
    SignedTransaction.prototype.signature = $util.newBuffer([]);

    /**
     * SignedTransaction createAccount.
     * @member {ICreateAccountTransaction|null|undefined} createAccount
     * @memberof SignedTransaction
     * @instance
     */
    SignedTransaction.prototype.createAccount = null;

    /**
     * SignedTransaction deployContract.
     * @member {IDeployContractTransaction|null|undefined} deployContract
     * @memberof SignedTransaction
     * @instance
     */
    SignedTransaction.prototype.deployContract = null;

    /**
     * SignedTransaction functionCall.
     * @member {IFunctionCallTransaction|null|undefined} functionCall
     * @memberof SignedTransaction
     * @instance
     */
    SignedTransaction.prototype.functionCall = null;

    /**
     * SignedTransaction sendMoney.
     * @member {ISendMoneyTransaction|null|undefined} sendMoney
     * @memberof SignedTransaction
     * @instance
     */
    SignedTransaction.prototype.sendMoney = null;

    /**
     * SignedTransaction stake.
     * @member {IStakeTransaction|null|undefined} stake
     * @memberof SignedTransaction
     * @instance
     */
    SignedTransaction.prototype.stake = null;

    /**
     * SignedTransaction swapKey.
     * @member {ISwapKeyTransaction|null|undefined} swapKey
     * @memberof SignedTransaction
     * @instance
     */
    SignedTransaction.prototype.swapKey = null;

    // OneOf field names bound to virtual getters and setters
    var $oneOfFields;

    /**
     * SignedTransaction body.
     * @member {"createAccount"|"deployContract"|"functionCall"|"sendMoney"|"stake"|"swapKey"|undefined} body
     * @memberof SignedTransaction
     * @instance
     */
    Object.defineProperty(SignedTransaction.prototype, 'body', {
        get: $util.oneOfGetter($oneOfFields = ['createAccount', 'deployContract', 'functionCall', 'sendMoney', 'stake', 'swapKey']),
        set: $util.oneOfSetter($oneOfFields)
    });

    /**
     * Creates a new SignedTransaction instance using the specified properties.
     * @function create
     * @memberof SignedTransaction
     * @static
     * @param {ISignedTransaction=} [properties] Properties to set
     * @returns {SignedTransaction} SignedTransaction instance
     */
    SignedTransaction.create = function create(properties) {
        return new SignedTransaction(properties);
    };

    /**
     * Encodes the specified SignedTransaction message. Does not implicitly {@link SignedTransaction.verify|verify} messages.
     * @function encode
     * @memberof SignedTransaction
     * @static
     * @param {ISignedTransaction} message SignedTransaction message or plain object to encode
     * @param {$protobuf.Writer} [writer] Writer to encode to
     * @returns {$protobuf.Writer} Writer
     */
    SignedTransaction.encode = function encode(message, writer) {
        if (!writer)
            writer = $Writer.create();
        if (message.signature != null && message.hasOwnProperty('signature'))
            writer.uint32(/* id 1, wireType 2 =*/10).bytes(message.signature);
        if (message.createAccount != null && message.hasOwnProperty('createAccount'))
            $root.CreateAccountTransaction.encode(message.createAccount, writer.uint32(/* id 2, wireType 2 =*/18).fork()).ldelim();
        if (message.deployContract != null && message.hasOwnProperty('deployContract'))
            $root.DeployContractTransaction.encode(message.deployContract, writer.uint32(/* id 3, wireType 2 =*/26).fork()).ldelim();
        if (message.functionCall != null && message.hasOwnProperty('functionCall'))
            $root.FunctionCallTransaction.encode(message.functionCall, writer.uint32(/* id 4, wireType 2 =*/34).fork()).ldelim();
        if (message.sendMoney != null && message.hasOwnProperty('sendMoney'))
            $root.SendMoneyTransaction.encode(message.sendMoney, writer.uint32(/* id 5, wireType 2 =*/42).fork()).ldelim();
        if (message.stake != null && message.hasOwnProperty('stake'))
            $root.StakeTransaction.encode(message.stake, writer.uint32(/* id 6, wireType 2 =*/50).fork()).ldelim();
        if (message.swapKey != null && message.hasOwnProperty('swapKey'))
            $root.SwapKeyTransaction.encode(message.swapKey, writer.uint32(/* id 7, wireType 2 =*/58).fork()).ldelim();
        return writer;
    };

    /**
     * Encodes the specified SignedTransaction message, length delimited. Does not implicitly {@link SignedTransaction.verify|verify} messages.
     * @function encodeDelimited
     * @memberof SignedTransaction
     * @static
     * @param {ISignedTransaction} message SignedTransaction message or plain object to encode
     * @param {$protobuf.Writer} [writer] Writer to encode to
     * @returns {$protobuf.Writer} Writer
     */
    SignedTransaction.encodeDelimited = function encodeDelimited(message, writer) {
        return this.encode(message, writer).ldelim();
    };

    /**
     * Decodes a SignedTransaction message from the specified reader or buffer.
     * @function decode
     * @memberof SignedTransaction
     * @static
     * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
     * @param {number} [length] Message length if known beforehand
     * @returns {SignedTransaction} SignedTransaction
     * @throws {Error} If the payload is not a reader or valid buffer
     * @throws {$protobuf.util.ProtocolError} If required fields are missing
     */
    SignedTransaction.decode = function decode(reader, length) {
        if (!(reader instanceof $Reader))
            reader = $Reader.create(reader);
        var end = length === undefined ? reader.len : reader.pos + length, message = new $root.SignedTransaction();
        while (reader.pos < end) {
            var tag = reader.uint32();
            switch (tag >>> 3) {
            case 1:
                message.signature = reader.bytes();
                break;
            case 2:
                message.createAccount = $root.CreateAccountTransaction.decode(reader, reader.uint32());
                break;
            case 3:
                message.deployContract = $root.DeployContractTransaction.decode(reader, reader.uint32());
                break;
            case 4:
                message.functionCall = $root.FunctionCallTransaction.decode(reader, reader.uint32());
                break;
            case 5:
                message.sendMoney = $root.SendMoneyTransaction.decode(reader, reader.uint32());
                break;
            case 6:
                message.stake = $root.StakeTransaction.decode(reader, reader.uint32());
                break;
            case 7:
                message.swapKey = $root.SwapKeyTransaction.decode(reader, reader.uint32());
                break;
            default:
                reader.skipType(tag & 7);
                break;
            }
        }
        return message;
    };

    /**
     * Decodes a SignedTransaction message from the specified reader or buffer, length delimited.
     * @function decodeDelimited
     * @memberof SignedTransaction
     * @static
     * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
     * @returns {SignedTransaction} SignedTransaction
     * @throws {Error} If the payload is not a reader or valid buffer
     * @throws {$protobuf.util.ProtocolError} If required fields are missing
     */
    SignedTransaction.decodeDelimited = function decodeDelimited(reader) {
        if (!(reader instanceof $Reader))
            reader = new $Reader(reader);
        return this.decode(reader, reader.uint32());
    };

    /**
     * Verifies a SignedTransaction message.
     * @function verify
     * @memberof SignedTransaction
     * @static
     * @param {Object.<string,*>} message Plain object to verify
     * @returns {string|null} `null` if valid, otherwise the reason why it is not
     */
    SignedTransaction.verify = function verify(message) {
        if (typeof message !== 'object' || message === null)
            return 'object expected';
        var properties = {};
        if (message.signature != null && message.hasOwnProperty('signature'))
            if (!(message.signature && typeof message.signature.length === 'number' || $util.isString(message.signature)))
                return 'signature: buffer expected';
        if (message.createAccount != null && message.hasOwnProperty('createAccount')) {
            properties.body = 1;
            {
                var error = $root.CreateAccountTransaction.verify(message.createAccount);
                if (error)
                    return 'createAccount.' + error;
            }
        }
        if (message.deployContract != null && message.hasOwnProperty('deployContract')) {
            if (properties.body === 1)
                return 'body: multiple values';
            properties.body = 1;
            {
                var error = $root.DeployContractTransaction.verify(message.deployContract);
                if (error)
                    return 'deployContract.' + error;
            }
        }
        if (message.functionCall != null && message.hasOwnProperty('functionCall')) {
            if (properties.body === 1)
                return 'body: multiple values';
            properties.body = 1;
            {
                var error = $root.FunctionCallTransaction.verify(message.functionCall);
                if (error)
                    return 'functionCall.' + error;
            }
        }
        if (message.sendMoney != null && message.hasOwnProperty('sendMoney')) {
            if (properties.body === 1)
                return 'body: multiple values';
            properties.body = 1;
            {
                var error = $root.SendMoneyTransaction.verify(message.sendMoney);
                if (error)
                    return 'sendMoney.' + error;
            }
        }
        if (message.stake != null && message.hasOwnProperty('stake')) {
            if (properties.body === 1)
                return 'body: multiple values';
            properties.body = 1;
            {
                var error = $root.StakeTransaction.verify(message.stake);
                if (error)
                    return 'stake.' + error;
            }
        }
        if (message.swapKey != null && message.hasOwnProperty('swapKey')) {
            if (properties.body === 1)
                return 'body: multiple values';
            properties.body = 1;
            {
                var error = $root.SwapKeyTransaction.verify(message.swapKey);
                if (error)
                    return 'swapKey.' + error;
            }
        }
        return null;
    };

    /**
     * Creates a SignedTransaction message from a plain object. Also converts values to their respective internal types.
     * @function fromObject
     * @memberof SignedTransaction
     * @static
     * @param {Object.<string,*>} object Plain object
     * @returns {SignedTransaction} SignedTransaction
     */
    SignedTransaction.fromObject = function fromObject(object) {
        if (object instanceof $root.SignedTransaction)
            return object;
        var message = new $root.SignedTransaction();
        if (object.signature != null)
            if (typeof object.signature === 'string')
                $util.base64.decode(object.signature, message.signature = $util.newBuffer($util.base64.length(object.signature)), 0);
            else if (object.signature.length)
                message.signature = object.signature;
        if (object.createAccount != null) {
            if (typeof object.createAccount !== 'object')
                throw TypeError('.SignedTransaction.createAccount: object expected');
            message.createAccount = $root.CreateAccountTransaction.fromObject(object.createAccount);
        }
        if (object.deployContract != null) {
            if (typeof object.deployContract !== 'object')
                throw TypeError('.SignedTransaction.deployContract: object expected');
            message.deployContract = $root.DeployContractTransaction.fromObject(object.deployContract);
        }
        if (object.functionCall != null) {
            if (typeof object.functionCall !== 'object')
                throw TypeError('.SignedTransaction.functionCall: object expected');
            message.functionCall = $root.FunctionCallTransaction.fromObject(object.functionCall);
        }
        if (object.sendMoney != null) {
            if (typeof object.sendMoney !== 'object')
                throw TypeError('.SignedTransaction.sendMoney: object expected');
            message.sendMoney = $root.SendMoneyTransaction.fromObject(object.sendMoney);
        }
        if (object.stake != null) {
            if (typeof object.stake !== 'object')
                throw TypeError('.SignedTransaction.stake: object expected');
            message.stake = $root.StakeTransaction.fromObject(object.stake);
        }
        if (object.swapKey != null) {
            if (typeof object.swapKey !== 'object')
                throw TypeError('.SignedTransaction.swapKey: object expected');
            message.swapKey = $root.SwapKeyTransaction.fromObject(object.swapKey);
        }
        return message;
    };

    /**
     * Creates a plain object from a SignedTransaction message. Also converts values to other types if specified.
     * @function toObject
     * @memberof SignedTransaction
     * @static
     * @param {SignedTransaction} message SignedTransaction
     * @param {$protobuf.IConversionOptions} [options] Conversion options
     * @returns {Object.<string,*>} Plain object
     */
    SignedTransaction.toObject = function toObject(message, options) {
        if (!options)
            options = {};
        var object = {};
        if (options.defaults)
            if (options.bytes === String)
                object.signature = '';
            else {
                object.signature = [];
                if (options.bytes !== Array)
                    object.signature = $util.newBuffer(object.signature);
            }
        if (message.signature != null && message.hasOwnProperty('signature'))
            object.signature = options.bytes === String ? $util.base64.encode(message.signature, 0, message.signature.length) : options.bytes === Array ? Array.prototype.slice.call(message.signature) : message.signature;
        if (message.createAccount != null && message.hasOwnProperty('createAccount')) {
            object.createAccount = $root.CreateAccountTransaction.toObject(message.createAccount, options);
            if (options.oneofs)
                object.body = 'createAccount';
        }
        if (message.deployContract != null && message.hasOwnProperty('deployContract')) {
            object.deployContract = $root.DeployContractTransaction.toObject(message.deployContract, options);
            if (options.oneofs)
                object.body = 'deployContract';
        }
        if (message.functionCall != null && message.hasOwnProperty('functionCall')) {
            object.functionCall = $root.FunctionCallTransaction.toObject(message.functionCall, options);
            if (options.oneofs)
                object.body = 'functionCall';
        }
        if (message.sendMoney != null && message.hasOwnProperty('sendMoney')) {
            object.sendMoney = $root.SendMoneyTransaction.toObject(message.sendMoney, options);
            if (options.oneofs)
                object.body = 'sendMoney';
        }
        if (message.stake != null && message.hasOwnProperty('stake')) {
            object.stake = $root.StakeTransaction.toObject(message.stake, options);
            if (options.oneofs)
                object.body = 'stake';
        }
        if (message.swapKey != null && message.hasOwnProperty('swapKey')) {
            object.swapKey = $root.SwapKeyTransaction.toObject(message.swapKey, options);
            if (options.oneofs)
                object.body = 'swapKey';
        }
        return object;
    };

    /**
     * Converts this SignedTransaction to JSON.
     * @function toJSON
     * @memberof SignedTransaction
     * @instance
     * @returns {Object.<string,*>} JSON object
     */
    SignedTransaction.prototype.toJSON = function toJSON() {
        return this.constructor.toObject(this, $protobuf.util.toJSONOptions);
    };

    return SignedTransaction;
})();

module.exports = $root;
