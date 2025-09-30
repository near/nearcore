# StateRecord

`type: Enum`

Enum that describes one of the records in the state storage.

## Account

`type: Unnamed struct`

Record that contains account information for a given account ID.

### account_id

`type: AccountId`

The account ID of the account.

### account

`type:` [Account](../DataStructures/Account.md)

The account structure. Serialized to JSON. U128 types are serialized to strings.


## Data

`type: Unnamed struct`

Record that contains key-value data record for a contract at the given account ID.

### account_id

`type: AccountId`

The account ID of the contract that contains this data record.

### data_key

`type: Vec<u8>`

Data Key serialized in Base64 format.

_NOTE: Key doesn't contain the data separator._

### value

`type: Vec<u8>`

Value serialized in Base64 format.


## Contract

`type: Unnamed struct`

Record that contains a contract code for a given account ID.

### account_id

`type: AccountId`

The account ID of that has the contract.

### code

`type: Vec<u8>`

WASM Binary contract code serialized in Base64 format.


## AccessKey

`type: Unnamed struct`

Record that contains an access key for a given account ID.

### account_id

`type: AccountId`

The account ID of the access key owner.

### public_key

`type: PublicKey`

The public key for the access key in JSON-friendly string format. E.g. `ed25519:5JFfXMziKaotyFM1t4hfzuwh8GZMYCiKHfqw1gTEWMYT`

### access_key

`type:` [AccessKey](../DataStructures/AccessKey.md)

The access key serialized in JSON format.


## PostponedReceipt

`type: Box<Receipt>` [Receipt](../RuntimeSpec/Receipts.md)

Record that contains a receipt that was postponed on a shard (e.g. it's waiting for incoming data).
The receipt is in JSON-friendly format. The receipt can only be an `ActionReceipt`.

NOTE: Box is used to decrease fixed size of the entire enum.


## ReceivedData

`type: Unnamed struct`

Record that contains information about received data for some action receipt, that is not yet received or processed for a given account ID.
The data is received using `DataReceipt` before. See [Receipts](../RuntimeSpec/Receipts.md) for details.

### account_id

`type: AccountId`

The account ID of the receiver of the data.

### data_id

`type: CryptoHash`

Data ID of the data in base58 format.

### data

`type: Option<Vec<u8>>`

Optional data encoded as base64 format or null in JSON.


## DelayedReceipt

`type: Box<Receipt>` [Receipt](../RuntimeSpec/Receipts.md)

Record that contains a receipt that was delayed on a shard. It means the shard was overwhelmed with receipts and it processes receipts from backlog.
The receipt is in JSON-friendly format.  See [Delayed Receipts](../RuntimeSpec/Components/RuntimeCrate.md#delayed-receipts) for details.

NOTE: Box is used to decrease fixed size of the entire enum.
