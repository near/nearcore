type BufferTypeIndex = u32;

const BUFFER_TYPE_ORIGINATOR_ACCOUNT_ID: BufferTypeIndex = 1;
const BUFFER_TYPE_CURRENT_ACCOUNT_ID: BufferTypeIndex = 2;

/**
 * Provides context for contract execution, including information about transaction sender, etc.
 */
class ContractContext {
  /**
   * Account ID of transaction sender.
   */
  get sender(): string {
    return this.getString(BUFFER_TYPE_ORIGINATOR_ACCOUNT_ID, "");
  }

  /**
   * Account ID of contract.
   */
  get contractName(): string {
    return this.getString(BUFFER_TYPE_CURRENT_ACCOUNT_ID, "");
  }

  /**
   * Returns context value with given index and key. Internal usage only for now.
   */
  getString(typeIndex: BufferTypeIndex, key: string): string {
    let len = read_len(typeIndex, near.utf8(key));
    if (len == 0) {
      return null;
    }

    let buf = new Uint8Array(len);
    read_into(typeIndex, near.utf8(key), buf.buffer.data);
    let value = String.fromUTF8(buf.buffer.data, buf.byteLength);
    return value;
  }
}

/**
 * Represents contract storage.
 *
 * For now it's just simple key-value store with prefix queries.
 */
export class GlobalStorage {
  /**
   * Returns list of keys starting with given prefix.
   *
   * NOTE: Must be very careful to avoid exploding amount of compute with this method.
   * Make sure there is a hard limit on number of keys returned even if contract state size grows.
   */
  keys(prefix: string): string[] {
    let result: string[] = [];
    let iterId = storage_iter(near.utf8(prefix));
    do {
      let len = storage_iter_peek_len(iterId);
      if (len > 0) {
        let buf = new Uint8Array(len);
        storage_iter_peek_into(iterId, buf.buffer.data);
        let key = String.fromUTF8(buf.buffer.data, buf.byteLength);
        result.push(key);
      }
    } while (storage_iter_next(iterId));
    return result;
  }

  setItem(key: string, value: string): void {
    this.setString(key, value);
  }
  getItem(key: string): string {
    return this.getString(key);
  }

  /**
   * Store string value under given key. Both key and value are encoded as UTF-8 strings.
   */
  setString(key: string, value: string): void {
    storage_write(near.utf8(key), near.utf8(value));
  }

  /**
   * Get string value stored under given key. Both key and value are encoded as UTF-8 strings.
   */
  getString(key: string): string {
    let len = storage_read_len(near.utf8(key));
    if (len == 0) {
      return null;
    }

    let buf = new Uint8Array(len);
    storage_read_into(near.utf8(key), buf.buffer.data);
    let value = String.fromUTF8(buf.buffer.data, buf.byteLength);
    return value;
  }

  /**
   * Store byte array under given key. Key is encoded as UTF-8 strings.
   * Byte array stored as is.
   *
   * It's convenient to use this together with `domainObject.encode()`.
   */
  setBytes(key: string, value: Uint8Array): void {
    storage_write(near.utf8(key), near.bufferWithSize(value).buffer.data)
  }

  /**
   * Get byte array stored under given key. Key is encoded as UTF-8 strings.
   * Byte array stored as is.
   *
   * It's convenient to use this together with `DomainObject.decode()`.
   */
  getBytes(key: string): Uint8Array {
    let len = storage_read_len(near.utf8(key));
    if (len == 0) {
      return null;
    }

    let buf = new Uint8Array(len);
    storage_read_into(near.utf8(key), buf.buffer.data);
    return buf;
  }

  removeItem(key: string): void {
    storage_remove(near.utf8(key));
  }

  /**
   * Store 64-bit unsigned int under given key. Key is encoded as UTF-8 strings.
   * Number is encoded as decimal string.
   */
  setU64(key: string, value: u64): void {
    this.setItem(key, value.toString());
  }

  /**
   * Get 64-bit unsigned int stored under given key. Key is encoded as UTF-8 strings.
   * Number is encoded as decimal string.
   *
   * @returns int value or 0 if value is not found
   */
  getU64(key: string): u64 {
    return U64.parseInt(this.getItem(key) || "0");
  }
}

export let globalStorage: GlobalStorage = new GlobalStorage();
export let contractContext: ContractContext = new ContractContext();

export namespace near {
  /**
   * Hash given data. Returns hash as 32-byte array.
   * @param data data can be passed as either Uint8Array or anything with .toString (hashed as UTF-8 string).
   */
  export function hash<T>(data: T): Uint8Array {
    let result = new Uint8Array(32);
    let dataToHash : Uint8Array;
    if (data instanceof Uint8Array) {
      dataToHash = bufferWithSize(data);
    } else {
      let str = data.toString();
      dataToHash = bufferWithSizeFromPtr(str.toUTF8(), str.lengthUTF8 - 1)
    }
    _near_hash(dataToHash.buffer.data, result.buffer.data);
    return result;
  }

  /**
   * Hash given data. Returns hash as 32-bit integer.
   * @param data data can be passed as either Uint8Array or anything with .toString (hashed as UTF-8 string).
   */
  export function hash32<T>(data: T): u32 {
    let dataToHash : Uint8Array;
    if (data instanceof Uint8Array) {
      dataToHash = bufferWithSize(data);
    } else {
      let str = data.toString();
      dataToHash = bufferWithSizeFromPtr(str.toUTF8(), str.lengthUTF8 - 1)
    }
    return _near_hash32(dataToHash.buffer.data);
  }

  /**
   * Returns random byte buffer of given length.
   */
  export function randomBuffer(len: u32): Uint8Array {
    let result = new Uint8Array(len);
    _near_random_buf(len, result.buffer.data);
    return result;
  }

  /**
   * Returns random 32-bit integer.
   */
  export function random32(): u32 {
    return _near_random32();
  }

  export function bufferWithSizeFromPtr(ptr: usize, length: usize): Uint8Array {
    let withSize = new Uint8Array(length + 4);
    store<u32>(withSize.buffer.data, length);
    // TODO: Should use better copy routine or better avoid copy altogether
    for (let i = <usize>0; i < length; i++) {
        withSize[i + 4] = load<u8>(ptr + i);
    }
    return withSize;
  }

  export function bufferWithSize(buf: Uint8Array): Uint8Array {
    return bufferWithSizeFromPtr(buf.buffer.data, buf.byteLength);
  }

  export function log(msg: string): void {
    _near_log(<usize>msg);
  }

  export function str<T>(value: T): string {
    let arr: Array<T> = [value];
    return arr.toString();
  }


  export function utf8(value: string): usize {
    return bufferWithSizeFromPtr(value.toUTF8(), value.lengthUTF8 - 1).buffer.data;
  }

  export function base58(source: Uint8Array): string {
    // Code converted from:
    // https://github.com/cryptocoinjs/base-x/blob/master/index.js
    const iFACTOR = 2; // TODO: Calculate precise value to avoid overallocating
    const ALPHABET = '123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz';
    let BASE = ALPHABET.length;
    let LEADER = ALPHABET.charAt(0);

    // Skip & count leading zeroes.
    let zeroes = 0
    let length = 0
    let pbegin = 0
    let pend = source.length

    while (pbegin !== pend && source[pbegin] === 0) {
      pbegin++
      zeroes++
    }

    // Allocate enough space in big-endian base58 representation.
    let size = ((pend - pbegin) * iFACTOR + 1) >>> 0
    let b58 = new Uint8Array(size)

    // Process the bytes.
    while (pbegin !== pend) {
      let carry = i32(source[pbegin])

      // Apply "b58 = b58 * 256 + ch".
      let i = 0
      for (let it = size - 1; (carry !== 0 || i < length) && (it !== -1); it--, i++) {
        carry += (256 * b58[it]) >>> 0
        b58[it] = (carry % BASE) >>> 0
        carry = (carry / BASE) >>> 0
      }

      assert(carry == 0, 'Non-zero carry');
      length = i
      pbegin++
    }

    // Skip leading zeroes in base58 result.
    let it = size - length
    while (it !== size && b58[it] === 0) {
      it++
    }

    // Translate the result into a string.
    let str = LEADER.repeat(zeroes)
    for (; it < size; ++it) str += ALPHABET.charAt(b58[it])

    return str
  }
}

export class ContractPromise {
  id: i32;

  static create(contractName: string, methodName: string, args: Uint8Array, mana: u32, amount: u64 = 0): ContractPromise {
    return { id: promise_create(near.utf8(contractName), near.utf8(methodName), near.bufferWithSize(args).buffer.data, mana, amount) }
  }

  then(methodName: string, args: Uint8Array, mana: u32): ContractPromise {
    return { id: promise_then(this.id, near.utf8(methodName), near.bufferWithSize(args).buffer.data, mana) };
  }

  returnAsResult(): void {
    return_promise(this.id);
  }

  static all(promises: ContractPromise[]): ContractPromise {
    let result: ContractPromise = promises[0];
    for (let i = 1; i < promises.length; i++) {
      result = { id: promise_and(result.id, promises[i].id) };
    }
    return result;
  }

  static getResults() : ContractPromiseResult[] {
    let count = <i32>result_count();
    let results = new Array<ContractPromiseResult>(count);
    for (let i = 0; i < count; i++) {
      let isOk = result_is_ok(i);
      // TODO: Check what to do when not ok
      let buffer: Uint8Array = null;
      if (isOk) {
        let len = result_read_len(i);
        buffer = new Uint8Array(len);
        result_read_into(i, buffer.buffer.data);
      }
      results[i] = { success: isOk, buffer: buffer };
    }
    return results;
  }
}

export class ContractPromiseResult {
  success: bool;
  buffer: Uint8Array;
}

// TODO: Other functions exposed by runtime should be defined here

@external("env", "storage_write")
declare function storage_write(key: usize, value: usize): void;
@external("env", "storage_read_len")
declare function storage_read_len(key: usize): usize;
@external("env", "storage_read_into")
declare function storage_read_into(key: usize, value: usize): void;
@external("env", "storage_remove")
declare function storage_remove(key: usize): void;
@external("env", "storage_iter")
declare function storage_iter(prefix: usize): u32;
@external("env", "storage_iter_next")
declare function storage_iter_next(id: u32): u32;
@external("env", "storage_iter_peek_len")
declare function storage_iter_peek_len(id: u32): usize;
@external("env", "storage_iter_peek_into")
declare function storage_iter_peek_into(id: u32, value: usize): void;

@external("env", "input_read_len")
declare function input_read_len(): usize;
@external("env", "input_read_into")
declare function input_read_into(ptr: usize): void;

@external("env", "result_count")
declare function result_count(): u32;
@external("env", "result_is_ok")
declare function result_is_ok(index: u32): bool;
@external("env", "result_read_len")
declare function result_read_len(index: u32): u32;
@external("env", "result_read_into")
declare function result_read_into(index: u32, value: usize): void;

@external("env", "return_value")
declare function return_value(value_ptr: usize): void;
@external("env", "return_promise")
declare function return_promise(promise_index: u32): void;

@external("env", "read_len")
declare function read_len(type_index: u32, key: usize): u32;
@external("env", "read_into")
declare function read_into(type_index: u32, key: usize, value: usize): void;

@external("env", "promise_create")
declare function promise_create(account_id: usize, method_name: usize, args: usize, mana: u32, amount: u64): u32;

@external("env", "promise_then")
declare function promise_then(promise_index: u32, method_name: usize, args: usize, mana: u32): u32;

@external("env", "promise_and")
declare function promise_and(promise_index1: u32, promise_index2: u32): u32;

/**
 * @hidden
 * Hash buffer is 32 bytes
 */
@external("env", "hash")
declare function _near_hash(buffer: usize, out: usize): void;

/**
 * @hidden
 */
@external("env", "hash32")
declare function _near_hash32(buffer: usize): u32;

/**
 * @hidden
 * Fills given buffer with random u8.
 */
@external("env", "random_buf")
declare function _near_random_buf(len: u32, out: usize): void

/**
 * @hidden
 */
@external("env", "random32")
declare function _near_random32(): u32;

/**
 * @hidden
 */
@external("env", "log")
declare function _near_log(msg_ptr: usize): void;


/*
    // TODO(#350): Refactor read/write APIs to unify them.
    // First 4 bytes are the length of the remaining buffer.
    fn storage_write(key: *const u8, value: *const u8);
    fn storage_read_len(key: *const u8) -> u32;
    fn storage_read_into(key: *const u8, value: *mut u8);

    fn input_read_len() -> u32;
    fn input_read_into(value: *mut u8);

    fn result_count() -> u32;
    fn result_is_ok(index: u32) -> bool;
    fn result_read_len(index: u32) -> u32;
    fn result_read_into(index: u32, value: *mut u8);

    fn return_value(value: *const u8);
    fn return_promise(promise_index: u32);

    // key can be 0 for certain types
    fn read_len(type_index: u32, key: *const u8) -> u32;
    fn read_into(type_index: u32, key: *const u8, value: *mut u8);

    // AccountID is just 32 bytes without the prefix length.
    fn promise_create(
        account_id: *const u8,
        method_name: *const u8,
        arguments: *const u8,
        mana: u32,
        amount: u64,
    ) -> u32;

    fn promise_then(
        promise_index: u32,
        method_name: *const u8,
        arguments: *const u8,
        mana: u32,
    ) -> u32;

    fn promise_and(promise_index1: u32, promise_index2: u32) -> u32;

    fn balance() -> u64;
    fn mana_left() -> u32;
    fn gas_left() -> u64;
    fn received_amount() -> u64;
    fn assert(expr: bool);

    /// Hash buffer is 32 bytes
    fn hash(buffer: *const u8, out: *mut u8);
    fn hash32(buffer: *const u8) -> u32;

    // Fills given buffer with random u8.
    fn random_buf(len: u32, out: *mut u8);
    fn random32() -> u32;

    fn block_index() -> u64;
*/
