//@nearfile out
import { context, storage, ContractPromise, near, logging } from "near-runtime-ts";

import { PromiseArgs, InputPromiseArgs, MyCallbackResult, MyContractPromiseResult } from "./model";

import { u128 } from "bignum";

export function hello(name: string): string {

  return "hello " + name;
}

export function setKeyValue(key: string, value: string): void {
  storage.setString(key, value);
}

export function getValueByKey(key: string): string {
  return storage.getString(key)!;
}

export function setValue(value: string): string {
  storage.setString("name", value);
  return value;
}

export function getValue(): string {
  return storage.getString("name")!;
}

export function getAllKeys(): string[] {
  let keys = storage.keys("n");
  assert(keys.length == 1);
  assert(keys[0] == "name");
  return keys;
}

export function benchmark(): string[] {
  let i = 0;
  while (i < 10) {
    storage.setString(i.toString(), "123123");
    i += 1;
  }
  return storage.keys("");
}

export function benchmark_storage(n: i32): string {
  let i = 0;
  while (i < n) {
    storage.setString(i.toString(), i.toString());
    i += 1;
  }
  i = 0;
  let sum: u64 = 0;
  while (i < n) {
    let item = I32.parseInt(storage.getString(i.toString()));
    sum += item;
    i += 1;
  }
  return sum.toString()
}

export function limited_storage(max_storage: u64): string {
  let i = 0;
  while (context.storageUsage <= max_storage) {
    i += 1;
    storage.setString(i.toString(), i.toString());
  }
  if (context.storageUsage > max_storage) {
    storage.delete(i.toString());
  }
  return i.toString()
}

export function benchmark_sum_n(n: i32): string {
  let i = 0;
  let sum: u64 = 0;
  while (i < n) {
    sum += i;
    i += 1;
  }
  return sum.toString()
}

export function generateLogs(): void {
  storage.setString("item", "value");
  logging.log("log1");
  logging.log("log2");
}

export function returnHiWithLogs(): string {
  logging.log("loooog1");
  logging.log("loooog2");
  return "Hi"
}

export function triggerAssert(): void {
  logging.log("log before assert");
  assert(false, "expected to fail");
}

export function testSetRemove(value: string): void {
  storage.setString("test", value);
  storage.delete("test");
  assert(storage.getString("test") == null, "Item must be empty");
}

function buildString(n: i32): string {
  assert(n >= 0);
  let result = "";
  for (let i = 20; i >= 0; --i) {
    result = result + result;
    if ((n >> i) & 1) {
      result += "a";
    }
  }
  return result;
}

export function insertStrings(from: i32, to: i32): void {
  let str = buildString(to);
  for (let i = from; i < to; i++) {
    storage.setString(str.substr(to - i) + "b", "x");
  }
}

export function deleteStrings(from: i32, to: i32): void {
  let str = buildString(to);
  for (let i = to - 1; i >= from; i--) {
    storage.delete(str.substr(to - i) + "b");
  }
}

export function recurse(n: i32): i32 {
  if (n <= 0) {
    return n;
  }
  return recurse(n - 1) + 1;
}

// For testing promises

export function callPromise(args: PromiseArgs): void {
  let inputArgs: InputPromiseArgs = { args: args.args };
  let balance = args.balance as u64;
  let promise = ContractPromise.create(
      args.receiver,
      args.methodName,
      inputArgs.encode().serialize(),
      args.gas,
      new u128(args.balance));
  if (args.callback) {
    inputArgs.args = args.callbackArgs;
    let callbackBalance = args.callbackBalance as u64;

    promise = promise.then(
        context.contractName,
        args.callback,
        inputArgs.encode().serialize(),
        args.callbackGas,
        new u128(callbackBalance)
    );
  }
  promise.returnAsResult();
}

export function callbackWithName(args: PromiseArgs): MyCallbackResult {
  let contractResults = ContractPromise.getResults();
  let allRes = Array.create<MyContractPromiseResult>(contractResults.length);
  for (let i = 0; i < contractResults.length; ++i) {
    allRes[i] = new MyContractPromiseResult();
    allRes[i].ok = (contractResults[i].status == 1);
    if (allRes[i].ok && contractResults[i].buffer != null && contractResults[i].buffer.length > 0) {
      allRes[i].r = MyCallbackResult.decode(contractResults[i].buffer);
    }
  }
  let result: MyCallbackResult = {
    rs: allRes,
    n: context.contractName,
  };
  let bytes = result.encode().serialize();
  storage.setBytes("lastResult", bytes);
  return result;
}

export function getLastResult(): MyCallbackResult {
  return MyCallbackResult.decode(storage.getBytes("lastResult"));
}
