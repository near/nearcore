import { context, storage, ContractPromise, ContractPromiseResult, near } from "./near";

import { PromiseArgs, InputPromiseArgs, MyCallbackResult, MyContractPromiseResult } from "./model.near";

import { u128 } from "./node_modules/bignum/assembly/integer/u128";

export function hello(name: string): string {

  return "hello " + name;
}

export function setKeyValue(key: string, value: string): void {
  storage.setItem(key, value);
}

export function getValueByKey(key: string): string {
  return storage.getItem(key);
}

export function setValue(value: string): string {
  storage.setItem("name", value);
  return value;
}

export function getValue(): string {
  return storage.getItem("name");
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
    storage.setItem(i.toString(), "123123");
    i += 1;
  }
  return storage.keys("");
}

export function benchmark_storage(n: i32): string {
  let i = 0;
  while (i < n) {
    storage.setItem(i.toString(), i.toString());
    i += 1;
  }
  i = 0;
  let sum: u64 = 0;
  while (i < n) {
    let item = I32.parseInt(storage.getItem(i.toString()));
    sum += item;
    i += 1;
  }
  return sum.toString()
}

export function limited_storage(max_storage: u64): string {
  let i = 0;
  while (context.storageUsage <= max_storage) {
    i += 1;
    storage.setItem(i.toString(), i.toString());
  }
  if (context.storageUsage > max_storage) {
    storage.removeItem(i.toString());
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
  storage.setItem("item", "value");
  near.log("log1");
  near.log("log2");
}

export function returnHiWithLogs(): string {
  near.log("loooog1");
  near.log("loooog2");
  return "Hi"
}

export function triggerAssert(): void {
  near.log("log before assert");
  assert(false, "expected to fail");
}

export function testSetRemove(value: string): void {
  storage.setItem("test", value);
  storage.removeItem("test");
  assert(storage.getItem("test") == null, "Item must be empty");
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
    storage.setItem(str.substr(to - i) + "b", "x");
  }
}

export function deleteStrings(from: i32, to: i32): void {
  let str = buildString(to);
  for (let i = to - 1; i >= from; i--) {
    storage.removeItem(str.substr(to - i) + "b");
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
      inputArgs.encode(),
      new u128(args.balance));
  if (args.callback) {
    inputArgs.args = args.callbackArgs;
    let callbackBalance = args.callbackBalance as u64;
    promise = promise.then(
        args.callback,
        inputArgs.encode(),
        new u128(callbackBalance));
  }
  promise.returnAsResult();
}

export function callbackWithName(args: PromiseArgs): MyCallbackResult {
  let contractResults = ContractPromise.getResults();
  let allRes = Array.create<MyContractPromiseResult>(contractResults.length);
  for (let i = 0; i < contractResults.length; ++i) {
    allRes[i] = new MyContractPromiseResult();
    allRes[i].ok = contractResults[i].success;
    if (allRes[i].ok && contractResults[i].buffer != null && contractResults[i].buffer.length > 0) {
      allRes[i].r = MyCallbackResult.decode(contractResults[i].buffer);
    }
  }
  let result: MyCallbackResult = {
    rs: allRes,
    n: context.contractName,
  };
  let bytes = result.encode();
  storage.setBytes("lastResult", bytes);
  return result;
}

export function getLastResult(): MyCallbackResult {
  return MyCallbackResult.decode(storage.getBytes("lastResult"));
}

