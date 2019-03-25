import "allocator/arena";
export { memory };

import { context, storage, ContractPromise, ContractPromiseResult, near } from "./near";

import { PromiseArgs, InputPromiseArgs, MyCallbackResult, MyContractPromiseResult } from "./model.near";

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

// For testing promises

export function callPromise(args: PromiseArgs): void {
  let inputArgs: InputPromiseArgs = { args: args.args };
  let promise = ContractPromise.create(
      args.receiver,
      args.methodName,
      inputArgs.encode(),
      args.additionalMana);
  if (args.callback) {
    inputArgs.args = args.callbackArgs;
    promise = promise.then(
        args.callback,
        inputArgs.encode(),
        args.callbackAdditionalMana);
  }
  promise.returnAsResult();
}

function strFromBytes(buffer: Uint8Array): string {
  return String.fromUTF8(buffer.buffer.data, buffer.byteLength);
}

export function callbackWithName(args: PromiseArgs): MyCallbackResult {
  let contractResults = ContractPromise.getResults();
  let allRes = new Array<MyContractPromiseResult>(contractResults.length);
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
  }
  let bytes = result.encode();
  near.log(strFromBytes(bytes));
  storage.setBytes("lastResult", bytes);
  return result;
}

export function getLastResult(): MyCallbackResult {
  return MyCallbackResult.decode(storage.getBytes("lastResult"));
}

