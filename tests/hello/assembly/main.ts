import "allocator/arena";
export { memory };

import { contractContext, globalStorage, ContractPromise, ContractPromiseResult, near } from "./near";

import { PromiseArgs, InputPromiseArgs, MyCallbackResult, MyContractPromiseResult, ResultWrappedMyCallbackResult } from "./model.near";

export function hello(name: string): string {

  return "hello " + name;
}

export function setKeyValue(key: string, value: string): void {
  globalStorage.setItem(key, value);
}

export function getValueByKey(key: string): string {
  return globalStorage.getItem(key);
}

export function setValue(value: string): string {
  globalStorage.setItem("name", value);
  return value;
}

export function getValue(): string {
  return globalStorage.getItem("name");
}

export function getAllKeys(): string[] {
  let keys = globalStorage.keys("n");
  assert(keys.length == 1);
  assert(keys[0] == "name");
  return keys;
}

export function benchmark(): string[] {
  let i = 0;
  while (i < 10) {
    globalStorage.setItem(i.toString(), "123123");
    i += 1;
  }
  return globalStorage.keys("");
}

export function benchmark_storage(n: i32): string {
  let i = 0;
  while (i < n) {
    globalStorage.setItem(i.toString(), i.toString());
    i += 1;
  }
  i = 0;
  let sum: u64 = 0;
  while (i < n) {
    let item = I32.parseInt(globalStorage.getItem(i.toString()));
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
  globalStorage.setItem("item", "value");
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
  globalStorage.setItem("test", value);
  globalStorage.removeItem("test");
  assert(globalStorage.getItem("test") == null, "Item must be empty");
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
    if (allRes[i].ok && contractResults[i].buffer.length > 0) {
      allRes[i].r = ResultWrappedMyCallbackResult.decode(contractResults[i].buffer).result;
    }
  } 
  let result: MyCallbackResult = {
    rs: allRes,
    n: contractContext.contractName,
  }
  let bytes = result.encode();
  near.log(strFromBytes(bytes));
  globalStorage.setBytes("lastResult", bytes);
  return result;
}

export function getLastResult(): MyCallbackResult {
  return MyCallbackResult.decode(globalStorage.getBytes("lastResult"));
}

