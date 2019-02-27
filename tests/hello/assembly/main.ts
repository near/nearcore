import "allocator/arena";
export { memory };

import { contractContext, globalStorage, near } from "./near";

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

export function triggerAssert(): void {
  near.log("log before assert");
  assert(false, "expected to fail");
}

export function testSetRemove(value: string): void {
    globalStorage.setItem("test", value);
    globalStorage.removeItem("test");
    assert(globalStorage.getItem("test") == null, "Item must be empty");
}
