import "allocator/arena";
export { memory };

import { contractContext, globalStorage, near } from "./near";

export function hello(name: string): string {

  return "hello " + name;
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

export function store_many(offset: i32, n: i32): void {
  for (let i = 0; i < n; ++i) {
    let key: i32 = offset + i;
    let value: i32 = near.random32() as i32;
    globalStorage.setItem(key.toString(), value.toString());
  }
}

export function read_many(offset: i32, n: i32): string[] {
  let values = new Array<string>(n);
  for (let i = 0; i < n; ++i) {
    let key: i32 = offset + i;
    values[i] = globalStorage.getItem(key.toString());
  }
  return values;
}

export function store_many_strs(keys: string[], values: string[]): void {
  assert(keys.length == values.length, "Should be equal");
  for (let i = 0; i < keys.length; ++i) {
    globalStorage.setItem(keys[i], values[i]);
  }
}

export function read_many_strs(keys: string[]): string[] {
  let values = new Array<string>(keys.length);
  for (let i = 0; i < keys.length; ++i) {
    values[i] = globalStorage.getItem(keys[i]);
  }
  return values;
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
