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

export function generateLogs(): void {
  globalStorage.setItem("item", "value");
  near.log("log1");
  near.log("log2");
}

export function triggerAssert(): void {
  near.log("log before assert");
  assert(false, "expected to fail");
}

