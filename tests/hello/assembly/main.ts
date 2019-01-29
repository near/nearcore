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

export function generateLogs(): void {
  globalStorage.setItem("item", "value");
  near.log("log1");
  near.log("log2");
}

export function triggerAssert(): void {
  near.log("log before assert");
  assert(false, "expected to fail");
}

