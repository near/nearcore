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
  let keys = getAllKeys();
  assert(keys.length == 1);
  assert(keys[0] == "name");
  return globalStorage.getItem("name");
}

export function getAllKeys(): string[] {
  return globalStorage.keys("n");
}