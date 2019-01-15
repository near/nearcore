import "allocator/arena";
export { memory };

import { contractContext, globalStorage, near } from "./near";

// --- contract code goes below
// --- bigints temporarily stringly typed, need support in bindgen

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
