
      import { near } from "./near";
      import { JSONEncoder} from "./json/encoder"
      import { JSONDecoder, ThrowingJSONHandler, DecoderState  } from "./json/decoder"

      // Runtime functions
      @external("env", "return_value")
      declare function return_value(value_ptr: u32): void;
      @external("env", "input_read_len")
      declare function input_read_len(): u32;
      @external("env", "input_read_into")
      declare function input_read_into(ptr: usize): void;
    
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

export class __near_ArgsParser_hello extends ThrowingJSONHandler {
        buffer: Uint8Array;
        decoder: JSONDecoder<__near_ArgsParser_hello>;
        handledRoot: boolean = false;
      
__near_param_name: String;
setString(name: string, value: String): void {
if (name == "name") {
            this.__near_param_name = value;
            return;
          }

          super.setString(name, value);
        }
setNull(name: string): void {
if (name == "name") {
        this.__near_param_name = <String>null;
        return;
      }

      super.setNull(name);
    }

      pushObject(name: string): bool {
if (!this.handledRoot) {
      assert(name == null);
      this.handledRoot = true;
      return true;
    } else {
      assert(name != null);
    }

        return super.pushObject(name);
      }

      pushArray(name: string): bool {

        return super.pushArray(name);
      }
}
export function near_func_hello(): void {
      let json = new Uint8Array(input_read_len());
      input_read_into(json.buffer.data);
      let handler = new __near_ArgsParser_hello();
      handler.buffer = json;
      handler.decoder = new JSONDecoder<__near_ArgsParser_hello>(handler);
      handler.decoder.deserialize(json);
let result = hello(
handler.__near_param_name
);

        let encoder = new JSONEncoder();
        encoder.pushObject(null);
      
if (result != null) {
            encoder.setString("result", result);
          } else {
            encoder.setNull("result");
          }

        encoder.popObject();
        return_value(near.bufferWithSize(encoder.serialize()).buffer.data);
      
}
export class __near_ArgsParser_setValue extends ThrowingJSONHandler {
        buffer: Uint8Array;
        decoder: JSONDecoder<__near_ArgsParser_setValue>;
        handledRoot: boolean = false;
      
__near_param_value: String;
setString(name: string, value: String): void {
if (name == "value") {
            this.__near_param_value = value;
            return;
          }

          super.setString(name, value);
        }
setNull(name: string): void {
if (name == "value") {
        this.__near_param_value = <String>null;
        return;
      }

      super.setNull(name);
    }

      pushObject(name: string): bool {
if (!this.handledRoot) {
      assert(name == null);
      this.handledRoot = true;
      return true;
    } else {
      assert(name != null);
    }

        return super.pushObject(name);
      }

      pushArray(name: string): bool {

        return super.pushArray(name);
      }
}
export function near_func_setValue(): void {
      let json = new Uint8Array(input_read_len());
      input_read_into(json.buffer.data);
      let handler = new __near_ArgsParser_setValue();
      handler.buffer = json;
      handler.decoder = new JSONDecoder<__near_ArgsParser_setValue>(handler);
      handler.decoder.deserialize(json);
let result = setValue(
handler.__near_param_value
);

        let encoder = new JSONEncoder();
        encoder.pushObject(null);
      
if (result != null) {
            encoder.setString("result", result);
          } else {
            encoder.setNull("result");
          }

        encoder.popObject();
        return_value(near.bufferWithSize(encoder.serialize()).buffer.data);
      
}
export class __near_ArgsParser_getValue extends ThrowingJSONHandler {
        buffer: Uint8Array;
        decoder: JSONDecoder<__near_ArgsParser_getValue>;
        handledRoot: boolean = false;
      
setNull(name: string): void {

      super.setNull(name);
    }

      pushObject(name: string): bool {
if (!this.handledRoot) {
      assert(name == null);
      this.handledRoot = true;
      return true;
    } else {
      assert(name != null);
    }

        return super.pushObject(name);
      }

      pushArray(name: string): bool {

        return super.pushArray(name);
      }
}
export function near_func_getValue(): void {
      let json = new Uint8Array(input_read_len());
      input_read_into(json.buffer.data);
      let handler = new __near_ArgsParser_getValue();
      handler.buffer = json;
      handler.decoder = new JSONDecoder<__near_ArgsParser_getValue>(handler);
      handler.decoder.deserialize(json);
let result = getValue(

);

        let encoder = new JSONEncoder();
        encoder.pushObject(null);
      
if (result != null) {
            encoder.setString("result", result);
          } else {
            encoder.setNull("result");
          }

        encoder.popObject();
        return_value(near.bufferWithSize(encoder.serialize()).buffer.data);
      
}