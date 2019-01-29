
      import { near } from "./near";
      import { JSONEncoder} from "./json/encoder"
      import { JSONDecoder, ThrowingJSONHandler, DecoderState  } from "./json/decoder"
      import {hello as wrapped_hello, setValue as wrapped_setValue, getValue as wrapped_getValue, getAllKeys as wrapped_getAllKeys, generateLogs as wrapped_generateLogs, triggerAssert as wrapped_triggerAssert} from "./main";

      // Runtime functions
      @external("env", "return_value")
      declare function return_value(value_ptr: u32): void;
      @external("env", "input_read_len")
      declare function input_read_len(): u32;
      @external("env", "input_read_into")
      declare function input_read_into(ptr: usize): void;
    
import {contractContext as contractContext,globalStorage as globalStorage,near as near} from "./near";
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
let result = wrapped_hello(
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
let result = wrapped_setValue(
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
let result = wrapped_getValue(

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
export class __near_ArgsParser_getAllKeys extends ThrowingJSONHandler {
        buffer: Uint8Array;
        decoder: JSONDecoder<__near_ArgsParser_getAllKeys>;
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
export function __near_encode_Array_String(
          value: Array<String>,
          encoder: JSONEncoder): void {
for (let i = 0; i < value.length; i++) {
if (value[i] != null) {
            encoder.setString(null, value[i]);
          } else {
            encoder.setNull(null);
          }
}
}
export function near_func_getAllKeys(): void {
      let json = new Uint8Array(input_read_len());
      input_read_into(json.buffer.data);
      let handler = new __near_ArgsParser_getAllKeys();
      handler.buffer = json;
      handler.decoder = new JSONDecoder<__near_ArgsParser_getAllKeys>(handler);
      handler.decoder.deserialize(json);
let result = wrapped_getAllKeys(

);

        let encoder = new JSONEncoder();
        encoder.pushObject(null);
      
if (result != null) {
          encoder.pushArray("result");
          __near_encode_Array_String(<Array<String>>result, encoder);
          encoder.popArray();
        } else {
          encoder.setNull("result");
        }

        encoder.popObject();
        return_value(near.bufferWithSize(encoder.serialize()).buffer.data);
      
}
export class __near_ArgsParser_generateLogs extends ThrowingJSONHandler {
        buffer: Uint8Array;
        decoder: JSONDecoder<__near_ArgsParser_generateLogs>;
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
export function near_func_generateLogs(): void {
      let json = new Uint8Array(input_read_len());
      input_read_into(json.buffer.data);
      let handler = new __near_ArgsParser_generateLogs();
      handler.buffer = json;
      handler.decoder = new JSONDecoder<__near_ArgsParser_generateLogs>(handler);
      handler.decoder.deserialize(json);
wrapped_generateLogs(

);
}
export class __near_ArgsParser_triggerAssert extends ThrowingJSONHandler {
        buffer: Uint8Array;
        decoder: JSONDecoder<__near_ArgsParser_triggerAssert>;
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
export function near_func_triggerAssert(): void {
      let json = new Uint8Array(input_read_len());
      input_read_into(json.buffer.data);
      let handler = new __near_ArgsParser_triggerAssert();
      handler.buffer = json;
      handler.decoder = new JSONDecoder<__near_ArgsParser_triggerAssert>(handler);
      handler.decoder.deserialize(json);
wrapped_triggerAssert(

);
}