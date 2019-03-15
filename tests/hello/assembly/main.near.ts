
      import { near } from "./near";
      import { JSONEncoder} from "./json/encoder"
      import { JSONDecoder, ThrowingJSONHandler, DecoderState  } from "./json/decoder"
      import {hello as wrapped_hello, setKeyValue as wrapped_setKeyValue, getValueByKey as wrapped_getValueByKey, setValue as wrapped_setValue, getValue as wrapped_getValue, getAllKeys as wrapped_getAllKeys, benchmark as wrapped_benchmark, benchmark_storage as wrapped_benchmark_storage, benchmark_sum_n as wrapped_benchmark_sum_n, generateLogs as wrapped_generateLogs, returnHiWithLogs as wrapped_returnHiWithLogs, triggerAssert as wrapped_triggerAssert, testSetRemove as wrapped_testSetRemove, callPromise as wrapped_callPromise, callbackWithName as wrapped_callbackWithName, getLastResult as wrapped_getLastResult} from "./main";

      // Runtime functions
      @external("env", "return_value")
      declare function return_value(value_ptr: u32): void;
      @external("env", "input_read_len")
      declare function input_read_len(): u32;
      @external("env", "input_read_into")
      declare function input_read_into(ptr: usize): void;
    
import {contractContext as contractContext,globalStorage as globalStorage,ContractPromise as ContractPromise,ContractPromiseResult as ContractPromiseResult,near as near} from "./near";
import {PromiseArgs as PromiseArgs,InputPromiseArgs as InputPromiseArgs,MyCallbackResult as MyCallbackResult,MyContractPromiseResult as MyContractPromiseResult,ResultWrappedMyCallbackResult as ResultWrappedMyCallbackResult} from "./model.near";
export class __near_ArgsParser_hello extends ThrowingJSONHandler {
        buffer: Uint8Array;
        decoder: JSONDecoder<__near_ArgsParser_hello>;
        handledRoot: boolean = false;
      
__near_param_name: String;
setString(name: string, value: String): void {
if (name == "name") {
            this.__near_param_name = <String>value;
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
export class __near_ArgsParser_setKeyValue extends ThrowingJSONHandler {
        buffer: Uint8Array;
        decoder: JSONDecoder<__near_ArgsParser_setKeyValue>;
        handledRoot: boolean = false;
      
__near_param_key: String;
__near_param_value: String;
setString(name: string, value: String): void {
if (name == "key") {
            this.__near_param_key = <String>value;
            return;
          }
if (name == "value") {
            this.__near_param_value = <String>value;
            return;
          }

        super.setString(name, value);
      }
setNull(name: string): void {
if (name == "key") {
        this.__near_param_key = <String>null;
        return;
      }
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
export function near_func_setKeyValue(): void {
      let json = new Uint8Array(input_read_len());
      input_read_into(json.buffer.data);
      let handler = new __near_ArgsParser_setKeyValue();
      handler.buffer = json;
      handler.decoder = new JSONDecoder<__near_ArgsParser_setKeyValue>(handler);
      handler.decoder.deserialize(json);
wrapped_setKeyValue(
handler.__near_param_key,handler.__near_param_value
);
}
export class __near_ArgsParser_getValueByKey extends ThrowingJSONHandler {
        buffer: Uint8Array;
        decoder: JSONDecoder<__near_ArgsParser_getValueByKey>;
        handledRoot: boolean = false;
      
__near_param_key: String;
setString(name: string, value: String): void {
if (name == "key") {
            this.__near_param_key = <String>value;
            return;
          }

        super.setString(name, value);
      }
setNull(name: string): void {
if (name == "key") {
        this.__near_param_key = <String>null;
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
export function near_func_getValueByKey(): void {
      let json = new Uint8Array(input_read_len());
      input_read_into(json.buffer.data);
      let handler = new __near_ArgsParser_getValueByKey();
      handler.buffer = json;
      handler.decoder = new JSONDecoder<__near_ArgsParser_getValueByKey>(handler);
      handler.decoder.deserialize(json);
let result = wrapped_getValueByKey(
handler.__near_param_key
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
            this.__near_param_value = <String>value;
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
export class __near_ArgsParser_benchmark extends ThrowingJSONHandler {
        buffer: Uint8Array;
        decoder: JSONDecoder<__near_ArgsParser_benchmark>;
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
export function near_func_benchmark(): void {
      let json = new Uint8Array(input_read_len());
      input_read_into(json.buffer.data);
      let handler = new __near_ArgsParser_benchmark();
      handler.buffer = json;
      handler.decoder = new JSONDecoder<__near_ArgsParser_benchmark>(handler);
      handler.decoder.deserialize(json);
let result = wrapped_benchmark(

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
export class __near_ArgsParser_benchmark_storage extends ThrowingJSONHandler {
        buffer: Uint8Array;
        decoder: JSONDecoder<__near_ArgsParser_benchmark_storage>;
        handledRoot: boolean = false;
      
__near_param_n: i32;
setInteger(name: string, value: i64): void {
if (name == "n") {
            this.__near_param_n = <i32>value;
            return;
          }

        super.setInteger(name, value);
      }
setNull(name: string): void {
if (name == "n") {
        this.__near_param_n = <i32>null;
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
export function near_func_benchmark_storage(): void {
      let json = new Uint8Array(input_read_len());
      input_read_into(json.buffer.data);
      let handler = new __near_ArgsParser_benchmark_storage();
      handler.buffer = json;
      handler.decoder = new JSONDecoder<__near_ArgsParser_benchmark_storage>(handler);
      handler.decoder.deserialize(json);
let result = wrapped_benchmark_storage(
handler.__near_param_n
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
export class __near_ArgsParser_benchmark_sum_n extends ThrowingJSONHandler {
        buffer: Uint8Array;
        decoder: JSONDecoder<__near_ArgsParser_benchmark_sum_n>;
        handledRoot: boolean = false;
      
__near_param_n: i32;
setInteger(name: string, value: i64): void {
if (name == "n") {
            this.__near_param_n = <i32>value;
            return;
          }

        super.setInteger(name, value);
      }
setNull(name: string): void {
if (name == "n") {
        this.__near_param_n = <i32>null;
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
export function near_func_benchmark_sum_n(): void {
      let json = new Uint8Array(input_read_len());
      input_read_into(json.buffer.data);
      let handler = new __near_ArgsParser_benchmark_sum_n();
      handler.buffer = json;
      handler.decoder = new JSONDecoder<__near_ArgsParser_benchmark_sum_n>(handler);
      handler.decoder.deserialize(json);
let result = wrapped_benchmark_sum_n(
handler.__near_param_n
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
export class __near_ArgsParser_returnHiWithLogs extends ThrowingJSONHandler {
        buffer: Uint8Array;
        decoder: JSONDecoder<__near_ArgsParser_returnHiWithLogs>;
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
export function near_func_returnHiWithLogs(): void {
      let json = new Uint8Array(input_read_len());
      input_read_into(json.buffer.data);
      let handler = new __near_ArgsParser_returnHiWithLogs();
      handler.buffer = json;
      handler.decoder = new JSONDecoder<__near_ArgsParser_returnHiWithLogs>(handler);
      handler.decoder.deserialize(json);
let result = wrapped_returnHiWithLogs(

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
export class __near_ArgsParser_testSetRemove extends ThrowingJSONHandler {
        buffer: Uint8Array;
        decoder: JSONDecoder<__near_ArgsParser_testSetRemove>;
        handledRoot: boolean = false;
      
__near_param_value: String;
setString(name: string, value: String): void {
if (name == "value") {
            this.__near_param_value = <String>value;
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
export function near_func_testSetRemove(): void {
      let json = new Uint8Array(input_read_len());
      input_read_into(json.buffer.data);
      let handler = new __near_ArgsParser_testSetRemove();
      handler.buffer = json;
      handler.decoder = new JSONDecoder<__near_ArgsParser_testSetRemove>(handler);
      handler.decoder.deserialize(json);
wrapped_testSetRemove(
handler.__near_param_value
);
}
import { __near_decode_PromiseArgs } from "./model.near";
export class __near_ArgsParser_callPromise extends ThrowingJSONHandler {
        buffer: Uint8Array;
        decoder: JSONDecoder<__near_ArgsParser_callPromise>;
        handledRoot: boolean = false;
      
__near_param_args: PromiseArgs;
setNull(name: string): void {
if (name == "args") {
        this.__near_param_args = <PromiseArgs>null;
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
if (name == "args") {
          this.__near_param_args = <PromiseArgs>__near_decode_PromiseArgs(this.buffer, this.decoder.state);
          return false;
        }

        return super.pushObject(name);
      }

      pushArray(name: string): bool {

        return super.pushArray(name);
      }
}
export function near_func_callPromise(): void {
      let json = new Uint8Array(input_read_len());
      input_read_into(json.buffer.data);
      let handler = new __near_ArgsParser_callPromise();
      handler.buffer = json;
      handler.decoder = new JSONDecoder<__near_ArgsParser_callPromise>(handler);
      handler.decoder.deserialize(json);
wrapped_callPromise(
handler.__near_param_args
);
}
export class __near_ArgsParser_callbackWithName extends ThrowingJSONHandler {
        buffer: Uint8Array;
        decoder: JSONDecoder<__near_ArgsParser_callbackWithName>;
        handledRoot: boolean = false;
      
__near_param_args: PromiseArgs;
setNull(name: string): void {
if (name == "args") {
        this.__near_param_args = <PromiseArgs>null;
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
if (name == "args") {
          this.__near_param_args = <PromiseArgs>__near_decode_PromiseArgs(this.buffer, this.decoder.state);
          return false;
        }

        return super.pushObject(name);
      }

      pushArray(name: string): bool {

        return super.pushArray(name);
      }
}
import { __near_encode_MyCallbackResult } from "./model.near";
export function near_func_callbackWithName(): void {
      let json = new Uint8Array(input_read_len());
      input_read_into(json.buffer.data);
      let handler = new __near_ArgsParser_callbackWithName();
      handler.buffer = json;
      handler.decoder = new JSONDecoder<__near_ArgsParser_callbackWithName>(handler);
      handler.decoder.deserialize(json);
let result = wrapped_callbackWithName(
handler.__near_param_args
);

        let encoder = new JSONEncoder();
        encoder.pushObject(null);
      
if (result != null) {
          encoder.pushObject("result");
          __near_encode_MyCallbackResult(<MyCallbackResult>result, encoder);
          encoder.popObject();
        } else {
          encoder.setNull("result");
        }

        encoder.popObject();
        return_value(near.bufferWithSize(encoder.serialize()).buffer.data);
      
}
export class __near_ArgsParser_getLastResult extends ThrowingJSONHandler {
        buffer: Uint8Array;
        decoder: JSONDecoder<__near_ArgsParser_getLastResult>;
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
export function near_func_getLastResult(): void {
      let json = new Uint8Array(input_read_len());
      input_read_into(json.buffer.data);
      let handler = new __near_ArgsParser_getLastResult();
      handler.buffer = json;
      handler.decoder = new JSONDecoder<__near_ArgsParser_getLastResult>(handler);
      handler.decoder.deserialize(json);
let result = wrapped_getLastResult(

);

        let encoder = new JSONEncoder();
        encoder.pushObject(null);
      
if (result != null) {
          encoder.pushObject("result");
          __near_encode_MyCallbackResult(<MyCallbackResult>result, encoder);
          encoder.popObject();
        } else {
          encoder.setNull("result");
        }

        encoder.popObject();
        return_value(near.bufferWithSize(encoder.serialize()).buffer.data);
      
}