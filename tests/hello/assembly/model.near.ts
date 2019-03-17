
      import { near } from "./near";
      import { JSONEncoder} from "./json/encoder"
      import { JSONDecoder, ThrowingJSONHandler, DecoderState  } from "./json/decoder"
      import {PromiseArgs as wrapped_PromiseArgs, InputPromiseArgs as wrapped_InputPromiseArgs, MyContractPromiseResult as wrapped_MyContractPromiseResult, MyCallbackResult as wrapped_MyCallbackResult, ResultWrappedMyCallbackResult as wrapped_ResultWrappedMyCallbackResult} from "./model";

      // Runtime functions
      @external("env", "return_value")
      declare function return_value(value_ptr: u32): void;
      @external("env", "input_read_len")
      declare function input_read_len(): u32;
      @external("env", "input_read_into")
      declare function input_read_into(ptr: usize): void;
    
export function __near_encode_PromiseArgs(
          value: wrapped_PromiseArgs,
          encoder: JSONEncoder): void {
if (value.receiver != null) {
            encoder.setString("receiver", value.receiver);
          } else {
            encoder.setNull("receiver");
          }
if (value.methodName != null) {
            encoder.setString("methodName", value.methodName);
          } else {
            encoder.setNull("methodName");
          }
if (value.args != null) {
          encoder.pushObject("args");
          __near_encode_PromiseArgs(<PromiseArgs>value.args, encoder);
          encoder.popObject();
        } else {
          encoder.setNull("args");
        }
encoder.setInteger("additionalMana", value.additionalMana);
if (value.callback != null) {
            encoder.setString("callback", value.callback);
          } else {
            encoder.setNull("callback");
          }
if (value.callbackArgs != null) {
          encoder.pushObject("callbackArgs");
          __near_encode_PromiseArgs(<PromiseArgs>value.callbackArgs, encoder);
          encoder.popObject();
        } else {
          encoder.setNull("callbackArgs");
        }
encoder.setInteger("callbackAdditionalMana", value.callbackAdditionalMana);
}
export class __near_JSONHandler_PromiseArgs extends ThrowingJSONHandler {
      buffer: Uint8Array;
      decoder: JSONDecoder<__near_JSONHandler_PromiseArgs>;
      handledRoot: boolean = false;
      value: wrapped_PromiseArgs = new wrapped_PromiseArgs();
setInteger(name: string, value: i64): void {
if (name == "additionalMana") {
            this.value.additionalMana = <i32>value;
            return;
          }
if (name == "callbackAdditionalMana") {
            this.value.callbackAdditionalMana = <i32>value;
            return;
          }

        super.setInteger(name, value);
      }
setString(name: string, value: String): void {
if (name == "receiver") {
            this.value.receiver = <String>value;
            return;
          }
if (name == "methodName") {
            this.value.methodName = <String>value;
            return;
          }
if (name == "callback") {
            this.value.callback = <String>value;
            return;
          }

        super.setString(name, value);
      }
setNull(name: string): void {
if (name == "receiver") {
        this.value.receiver = <String>null;
        return;
      }
if (name == "methodName") {
        this.value.methodName = <String>null;
        return;
      }
if (name == "args") {
        this.value.args = <wrapped_PromiseArgs>null;
        return;
      }
if (name == "additionalMana") {
        this.value.additionalMana = <i32>null;
        return;
      }
if (name == "callback") {
        this.value.callback = <String>null;
        return;
      }
if (name == "callbackArgs") {
        this.value.callbackArgs = <wrapped_PromiseArgs>null;
        return;
      }
if (name == "callbackAdditionalMana") {
        this.value.callbackAdditionalMana = <i32>null;
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
          this.value.args = <PromiseArgs>__near_decode_PromiseArgs(this.buffer, this.decoder.state);
          return false;
        }
if (name == "callbackArgs") {
          this.value.callbackArgs = <PromiseArgs>__near_decode_PromiseArgs(this.buffer, this.decoder.state);
          return false;
        }

        return super.pushObject(name);
      }

      pushArray(name: string): bool {

        return super.pushArray(name);
      }
}

export function __near_decode_PromiseArgs(
        buffer: Uint8Array, state: DecoderState):wrapped_PromiseArgs {
      let handler = new __near_JSONHandler_PromiseArgs();
      handler.buffer = buffer;
      handler.decoder = new JSONDecoder<__near_JSONHandler_PromiseArgs>(handler);
      handler.decoder.deserialize(buffer, state);
      return handler.value;
    }

export function __near_encode_InputPromiseArgs(
          value: wrapped_InputPromiseArgs,
          encoder: JSONEncoder): void {
if (value.args != null) {
          encoder.pushObject("args");
          __near_encode_PromiseArgs(<PromiseArgs>value.args, encoder);
          encoder.popObject();
        } else {
          encoder.setNull("args");
        }
}
export class __near_JSONHandler_InputPromiseArgs extends ThrowingJSONHandler {
      buffer: Uint8Array;
      decoder: JSONDecoder<__near_JSONHandler_InputPromiseArgs>;
      handledRoot: boolean = false;
      value: wrapped_InputPromiseArgs = new wrapped_InputPromiseArgs();
setNull(name: string): void {
if (name == "args") {
        this.value.args = <wrapped_PromiseArgs>null;
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
          this.value.args = <PromiseArgs>__near_decode_PromiseArgs(this.buffer, this.decoder.state);
          return false;
        }

        return super.pushObject(name);
      }

      pushArray(name: string): bool {

        return super.pushArray(name);
      }
}

export function __near_decode_InputPromiseArgs(
        buffer: Uint8Array, state: DecoderState):wrapped_InputPromiseArgs {
      let handler = new __near_JSONHandler_InputPromiseArgs();
      handler.buffer = buffer;
      handler.decoder = new JSONDecoder<__near_JSONHandler_InputPromiseArgs>(handler);
      handler.decoder.deserialize(buffer, state);
      return handler.value;
    }

export function __near_encode_Array_MyContractPromiseResult(
          value: Array<wrapped_MyContractPromiseResult>,
          encoder: JSONEncoder): void {
for (let i = 0; i < value.length; i++) {
if (value[i] != null) {
          encoder.pushObject(null);
          __near_encode_MyContractPromiseResult(<MyContractPromiseResult>value[i], encoder);
          encoder.popObject();
        } else {
          encoder.setNull(null);
        }
}
}
export function __near_encode_MyCallbackResult(
          value: wrapped_MyCallbackResult,
          encoder: JSONEncoder): void {
if (value.rs != null) {
          encoder.pushArray("rs");
          __near_encode_Array_MyContractPromiseResult(<Array<MyContractPromiseResult>>value.rs, encoder);
          encoder.popArray();
        } else {
          encoder.setNull("rs");
        }
if (value.n != null) {
            encoder.setString("n", value.n);
          } else {
            encoder.setNull("n");
          }
}
export function __near_encode_MyContractPromiseResult(
          value: wrapped_MyContractPromiseResult,
          encoder: JSONEncoder): void {
encoder.setBoolean("ok", value.ok);
if (value.r != null) {
          encoder.pushObject("r");
          __near_encode_MyCallbackResult(<MyCallbackResult>value.r, encoder);
          encoder.popObject();
        } else {
          encoder.setNull("r");
        }
}
export class __near_JSONHandler_MyContractPromiseResult extends ThrowingJSONHandler {
      buffer: Uint8Array;
      decoder: JSONDecoder<__near_JSONHandler_MyContractPromiseResult>;
      handledRoot: boolean = false;
      value: wrapped_MyContractPromiseResult = new wrapped_MyContractPromiseResult();
setBoolean(name: string, value: bool): void {
if (name == "ok") {
            this.value.ok = <bool>value;
            return;
          }

        super.setBoolean(name, value);
      }
setNull(name: string): void {
if (name == "ok") {
        this.value.ok = <bool>null;
        return;
      }
if (name == "r") {
        this.value.r = <wrapped_MyCallbackResult>null;
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
if (name == "r") {
          this.value.r = <MyCallbackResult>__near_decode_MyCallbackResult(this.buffer, this.decoder.state);
          return false;
        }

        return super.pushObject(name);
      }

      pushArray(name: string): bool {

        return super.pushArray(name);
      }
}

export class __near_JSONHandler_MyCallbackResult extends ThrowingJSONHandler {
      buffer: Uint8Array;
      decoder: JSONDecoder<__near_JSONHandler_MyCallbackResult>;
      handledRoot: boolean = false;
      value: wrapped_MyCallbackResult = new wrapped_MyCallbackResult();
setString(name: string, value: String): void {
if (name == "n") {
            this.value.n = <String>value;
            return;
          }

        super.setString(name, value);
      }
setNull(name: string): void {
if (name == "rs") {
        this.value.rs = <Array<wrapped_MyContractPromiseResult>>null;
        return;
      }
if (name == "n") {
        this.value.n = <String>null;
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
if (name == "rs") {
          this.value.rs = <Array<MyContractPromiseResult>>__near_decode_Array_MyContractPromiseResult(this.buffer, this.decoder.state);
          return false;
        }

        return super.pushArray(name);
      }
}

export class __near_JSONHandler_Array_MyContractPromiseResult extends ThrowingJSONHandler {
      buffer: Uint8Array;
      decoder: JSONDecoder<__near_JSONHandler_Array_MyContractPromiseResult>;
      handledRoot: boolean = false;
      value: Array<wrapped_MyContractPromiseResult> = new Array<wrapped_MyContractPromiseResult>();
pushObject(name: string): bool {
        this.value.push(<MyContractPromiseResult>__near_decode_MyContractPromiseResult(this.buffer, this.decoder.state));
        return false;
      }
      pushArray(name: string): bool {
        assert(name == null);
        if (!this.handledRoot) {
          this.handledRoot = true;
          return true;
        }
        this.value.push(<MyContractPromiseResult>__near_decode_MyContractPromiseResult(this.buffer, this.decoder.state));
        return false;
      }
}

export function __near_decode_Array_MyContractPromiseResult(
        buffer: Uint8Array, state: DecoderState):Array<wrapped_MyContractPromiseResult> {
      let handler = new __near_JSONHandler_Array_MyContractPromiseResult();
      handler.buffer = buffer;
      handler.decoder = new JSONDecoder<__near_JSONHandler_Array_MyContractPromiseResult>(handler);
      handler.decoder.deserialize(buffer, state);
      return handler.value;
    }

export function __near_decode_MyCallbackResult(
        buffer: Uint8Array, state: DecoderState):wrapped_MyCallbackResult {
      let handler = new __near_JSONHandler_MyCallbackResult();
      handler.buffer = buffer;
      handler.decoder = new JSONDecoder<__near_JSONHandler_MyCallbackResult>(handler);
      handler.decoder.deserialize(buffer, state);
      return handler.value;
    }

export function __near_decode_MyContractPromiseResult(
        buffer: Uint8Array, state: DecoderState):wrapped_MyContractPromiseResult {
      let handler = new __near_JSONHandler_MyContractPromiseResult();
      handler.buffer = buffer;
      handler.decoder = new JSONDecoder<__near_JSONHandler_MyContractPromiseResult>(handler);
      handler.decoder.deserialize(buffer, state);
      return handler.value;
    }

export function __near_encode_ResultWrappedMyCallbackResult(
          value: wrapped_ResultWrappedMyCallbackResult,
          encoder: JSONEncoder): void {
if (value.result != null) {
          encoder.pushObject("result");
          __near_encode_MyCallbackResult(<MyCallbackResult>value.result, encoder);
          encoder.popObject();
        } else {
          encoder.setNull("result");
        }
}
export class __near_JSONHandler_ResultWrappedMyCallbackResult extends ThrowingJSONHandler {
      buffer: Uint8Array;
      decoder: JSONDecoder<__near_JSONHandler_ResultWrappedMyCallbackResult>;
      handledRoot: boolean = false;
      value: wrapped_ResultWrappedMyCallbackResult = new wrapped_ResultWrappedMyCallbackResult();
setNull(name: string): void {
if (name == "result") {
        this.value.result = <wrapped_MyCallbackResult>null;
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
if (name == "result") {
          this.value.result = <MyCallbackResult>__near_decode_MyCallbackResult(this.buffer, this.decoder.state);
          return false;
        }

        return super.pushObject(name);
      }

      pushArray(name: string): bool {

        return super.pushArray(name);
      }
}

export function __near_decode_ResultWrappedMyCallbackResult(
        buffer: Uint8Array, state: DecoderState):wrapped_ResultWrappedMyCallbackResult {
      let handler = new __near_JSONHandler_ResultWrappedMyCallbackResult();
      handler.buffer = buffer;
      handler.decoder = new JSONDecoder<__near_JSONHandler_ResultWrappedMyCallbackResult>(handler);
      handler.decoder.deserialize(buffer, state);
      return handler.value;
    }

export class PromiseArgs extends wrapped_PromiseArgs {
        static decode(json: Uint8Array): PromiseArgs {
          return <PromiseArgs>__near_decode_PromiseArgs(json, null);
        }

        encode(): Uint8Array {
          let encoder: JSONEncoder = new JSONEncoder();
          encoder.pushObject(null);
          __near_encode_PromiseArgs(<PromiseArgs>this, encoder);
          encoder.popObject();
          return encoder.serialize();
        }
      }
export class InputPromiseArgs extends wrapped_InputPromiseArgs {
        static decode(json: Uint8Array): InputPromiseArgs {
          return <InputPromiseArgs>__near_decode_InputPromiseArgs(json, null);
        }

        encode(): Uint8Array {
          let encoder: JSONEncoder = new JSONEncoder();
          encoder.pushObject(null);
          __near_encode_InputPromiseArgs(<InputPromiseArgs>this, encoder);
          encoder.popObject();
          return encoder.serialize();
        }
      }
export class MyContractPromiseResult extends wrapped_MyContractPromiseResult {
        static decode(json: Uint8Array): MyContractPromiseResult {
          return <MyContractPromiseResult>__near_decode_MyContractPromiseResult(json, null);
        }

        encode(): Uint8Array {
          let encoder: JSONEncoder = new JSONEncoder();
          encoder.pushObject(null);
          __near_encode_MyContractPromiseResult(<MyContractPromiseResult>this, encoder);
          encoder.popObject();
          return encoder.serialize();
        }
      }
export class MyCallbackResult extends wrapped_MyCallbackResult {
        static decode(json: Uint8Array): MyCallbackResult {
          return <MyCallbackResult>__near_decode_MyCallbackResult(json, null);
        }

        encode(): Uint8Array {
          let encoder: JSONEncoder = new JSONEncoder();
          encoder.pushObject(null);
          __near_encode_MyCallbackResult(<MyCallbackResult>this, encoder);
          encoder.popObject();
          return encoder.serialize();
        }
      }
export class ResultWrappedMyCallbackResult extends wrapped_ResultWrappedMyCallbackResult {
        static decode(json: Uint8Array): ResultWrappedMyCallbackResult {
          return <ResultWrappedMyCallbackResult>__near_decode_ResultWrappedMyCallbackResult(json, null);
        }

        encode(): Uint8Array {
          let encoder: JSONEncoder = new JSONEncoder();
          encoder.pushObject(null);
          __near_encode_ResultWrappedMyCallbackResult(<ResultWrappedMyCallbackResult>this, encoder);
          encoder.popObject();
          return encoder.serialize();
        }
      }