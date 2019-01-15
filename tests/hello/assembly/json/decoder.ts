declare function logStr(str: string): void;
declare function logF64(val: f64): void;

/**
 * Extend from this class to handle events from parser.
 * Default implementation traverses whole object tree and does nothing.
 */
export abstract class JSONHandler {
    setString(name: string, value: string): void {
    }

    setBoolean(name: string, value: bool): void {
    }

    setNull(name: string): void {
    }

    setInteger(name: string, value: i32): void {
    }

    pushArray(name: string): bool {
        return true;
    }

    popArray(): void {
    }

    pushObject(name: string): bool {
        return true;
    }

    popObject(): void {
    }
}

/**
 * Extend from this class to handle events from parser.
 * This implementation crashes on every unimplemented set/push method
 * to allow easier validation of input.
 */
export class ThrowingJSONHandler extends JSONHandler {
    setString(name: string, value: string): void {
       assert(false, 'Unexpected string field ' + name + ' : "' + value + '"');
    }

    setBoolean(name: string, value: bool): void {
       assert(false, 'Unexpected bool field ' + name + ' : ' + (value ? 'true' : 'false'));
    }

    setNull(name: string): void {
       assert(false, 'Unexpected null field ' + name);
    }

    setInteger(name: string, value: i32): void {
       let arr: Array<i32> = [value];
       assert(false, 'Unexpected integer field ' + name + ' : ' + arr.toString());
    }

    pushArray(name: string): bool {
        assert(false, 'Unexpected array field ' + name);
        return true;
    }

    pushObject(name: string): bool {
        assert(false, 'Unexpected object field ' + name);
        return true;
    }
}

const TRUE_STR = "true";
const FALSE_STR = "false";
const NULL_STR = "null";
let CHAR_0 = "0".charCodeAt(0);
let CHAR_9 = "9".charCodeAt(0);
let CHAR_A = "A".charCodeAt(0);
let CHAR_A_LOWER = "a".charCodeAt(0);

export class DecoderState {
    readIndex: i32 = 0;
    buffer: Uint8Array = null;
    lastKey: string = null;
}

export class JSONDecoder<JSONHandlerT extends JSONHandler> {

    handler: JSONHandlerT;
    state: DecoderState;

    constructor(handler: JSONHandlerT) {
        this.handler = handler;
    }

    deserialize(buffer: Uint8Array, decoderState: DecoderState = null): void {
        if (decoderState) {
            this.state = decoderState;
        } else {
            this.state = new DecoderState();
            this.state.readIndex = 0;
            this.state.buffer = buffer;
            this.state.lastKey = null;
        }

        assert(this.parseValue(), "Cannot parse JSON");
        // TODO: Error if input left
    }

    private peekChar(): i32 {
        if (this.state.readIndex >= this.state.buffer.length) {
          return -1;
        }
        return this.state.buffer[this.state.readIndex];
    }

    private readChar(): i32 {
        assert(this.state.readIndex < this.state.buffer.length, "Unexpected input end");
        return this.state.buffer[this.state.readIndex++];
    }

    private parseValue(): bool {
        this.skipWhitespace();
        let result = this.parseObject()
            || this.parseArray()
            || this.parseString()
            || this.parseBoolean()
            || this.parseNumber()
            || this.parseNull()
        this.skipWhitespace();
        return result;
    }

    private parseObject(): bool {
        if (this.peekChar() != "{".charCodeAt(0)) {
            return false;
        }
        let key = this.state.lastKey;
        this.state.lastKey = null;
        if (this.handler.pushObject(key)) {
            this.readChar();
            this.skipWhitespace();

            let firstItem = true;
            while (this.peekChar() != "}".charCodeAt(0)) {
                if (!firstItem) {
                    assert(this.readChar() == ",".charCodeAt(0), "Expected ','");
                } else {
                    firstItem = false;
                }
                this.parseKey();
                this.parseValue();
            }
            assert(this.readChar() == "}".charCodeAt(0), "Unexpected end of object");
        }
        this.handler.popObject();
        return true;
    }

    private parseKey(): void {
        this.skipWhitespace();
        this.state.lastKey = this.readString();
        this.skipWhitespace();
        assert(this.readChar() == ":".charCodeAt(0), "Expected ':'");
    }

    private parseArray(): bool {
        if (this.peekChar() != "[".charCodeAt(0)) {
            return false;
        }
        let key = this.state.lastKey;
        this.state.lastKey = null;
        if (this.handler.pushArray(key)) {
            this.readChar();
            this.skipWhitespace();

            let firstItem = true;
            while (this.peekChar() != "]".charCodeAt(0)) {
                if (!firstItem) {
                    assert(this.readChar() == ",".charCodeAt(0), "Expected ','");
                } else {
                    firstItem = false;
                }
                this.parseValue();
            }
            assert(this.readChar() == "]".charCodeAt(0), "Unexpected end of array");
        }
        this.handler.popArray();
        return true;;
    }

    private parseString(): bool {
        if (this.peekChar() != '"'.charCodeAt(0)) {
            return false;
        }
        this.handler.setString(this.state.lastKey, this.readString());
        return true;
    }

    private readString(): string {
        assert(this.readChar() == '"'.charCodeAt(0), "Expected double-quoted string");
        let savedIndex = this.state.readIndex;
        let stringParts: Array<string> = new Array<string>();
        for (;;) {
            let byte = this.readChar();
            assert(byte >= 0x20, "Unexpected control character");
            // TODO: Make sure unicode handled properly
            if (byte == '"'.charCodeAt(0)) {
                stringParts.push(
                    String.fromUTF8(this.state.buffer.buffer.data + savedIndex, this.state.readIndex - savedIndex - 1));
                return stringParts.join("");
            }
            if (byte == "\\".charCodeAt(0)) {
                if (this.state.readIndex > savedIndex + 1) {
                    stringParts.push(
                        String.fromUTF8(this.state.buffer.buffer.data + savedIndex, this.state.readIndex - savedIndex - 1));
                }
                stringParts.push(this.readEscapedChar());
                savedIndex = this.state.readIndex;
            }
        }
        // Should never happen
        return "";
    }

    private readEscapedChar(): string {
        let byte = this.readChar();
        // TODO: Use lookup table for anything except \u
        if (byte == '"'.charCodeAt(0)) {
            return '"';
        }
        if (byte == "\\".charCodeAt(0)) {
            return "\\";
        }
        if (byte == "/".charCodeAt(0)) {
            return "/";
        }
        if (byte == "b".charCodeAt(0)) {
            return "\b";
        }
        if (byte == "n".charCodeAt(0)) {
            return "\n";
        }
        if (byte == "r".charCodeAt(0)) {
            return "\r";
        }
        if (byte == "t".charCodeAt(0)) {
            return "\t";
        }
        if (byte == "u".charCodeAt(0)) {
            let d1 = this.readHexDigit();
            let d2 = this.readHexDigit();
            let d3 = this.readHexDigit();
            let d4 = this.readHexDigit();
            let charCode = d1 * 0x1000 + d2 * 0x100 + d3 * 0x10 + d4;
            return String.fromCodePoint(charCode);
        }
        assert(false, "Unexpected escaped character: " + String.fromCharCode(byte));
        return "";
    }

    private readHexDigit(): i32 {
        let byte = this.readChar();
        let digit = byte - CHAR_0;
        if (digit > 9) {
            digit = byte - CHAR_A + 10;
            if (digit < 10 || digit > 15) {
                digit = byte - CHAR_A_LOWER + 10;
            }
        }
        let arr: Array<i32> = [byte, digit];
        assert(digit >= 0 && digit < 16, "Unexpected \\u digit");
        return digit;
    }

    private parseNumber(): bool {
        // TODO: Parse floats
        let number: i32 = 0;
        let sign: i32 = 1;
        if (this.peekChar() == "-".charCodeAt(0)) {
            sign = -1;
            this.readChar();
        }
        let digits = 0;
        while (CHAR_0 <= this.peekChar() && this.peekChar() <= CHAR_9 ) {
            let byte = this.readChar();
            number *= 10;
            number += byte - CHAR_0;
            digits++;
        }
        if (digits > 0) {
            this.handler.setInteger(this.state.lastKey, number * sign);
            return true;
        }
        return false;
    }

    private parseBoolean(): bool {
        if (this.peekChar() == FALSE_STR.charCodeAt(0)) {
            this.readAndAssert(FALSE_STR);
            this.handler.setBoolean(this.state.lastKey, false);
            return true;
        }
        if (this.peekChar() == TRUE_STR.charCodeAt(0)) {
            this.readAndAssert(TRUE_STR);
            this.handler.setBoolean(this.state.lastKey, true);
            return true;
        }

        return false;
    }

    private parseNull(): bool {
        if (this.peekChar() == NULL_STR.charCodeAt(0)) {
            this.readAndAssert(NULL_STR);
            this.handler.setNull(this.state.lastKey);
            return true;
        }
        return false;
    }

    private readAndAssert(str: string): void {
        for (let i = 0; i < str.length; i++) {
            assert(str.charCodeAt(i) == this.readChar(), "Expected '" + str + "'");
        }
    }

    private skipWhitespace(): void {
        while (this.isWhitespace(this.peekChar())) {
            this.readChar();
        }
    }

    private isWhitespace(charCode: i32): bool {
        return charCode == 0x9 || charCode == 0xa || charCode == 0xd || charCode == 0x20;
    }
}
