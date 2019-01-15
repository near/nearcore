declare function logStr(str: string): void;
declare function logF64(val: f64): void;

export class JSONEncoder {
    private isFirstKey: boolean = true
    private inObject: Array<boolean> = [false]
    private result: string = ""

    serialize(): Uint8Array {
        // TODO: Write directly to UTF8 bytes
        let utf8ptr = this.result.toUTF8();
        let buffer = new Uint8Array(this.result.lengthUTF8);
        for (let i = 0; i <  buffer.length; i++) {
            buffer[i] = load<u8>(utf8ptr + i);
        }
        return buffer.subarray(0, buffer.length - 1);
    }

    setString(name: string, value: string): void {
        this.writeKey(name);
        this.writeString(value);
    }

    setBoolean(name: string, value: bool): void {
        this.writeKey(name);
        this.writeBoolean(value);
    }

    setNull(name: string): void {
        this.writeKey(name);
        this.write("null");
    }

    setInteger(name: string, value: i32): void {
        this.writeKey(name);
        this.writeInteger(value);
    }

    pushArray(name: string): bool {
        this.writeKey(name);
        this.write("[");
        this.isFirstKey = true
        this.inObject.push(false);
        return true;
    }

    popArray(): void {
        this.write("]");
    }

    pushObject(name: string): bool {
        this.writeKey(name);
        this.write("{");
        this.isFirstKey = true
        this.inObject.push(true);
        return true;
    }

    popObject(): void {
        this.write("}");
    }

    private writeKey(str: string): void {
        if (!this.isFirstKey ) {
            this.write(",");
        } else {
            this.isFirstKey = false;
        }
        if (str != null) {
            this.writeString(str);
            this.write(":");
        }
    }

    private writeString(str: string): void {
        this.write('"');
        let savedIndex = 0;
        for (let i = 0; i < str.length; i++) {
            let char = str.charCodeAt(i);
            let needsEscaping = char < 0x20 || char == '"'.charCodeAt(0) || char == '\\'.charCodeAt(0);
            if (needsEscaping) {
                this.write(str.substring(savedIndex, i));
                savedIndex = i + 1;
                if (char == '"'.charCodeAt(0)) {
                    this.write('\\"');
                } else if (char == "\\".charCodeAt(0)) {
                    this.write("\\\\");
                } else if (char == "\b".charCodeAt(0)) {
                    this.write("\\b");
                } else if (char == "\n".charCodeAt(0)) {
                    this.write("\\n");
                } else if (char == "\r".charCodeAt(0)) {
                    this.write("\\r");
                } else if (char == "\t".charCodeAt(0)) {
                    this.write("\\t");
                } else {
                    // TODO: Implement encoding for other contol characters
                    assert(false, "Unsupported control chracter");
                }
            }
        }
        this.write(str.substring(savedIndex, str.length));
        this.write('"');
    }

    private writeBoolean(value: bool): void {
        this.write(value ? "true" : "false");
    }

    private writeInteger(value: i32): void {
        // TODO: More efficient encoding
        let arr: Array<i32> = [value];
        this.write(arr.toString());
    }

    private write(str: string): void {
        this.result += str;
    }
}