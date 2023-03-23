class IndentPrinter {
    result = '';
    indentation = 0;
    isBeginningOfLine = true;

    push(c: string, newlineBefore: boolean, newlineAfter: boolean) {
        if (c == '\n') {
            this._nextLine();
            return;
        }
        if (newlineBefore && !this.isBeginningOfLine) {
            this._nextLine();
        }
        this._appendToLine(c);
        if (newlineAfter) {
            this._nextLine();
        }
    }
    indent() {
        this.indentation++;
    }
    unindent() {
        this.indentation--;
    }
    _nextLine() {
        this.result += '\n';
        this.isBeginningOfLine = true;
    }
    _appendToLine(c: string) {
        if (this.isBeginningOfLine) {
            if (c == ' ') {
                return;
            }
            this.result += ' '.repeat(this.indentation);
            this.isBeginningOfLine = false;
        }
        this.result += c;
    }
}

export function prettyPrint(s: string): string {
    const printer = new IndentPrinter();
    for (let i = 0; i < s.length; i++) {
        const c = s[i];
        const isClose = c === '}' || c === ']' || c === ')';
        const isOpen = c === '{' || c === '[' || c === '(';
        const isComma = c === ',';
        if (isClose) {
            printer.unindent();
        }
        const newlineBefore = isClose;
        const newlineAfter = isOpen || isComma;
        printer.push(c, newlineBefore, newlineAfter);
        if (isOpen) {
            printer.indent();
        }
    }
    return printer.result;
}
