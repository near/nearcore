// Concrete implementations for EntityKey.

import { EntityKey, EntityKeyType } from './types';

export class StringEntityKey {
    constructor(private _type: EntityKeyType, private value: string) {}
    type(): EntityKeyType {
        return this._type;
    }
    toString(): string {
        return this.value;
    }
    toJSON(): unknown {
        return this.value;
    }
}

export class NumericEntityKey {
    constructor(private _type: EntityKeyType, private value: number) {}
    type(): EntityKeyType {
        return this._type;
    }
    toString(): string {
        return this.value.toString();
    }
    toJSON(): unknown {
        return this.value;
    }
}

/// Parses an EntityKey from string.
export function parseEntityKey(keyType: EntityKeyType, input: string): EntityKey | null {
    switch (keyType) {
        case 'account_id':
            if (input.length == 0) {
                return null;
            }
            return new StringEntityKey(keyType, input);
        case 'block_height':
        case 'block_ordinal':
            if (/^\d+$/.test(input)) {
                return new NumericEntityKey(keyType, parseInt(input));
            }
            return null;
        case 'block_hash':
        case 'chunk_hash':
        case 'epoch_id':
        case 'receipt_id':
        case 'transaction_hash':
        case 'state_root':
            if (input.length != 44) {
                return null;
            }
            return new StringEntityKey(keyType, input);
        case 'shard_id':
            if (/^\d+$/.test(input)) {
                return new NumericEntityKey(keyType, parseInt(input));
            }
            return null;
        case 'shard_uid':
            if (/^s\d+[.]v\d+$/.test(input)) {
                return new StringEntityKey(keyType, input);
            }
            return null;
        case 'trie_path':
            if (/^s\d+[.]v\d+$\/[0-9A-Za-z]{44}\/[0-9a-f]*$/.test(input)) {
                return new StringEntityKey(keyType, input);
            }
            return null;
        case 'trie_key':
            if (/^([0-9a-f][0-9a-f])*$/.test(input)) {
                return new StringEntityKey(keyType, input);
            }
            return null;
    }
}
