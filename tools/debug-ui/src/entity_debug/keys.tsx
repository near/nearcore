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
            // Length of 32-byte array encoded in base58 is 43 or 44 characters,
            // depending on whether we need additional character or not.
            //
            // Short explanation: 32 bytes are 256 bits, each base58 character
            // encodes log2(58) ≈ 5.858 bits. Then length of 32-byte array
            // encoded in base58 is ≈ 256 / 5.858 ≈ 43.7.
            if (![43, 44].includes(input.length)) {
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
