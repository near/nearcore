import { FormEvent, useCallback, useContext, useState } from 'react';
import { ComposingQueryContext } from './composing_query';
import { EntityKeyType } from './types';
import { parseEntityKey } from './keys';
import './KeyInput.scss';

export type KeyInputProps = {
    keyType: EntityKeyType;
};

/// A component to accept a string input for a specific type of EntityKey.
/// It works by adding or removing the key from the composing query (from
/// context). If the key is valid it is added, but if it's invalid it's
/// removed.
export const KeyInput = ({ keyType }: KeyInputProps) => {
    const { composingQuery, dispatch } = useContext(ComposingQueryContext);
    const key = composingQuery.keys.find((key) => key.type() === keyType);
    const [text, setText] = useState('');
    const onInput = useCallback(
        (e: FormEvent<HTMLInputElement>) => {
            const value = e.currentTarget.value;
            setText(value);
            const parsedKey = parseEntityKey(keyType, value);
            if (!parsedKey) {
                dispatch({ type: 'remove-key', keyType });
            } else {
                dispatch({ type: 'set-key', key: parsedKey });
            }
        },
        [keyType, dispatch]
    );
    return (
        <div className="key-editor">
            <div className="key-type">{keyType}</div>
            <input
                className={`key-value ${key ? '' : 'key-value-invalid'}`}
                value={key?.toString() || text}
                onInput={onInput}
            />
        </div>
    );
};
