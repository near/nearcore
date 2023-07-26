import { useContext } from 'react';
import { PinnedKeysContext } from './pinned_keys';
import './PinnedKeysView.scss';

export const PinnedKeysView = () => {
    const { keys, dispatch } = useContext(PinnedKeysContext);
    return (
        <div className="pinned-keys">
            <div className="pinned-keys-title">Pinned keys:</div>
            {keys.map((key) => (
                <div key={key.type()} className="pinned-key">
                    <div className="pinned-key-type">{key.type()}</div>
                    <div className="pinned-key-value">{key.toString()}</div>
                    <div
                        className="pinned-key-delete"
                        onClick={() => dispatch({ type: 'remove-key', keyType: key.type() })}>
                        ×
                    </div>
                </div>
            ))}
            {keys.length == 0 && (
                <div className="pinned-keys-empty">(Click an entity key to pin it)</div>
            )}
        </div>
    );
};
