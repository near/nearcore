/// A react data structure for a list of pinned entity keys.
/// See https://react.dev/reference/react/useReducer to understand this pattern.

import { createContext } from 'react';
import { EntityKey, EntityKeyType } from './types';

export type PinnedKeysAction =
    | {
          type: 'add-key';
          key: EntityKey;
      }
    | { type: 'remove-key'; keyType: EntityKeyType };

export function pinnedKeysReducer(keys: EntityKey[], action: PinnedKeysAction): EntityKey[] {
    switch (action.type) {
        case 'add-key':
            if (keys.some((key) => key.type() === action.key.type())) {
                return keys.map((key) => (key.type() === action.key.type() ? action.key : key));
            }
            return [...keys, action.key];
        case 'remove-key':
            return keys.filter((key) => key.type() !== action.keyType);
    }
}

export const PinnedKeysContext = createContext<{
    keys: EntityKey[];
    dispatch: React.Dispatch<PinnedKeysAction>;
}>({
    keys: [],
    dispatch: () => {},
});

export const ColdStorageChoiceContext = createContext<{
    coldStorage: boolean;
    dispatch: React.Dispatch<boolean>;
}>({
    coldStorage: false,
    dispatch: () => {},
});
