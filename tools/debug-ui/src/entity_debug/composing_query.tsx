/// A react data structure for a simple query that is being composed via the UI.
/// See https://react.dev/reference/react/useReducer to understand this pattern.

import { createContext } from 'react';
import { EntityKey, EntityKeyType, EntityQueryType, entityQueryTypes } from './types';

export type ComposingQuery = {
    queryType: EntityQueryType;
    keys: EntityKey[];
};

export type ComposingQueryAction =
    | {
          type: 'set-type';
          queryType: EntityQueryType;
      }
    | {
          type: 'set-key';
          key: EntityKey;
      }
    | {
          type: 'remove-key';
          keyType: EntityKeyType;
      };

export function composingQueryReducer(
    state: ComposingQuery,
    action: ComposingQueryAction
): ComposingQuery {
    switch (action.type) {
        case 'set-type':
            return {
                ...state,
                queryType: action.queryType,
            };
        case 'set-key': {
            const keys = state.keys.filter((key) => key.type() !== action.key.type());
            return {
                ...state,
                keys: [...keys, action.key],
            };
        }
        case 'remove-key':
            return {
                ...state,
                keys: state.keys.filter((key) => key.type() !== action.keyType),
            };
    }
}

export const ComposingQueryContext = createContext<{
    composingQuery: ComposingQuery;
    dispatch: React.Dispatch<ComposingQueryAction>;
}>({
    composingQuery: {
        queryType: entityQueryTypes[0],
        keys: [],
    },
    dispatch: () => {},
});
