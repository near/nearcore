/// A react data structure for a list of all queries displayed on the UI.
/// See https://react.dev/reference/react/useReducer to understand this pattern.

import { createContext } from 'react';
import { EntityDataRootNode, EntityQuery } from './types';

export type AllQueriesAction =
    | {
          type: 'add-query';
          query: EntityQuery;
          node: EntityDataRootNode;
      }
    | {
          type: 'remove-query';
          index: number;
      }
    | {
          type: 'select-query';
          index: number;
      };

export type AllQueriesState = {
    /// parallel array of queries and results.
    queries: EntityQuery[];
    results: EntityDataRootNode[];
    selectedIndex: number; // -1 means no selection
};

export function allQueriesReducer(
    queries: AllQueriesState,
    action: AllQueriesAction
): AllQueriesState {
    switch (action.type) {
        case 'add-query':
            return {
                queries: [...queries.queries, action.query],
                results: [...queries.results, action.node],
                selectedIndex: queries.queries.length,
            };
        case 'remove-query': {
            const newSelectedIndex =
                queries.selectedIndex === action.index
                    ? -1
                    : queries.selectedIndex > action.index
                    ? queries.selectedIndex - 1
                    : queries.selectedIndex;
            return {
                queries: queries.queries.filter((_, i) => i !== action.index),
                results: queries.results.filter((_, i) => i !== action.index),
                selectedIndex: newSelectedIndex,
            };
        }
        case 'select-query':
            return {
                queries: queries.queries,
                results: queries.results,
                selectedIndex: action.index,
            };
    }
}

export const AllQueriesContext = createContext<{
    queries: EntityQuery[];
    results: EntityDataRootNode[];
    selectedIndex: number;
    dispatch: React.Dispatch<AllQueriesAction>;
}>({
    queries: [],
    results: [],
    selectedIndex: -1,
    dispatch: () => {},
});
