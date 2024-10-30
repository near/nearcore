import { useCallback, useContext, useReducer } from 'react';
import {
    EntityDataRootNode,
    EntityKeyType,
    EntityQuery,
    entityQueryKeyTypes,
    entityQueryTypes,
} from './types';
import { ComposingQueryContext, composingQueryReducer } from './composing_query';
import { KeyInput } from './KeyInput';
import { AllQueriesContext } from './all_queries';
import { FetcherContext } from './fetcher';
import './EntityQueryComposer.scss';
import { ColdStorageChoiceContext, PinnedKeysContext } from './pinned_keys';

export const EntityQueryComposer = () => {
    const { keys: pinnedKeys } = useContext(PinnedKeysContext);
    const { coldStorage } = useContext(ColdStorageChoiceContext);
    const [query, queryDispatch] = useReducer(composingQueryReducer, {
        queryType: entityQueryTypes[0],
        keys: pinnedKeys,
    });
    const { dispatch: allQueriesDispatch } = useContext(AllQueriesContext);
    const fetcher = useContext(FetcherContext);

    const constructedQuery: EntityQuery | null = (() => {
        if (entityQueryKeyTypes[query.queryType].length == 0) {
            return { [query.queryType]: null };
        }
        const args: { [_ in EntityKeyType]?: unknown } = {};
        for (const { keyType } of entityQueryKeyTypes[query.queryType]) {
            const key = query.keys.find((key) => key.type() === keyType);
            if (!key) {
                return null;
            }
            args[keyType] = key.toJSON();
        }
        return {
            [query.queryType]: args as any,
        };
    })();

    const submitQuery = useCallback(() => {
        if (constructedQuery && fetcher) {
            allQueriesDispatch({
                type: 'add-query',
                query: constructedQuery,
                node: new EntityDataRootNode(
                    constructedQuery,
                    coldStorage,
                    fetcher.fetch(constructedQuery, coldStorage)
                ),
            });
        }
    }, [constructedQuery, fetcher, allQueriesDispatch, coldStorage]);

    const selectionList = (
        <div className="query-type-list">
            <div className="list-header">
                <span>All Query Types</span>
            </div>
            <div className="list-body">
                {entityQueryTypes.map((queryType) => {
                    return (
                        <div
                            key={queryType}
                            className={`list-item ${
                                queryType == query.queryType ? 'list-item-selected' : ''
                            }`}
                            onClick={() => queryDispatch({ type: 'set-type', queryType })}>
                            <span>{queryType}</span>
                        </div>
                    );
                })}
            </div>
        </div>
    );
    const composer = (
        <div className="query-composer">
            <div className="query-composer-header">
                <span>{query.queryType}</span>
            </div>
            <div className="query-composer-keys">
                {entityQueryKeyTypes[query.queryType].map(({ keyType }) => (
                    <KeyInput key={keyType} keyType={keyType} />
                ))}
            </div>
            <div className="query-composer-confirm">
                <button onClick={submitQuery} disabled={constructedQuery === null}>
                    Add Query
                </button>
            </div>
        </div>
    );
    return (
        <ComposingQueryContext.Provider value={{ composingQuery: query, dispatch: queryDispatch }}>
            <div className="query-composer-container">
                {selectionList}
                {composer}
            </div>
        </ComposingQueryContext.Provider>
    );
};
