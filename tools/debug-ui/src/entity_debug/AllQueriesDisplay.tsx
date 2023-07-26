import { useContext } from 'react';
import { AllQueriesContext } from './all_queries';
import { getQueryType } from './types';
import './AllQueriesDisplay.scss';

export const AllQueriesDisplay = () => {
    const { queries, selectedIndex, dispatch } = useContext(AllQueriesContext);
    return (
        <div className="query-list">
            <div className="header">Queries</div>
            <div className="body">
                {queries.map((query, i) => {
                    const queryType = getQueryType(query);
                    const queryKeys: Record<string, unknown> | null | undefined = query[queryType];
                    return (
                        <div
                            key={i}
                            className={`one-query ${
                                selectedIndex === i ? 'one-query-selected' : ''
                            }`}
                            onClick={() => dispatch({ type: 'select-query', index: i })}>
                            <div className="one-query-description">
                                <div className="one-query-type">{queryType}</div>
                                {queryKeys && (
                                    <div className="one-query-keys">
                                        {Object.keys(queryKeys).map((key) => {
                                            const value = queryKeys[key];
                                            return (
                                                <div key={key} className="one-query-key">
                                                    <div className="one-query-key-name">{key}</div>
                                                    <div className="one-query-key-value">
                                                        {'' + value}
                                                    </div>
                                                </div>
                                            );
                                        })}
                                    </div>
                                )}
                            </div>
                            <div
                                className="one-query-delete"
                                onClick={(e) => {
                                    e.stopPropagation();
                                    dispatch({ type: 'remove-query', index: i });
                                }}>
                                ×
                            </div>
                        </div>
                    );
                })}
                <div
                    className={`one-query ${selectedIndex === -1 ? 'one-query-selected' : ''}`}
                    onClick={() => dispatch({ type: 'select-query', index: -1 })}>
                    + Add New Query
                </div>
            </div>
        </div>
    );
};
