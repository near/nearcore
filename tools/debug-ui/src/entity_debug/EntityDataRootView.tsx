import { useCallback, useContext, useEffect, useState } from 'react';
import {
    EntityDataValueNode,
    EntityDataRootNode,
    entityQueryOutputType,
    getQueryType,
} from './types';
import { FetcherContext } from './fetcher';
import { AllQueriesContext } from './all_queries';
import './EntityDataRootView.scss';
import { EntityDataValueView } from './EntityDataValueView';

export type EntityDataRootViewProps = {
    node: EntityDataRootNode;
    removalCallback: (() => void) | null;
};

export const EntityDataRootView = ({ node, removalCallback }: EntityDataRootViewProps) => {
    const [entry, setEntry] = useState<EntityDataValueNode | null>(null);
    const [error, setError] = useState<string | null>(null);
    const [, setVersion] = useState(0);
    useEffect(() => {
        node.entry.then(
            (result) => {
                setEntry(result);
            },
            (err) => {
                setError('' + err);
            }
        );
        return () => {
            setEntry(null);
            setError(null);
        };
    }, [node.entry]);
    const { dispatch: allQueriesDispatch } = useContext(AllQueriesContext);
    const fetcher = useContext(FetcherContext);
    const extractCallback = useCallback(() => {
        allQueriesDispatch({ type: 'add-query', query: node.query, node: node });
        removalCallback!();
    }, [allQueriesDispatch, node, removalCallback]);
    const refreshCallback = useCallback(() => {
        node.entry = fetcher!.fetch(node.query, node.useColdStorage);
        setVersion((v) => v + 1);
    }, [node, fetcher]);
    let content: JSX.Element;
    if (error) {
        const message = error.match(/String\("([^"]*)"\)/);
        content = <div className="error">{message ? message[1] : error}</div>;
    } else if (entry === null) {
        content = <div>Loading...</div>;
    } else {
        content = (
            <div className="entity-entry-children-at-root">
                {typeof entry.value === 'string' ? (
                    <EntityDataValueView entry={entry} hideName={true} />
                ) : (
                    entry.value.entries.map((entry) => (
                        <EntityDataValueView key={entry.name} entry={entry} hideName={false} />
                    ))
                )}
            </div>
        );
    }
    return (
        <div className="entity-root">
            <div className="entity-root-header">
                <div className="entity-type">{entityQueryOutputType[getQueryType(node.query)]}</div>
                {removalCallback && (
                    <div className="entity-root-button" title="Remove" onClick={removalCallback}>
                        ✕
                    </div>
                )}
                {removalCallback && (
                    <div
                        className="entity-root-button"
                        title="Move to top-level query"
                        onClick={extractCallback}>
                        ⧉
                    </div>
                )}
                <div className="entity-root-button" title="Refresh" onClick={refreshCallback}>
                    ⟳
                </div>
            </div>
            {content}
        </div>
    );
};
