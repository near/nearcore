import { useCallback, useContext, useEffect, useState } from 'react';
import { AllQueriesContext } from './all_queries';
import './EntityDataRootView.scss';
import { EntityDataValueView } from './EntityDataValueView';
import { FetcherContext } from './fetcher';
import {
    EntityDataRootNode,
    EntityDataValueNode,
    entityQueryOutputType,
    getQueryType,
} from './types';

export type EntityDataRootViewProps = {
    node: EntityDataRootNode;
    removalCallback: (() => void) | null;
};

export const EntityDataRootView = ({ node, removalCallback }: EntityDataRootViewProps) => {
    const [entry, setEntry] = useState<EntityDataValueNode | null>(null);
    const [error, setError] = useState<string | null>(null);
    const [, setVersion] = useState(0);
    const [isCollapsed, setIsCollapsed] = useState(true);
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
        const errorText = message ? message[1] : error;
        
        // Get first line for collapsed view
        const firstLine = errorText.split('\\n')[0];
        const hasMultipleLines = errorText.includes('\\n');
        
        // Convert literal \n strings to actual line breaks
        const formatErrorText = (text: string) => {
            const lines = text.split('\\n');
            return lines.map((line, index) => (
                <span key={index}>
                    {line}
                    {index < lines.length - 1 && <br />}
                </span>
            ));
        };
        
        content = (
            <div className={`error ${isCollapsed ? 'compact' : ''}`}>
                {hasMultipleLines && (
                    <span 
                        className="error-toggle" 
                        onClick={() => setIsCollapsed(!isCollapsed)}
                        style={{ cursor: 'pointer', marginRight: '4px' }}
                    >
                        {isCollapsed ? '▶' : '▼'}
                    </span>
                )}
                <div>
                    {isCollapsed ? firstLine : formatErrorText(errorText)}
                </div>
                {isCollapsed && hasMultipleLines && (
                    <div className="error-details" style={{ fontSize: '0.7em', marginTop: '2px', opacity: 0.7 }}>
                        Click to expand full error
                    </div>
                )}
            </div>
        );
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
