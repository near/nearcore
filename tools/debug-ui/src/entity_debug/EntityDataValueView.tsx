import { useCallback, useContext, useState } from 'react';
import { Tooltip } from 'react-tooltip';
import { FetcherContext } from './fetcher';
import {
    EntityDataValueNode,
    EntityDataRootNode,
    EntityKey,
    EntityKeyType,
    EntityQuery,
    EntityQueryType,
    entityQueryKeyTypes,
    entityQueryOutputType,
    entityQueryTypes,
} from './types';
import { PinnedKeysContext } from './pinned_keys';
import { EntityDataRootView } from './EntityDataRootView';
import './EntityDataValueView.scss';

export type EntityDataValueViewProps = {
    entry: EntityDataValueNode;
    hideName: boolean;
};

export const EntityDataValueView = ({ entry, hideName }: EntityDataValueViewProps) => {
    const fetcher = useContext(FetcherContext);
    const [expanded, setExpanded] = useState(false);
    const [hovering, setHovering] = useState(false);
    const [, setChildrenVersion] = useState(0);
    const { keys: contextKeys, dispatch: contextKeysDispatch } = useContext(PinnedKeysContext);

    const addQuery = useCallback(
        (query: EntityQuery) => {
            if (fetcher == null) {
                return;
            }
            entry.queriedChildren.push(new EntityDataRootNode(query, fetcher.fetch(query)));
            setChildrenVersion((v) => v + 1);
        },
        [fetcher, entry]
    );

    const onMouseEnter = useCallback(() => setHovering(true), []);
    const onMouseLeave = useCallback(() => setHovering(false), []);
    const toggleExpanded = useCallback(() => setExpanded((v) => !v), []);

    let entryValue = null;
    if (typeof entry.value === 'string') {
        entryValue = <span>{entry.value}</span>;
        if (entry.semantic !== undefined && 'display' in entry.semantic) {
            const display = entry.semantic.display!;
            if (display === 'trie_path') {
                const [shard_uid, state_root, nibbles] = entry.value.split('/');
                if (nibbles) {
                    entryValue = (
                        <span className="value-trie-path">
                            <span className="shard-uid">{shard_uid}</span>
                            <span className="state-root">{state_root}</span>
                            <span className="nibbles">{visualizeNibbles(nibbles)}</span>
                        </span>
                    );
                }
            } else if (display === 'nibbles') {
                entryValue = <span className="nibbles">{visualizeNibbles(entry.value)}</span>;
            }
        }
    } else if (entry.semantic?.titleKey !== undefined) {
        const titleKey = entry.semantic.titleKey;
        const titleEntry = entry.value.entries.find((e) => e.name === titleKey);
        if (titleEntry !== undefined) {
            entryValue = <span className="value-title">{titleEntry.value as string}</span>;
        }
    }
    const header = (
        <div
            className="entity-entry-header"
            onMouseEnter={onMouseEnter}
            onMouseLeave={onMouseLeave}>
            {typeof entry.value !== 'string' && (
                <div className="entity-entry-expander" onClick={toggleExpanded}>
                    {expanded ? '-' : '+'}
                </div>
            )}
            {!(hideName && typeof entry.value === 'string') && (
                <div className="entity-entry-name">{entry.name}</div>
            )}
            <div
                className={
                    'entity-entry-value' +
                    (entry.keys.length > 0 ? ' entity-entry-value-with-keys' : '')
                }>
                {entryValue}
            </div>
            {entry.keys.map((key) => {
                const selected = contextKeys.some(
                    (contextKey) =>
                        contextKey.type() === key.type() && contextKey.toString() === key.toString()
                );
                if (selected || hovering) {
                    return (
                        <div
                            className={`entity-key-pin-button ${selected ? 'selected' : ''}`}
                            key={key.type()}
                            onClick={(e) =>
                                e.currentTarget.classList.contains('selected')
                                    ? contextKeysDispatch({
                                          type: 'remove-key',
                                          keyType: key.type(),
                                      })
                                    : contextKeysDispatch({ type: 'add-key', key })
                            }>
                            {selected ? '☑' : '☐'} {key.type()}
                        </div>
                    );
                }
                return null;
            })}
            {hovering &&
                getAvailableQueries(entry.keys, contextKeys).map(({ queryType, keys, query }) => {
                    const entityType = entityQueryOutputType[queryType];
                    return (
                        <>
                            <div
                                data-tooltip-id={`entity-query-button-${queryType}`}
                                className="entity-query-button">
                                <button
                                    key={queryType}
                                    disabled={query === null}
                                    onClick={() => addQuery(query!)}>
                                    ➤ {entityType}
                                </button>
                            </div>
                            <Tooltip id={`entity-query-button-${queryType}`} place="bottom">
                                <div className="entity-query-tooltip">
                                    <div className="entity-query-type">{queryType}</div>
                                    <div className="entity-query-keys">
                                        {keys.map(({ keyType, key }) => (
                                            <div key={keyType}>
                                                {keyType}:{' '}
                                                {key ? (
                                                    <span>{key.toString()}</span>
                                                ) : (
                                                    <span className="missing">
                                                        (missing, please pin a key)
                                                    </span>
                                                )}
                                            </div>
                                        ))}
                                    </div>
                                </div>
                            </Tooltip>
                        </>
                    );
                })}
        </div>
    );

    return (
        <div className="entity-entry">
            {header}
            {expanded && typeof entry.value !== 'string' && (
                <div className="entity-entry-children">
                    {entry.value.entries.map((entry) => (
                        <EntityDataValueView key={entry.name} entry={entry} hideName={false} />
                    ))}
                </div>
            )}
            {entry.queriedChildren.length > 0 && (
                <div className="entity-entry-queried-children">
                    {entry.queriedChildren.map((child, i) => (
                        <EntityDataRootView
                            key={i}
                            node={child}
                            removalCallback={() => {
                                entry.queriedChildren.splice(i, 1);
                                setChildrenVersion((v) => v + 1);
                            }}
                        />
                    ))}
                </div>
            )}
        </div>
    );
};

type AvailableQueryKey = {
    keyType: EntityKeyType;
    key: EntityKey | null;
};

type AvailableQuery = {
    queryType: EntityQueryType;
    keys: AvailableQueryKey[];
    query: EntityQuery | null;
};

function getAvailableQueries(keys: EntityKey[], contextKeys: EntityKey[]): AvailableQuery[] {
    const result: AvailableQuery[] = [];
    const keyTypeToKey = new Map<EntityKeyType, EntityKey>();
    const explicitKeyTypes = new Set<EntityKeyType>();
    for (const key of keys) {
        keyTypeToKey.set(key.type(), key);
        explicitKeyTypes.add(key.type());
    }
    for (const key of contextKeys) {
        if (!keyTypeToKey.has(key.type())) {
            keyTypeToKey.set(key.type(), key);
        }
    }

    for (const queryType of entityQueryTypes) {
        let explicitKeyCount = 0;
        let anyKeyNotFound = false;
        const keys = entityQueryKeyTypes[queryType].map(({ keyType, implicitOnly }) => {
            const key = keyTypeToKey.get(keyType) ?? null;
            if (explicitKeyTypes.has(keyType) && !implicitOnly) {
                explicitKeyCount++;
            }
            if (key === null) {
                anyKeyNotFound = true;
            }
            return { keyType, key };
        });
        if (explicitKeyCount > 0) {
            if (anyKeyNotFound) {
                result.push({ queryType, keys, query: null });
            } else {
                const query: EntityQuery = {};
                const args: { [_ in EntityKeyType]?: unknown } = {};
                query[queryType] = args as any;
                for (const { keyType, key } of keys) {
                    if (key) {
                        args[keyType] = key.toJSON();
                    }
                }
                result.push({ queryType, keys, query });
            }
        }
    }
    return result;
}

function visualizeNibbles(nibbles: string): JSX.Element {
    const children = [];
    for (let i = 0; i < Math.floor(nibbles.length / 2); i++) {
        const byte = parseInt(nibbles.substring(i * 2, i * 2 + 2), 16);
        if (byte >= 0x20 && byte <= 0x7e) {
            children.push(
                <span className="ascii-char" key={i}>
                    {String.fromCharCode(byte)}
                </span>
            );
        } else {
            children.push(
                <span className="hex-chars" key={i}>
                    {nibbles.substring(i * 2, i * 2 + 2)}
                </span>
            );
        }
    }
    if (nibbles.length % 2 == 1) {
        children.push(
            <span className="hex-chars" key={Math.floor(nibbles.length / 2)}>
                {nibbles.substring(nibbles.length - 1)}
            </span>
        );
    }
    return <>{children}</>;
}
