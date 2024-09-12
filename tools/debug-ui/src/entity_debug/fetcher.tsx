import { createContext } from 'react';
import { ApiEntityDataEntry, fetchEntity } from '../api';
import {
    EntityDataValueNode,
    EntityDataStructNode,
    EntityQuery,
    FieldSemantic,
    entityQueryOutputType,
    EntityQueryWithParams,
} from './types';
import { fieldSemantics } from './fields';

/// A simple class to fetch entity data for a query.
export class Fetcher {
    constructor(private addr: string) {}

    async fetch(query: EntityQuery, useColdStorage: boolean): Promise<EntityDataValueNode> {
        const queryWithParams: EntityQueryWithParams = { ...query };
        if (useColdStorage) {
            // For compatibility with older node builds, do not include this field if
            // it is false.
            queryWithParams.use_cold_storage = true;
        }
        const result = await fetchEntity(this.addr, queryWithParams);
        const queryType: keyof EntityQuery = Object.keys(query)[0] as keyof EntityQuery;
        const entityType = entityQueryOutputType[queryType];
        const rootEntry = this._parseApiEntityDataValue(fieldSemantics[entityType], {
            name: '_root',
            value: result,
        });
        return rootEntry;
    }

    /// Parses the API EntityDataValue response into our representation of it.
    _parseApiEntityDataValue(
        fieldSemantic: FieldSemantic,
        entry: ApiEntityDataEntry
    ): EntityDataValueNode {
        if (typeof entry.value === 'string') {
            return new EntityDataValueNode(fieldSemantic, entry.name, entry.value);
        }
        const data = new EntityDataStructNode();
        for (const childEntry of entry.value.entries) {
            const isIndex = /^\d+$/.test(childEntry.name);
            let childSemantic;
            if (isIndex) {
                childSemantic =
                    fieldSemantic !== undefined && 'array' in fieldSemantic
                        ? fieldSemantic.array
                        : undefined;
            } else {
                childSemantic =
                    fieldSemantic !== undefined && 'struct' in fieldSemantic
                        ? fieldSemantic.struct![childEntry.name]
                        : undefined;
            }
            data.entries.push(this._parseApiEntityDataValue(childSemantic, childEntry));
        }
        return new EntityDataValueNode(fieldSemantic, entry.name, data);
    }
}

export const FetcherContext = createContext<Fetcher | null>(null);
