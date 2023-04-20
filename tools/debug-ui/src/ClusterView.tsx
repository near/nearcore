import { useCallback, useEffect, useRef, useState } from 'react';
import { ClusterNodeView } from './ClusterNodeView';
import './ClusterView.scss';

interface Props {
    initialAddr: string;
}

function sortingKeyForNode(addr: string, addrToName: Map<string, string>): string {
    if (addrToName.has(addr)) {
        return '0' + addrToName.get(addr);
    } else {
        return '1' + addr;
    }
}

export class DiscoveryNodes {
    nodes: Set<string> = new Set();
    addrToName: Map<string, string> = new Map();

    reset(initialAddr: string) {
        this.nodes.clear();
        this.addrToName.clear();
        this.nodes.add(initialAddr);
    }

    sorted(): string[] {
        return Array.from(this.nodes).sort((a, b) => {
            return sortingKeyForNode(a, this.addrToName).localeCompare(
                sortingKeyForNode(b, this.addrToName)
            );
        });
    }
}

export const ClusterView = ({ initialAddr }: Props) => {
    const [sortedNodes, setSortedNodes] = useState<string[]>([]);
    const [highestHeight, setHighestHeight] = useState<number>(0);
    const nodes = useRef(new DiscoveryNodes());

    useEffect(() => {
        nodes.current.reset(initialAddr);
        setHighestHeight(0);
        setSortedNodes(nodes.current.sorted());
    }, [initialAddr]);

    const basicStatusCallback = useCallback((addr: string, name: string | null, height: number) => {
        setHighestHeight((prev) => Math.max(prev, height));
        if (name) {
            nodes.current.addrToName.set(addr, name);
        }
        setSortedNodes(nodes.current.sorted());
    }, []);

    const newNodesDiscoveredCallback = useCallback((newIps: string[]) => {
        for (const addr of newIps) {
            nodes.current.nodes.add(addr);
        }
        setSortedNodes(nodes.current.sorted());
    }, []);

    return (
        <div className="cluster-view">
            <div className="title">Discovered {sortedNodes.length} nodes in cluster</div>
            {sortedNodes.map((addr) => {
                return (
                    <ClusterNodeView
                        addr={addr}
                        key={addr}
                        highestHeight={highestHeight}
                        basicStatusChanged={basicStatusCallback}
                        newNodesDiscovered={newNodesDiscoveredCallback}
                    />
                );
            })}
        </div>
    );
};
