import { ReactElement } from 'react';

export function formatDurationInMillis(millis: number): string {
    if (millis == null) {
        return '(null)';
    }
    const total_seconds = Math.floor(millis / 1000);
    const hours = Math.floor(total_seconds / 3600);
    const minutes = Math.floor((total_seconds - hours * 3600) / 60);
    const seconds = total_seconds - hours * 3600 - minutes * 60;
    if (hours > 0) {
        if (minutes > 0) {
            return `${hours}h ${minutes}m ${seconds}s`;
        } else {
            return `${hours}h ${seconds}s`;
        }
    }
    if (minutes > 0) {
        return `${minutes}m ${seconds}s`;
    }
    return `${seconds}s`;
}

function formatBytesPerSecond(bytes_per_second: number): string {
    if (bytes_per_second == null) {
        return '-';
    }
    if (bytes_per_second < 3000) {
        return `${bytes_per_second} bps`;
    }
    const kilobytes_per_second = bytes_per_second / 1024;
    if (kilobytes_per_second < 3000) {
        return `${kilobytes_per_second.toFixed(1)} Kbps`;
    }
    const megabytes_per_second = kilobytes_per_second / 1024;
    return `${megabytes_per_second.toFixed(1)} Mbps`;
}

export function formatTraffic(bytes_received: number, bytes_sent: number): ReactElement {
    return (
        <div>
            <div>{'⬇ ' + formatBytesPerSecond(bytes_received)}</div>
            <div>{'⬆ ' + formatBytesPerSecond(bytes_sent)}</div>
        </div>
    );
}

export function addDebugPortLink(peer_network_addr: string): ReactElement {
    // Each node running in a machine is assigned ports 24567+peer_num and 3030+peer_num, whereby peer_num is a whole number
    // peer_rpc_address is not shared between peer nodes. Hence, it cannot be programmatically fetched.
    // https://github.com/near/nearcore/blob/700ec29270f72f2e78a17029b4799a8228926c07/chain/network/src/network_protocol/peer.rs#L13-L19
    const DEFAULT_RPC_PORT = 3030;
    const DEFAULT_NETWORK_PORT = 24567;
    const peer_network_addr_array = peer_network_addr.split(':');
    const peer_network_port = parseInt(peer_network_addr_array.pop() || '24567');
    const peer_network_ip = peer_network_addr_array.pop() || peer_network_addr;
    let peer_num = 0;
    if (peer_network_ip.includes('127.0.0.1')) {
        peer_num = peer_network_port - DEFAULT_NETWORK_PORT;
    }
    const peer_rpc_port = DEFAULT_RPC_PORT + peer_num;
    const peer_rpc_address =
        peer_network_addr.replace(/:.*/, ':') + peer_rpc_port.toString() + '/debug';
    return <a href={'http://localhost:3000/' + peer_rpc_address}>{peer_network_addr}</a>;
}

export function toHumanTime(seconds: number): string {
    let result = '';
    if (seconds >= 60) {
        let minutes = Math.floor(seconds / 60);
        seconds = seconds % 60;
        if (minutes > 60) {
            let hours = Math.floor(minutes / 60);
            minutes = minutes % 60;
            if (hours > 24) {
                const days = Math.floor(hours / 24);
                hours = hours % 24;
                result += days + ' days ';
            }
            result += hours + ' h ';
        }
        result += minutes + ' m ';
    }
    result += seconds + ' s';
    return result;
}
