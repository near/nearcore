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
    // Assume rpc port is always 3030 and network port is always 24567 for the first neard process on a machine
    // Then, each subsequence neard process on the same machine will are strict increments of a fix constant from both ports
    // peer_num should only be > 0 for nearup's localnet and 0 otherwise
    // Reference to nearup's localnet port assumptions
    // https://github.com/near/nearup/blob/0b9a7b60236f3164dd32677b6fa58c531a586200/nearuplib/localnet.py#L93-L94
    // Reference to mainnet's assumption of network port
    // https://github.com/near/nearcore/blob/e61da3d26e3614d3f6c1b669d31fca7a12d55296/chain/network/src/raw/connection.rs#L153
    const rpc_port_assumption = 3030;
    const network_port_assumption = 24567;
    const peer_network_port = parseInt(peer_network_addr.split(':').pop() || '24567');
    const peer_num = peer_network_port - network_port_assumption;
    const peer_rpc_port = rpc_port_assumption + peer_num;
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
