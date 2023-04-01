function convertTime(millis) {
    if (millis == null) {
        return '(null)';
    }
    let total_seconds = Math.floor(millis / 1000);
    let hours = Math.floor(total_seconds / 3600)
    let minutes = Math.floor((total_seconds - (hours * 3600)) / 60)
    let seconds = total_seconds - (hours * 3600) - (minutes * 60)
    if (hours > 0) {
        if (minutes > 0) {
            return `${hours}h ${minutes}m ${seconds}s`
        } else {
            return `${hours}h ${seconds}s`
        }
    }
    if (minutes > 0) {
        return `${minutes}m ${seconds}s`
    }
    return `${seconds}s`
}

function convertBps(bytes_per_second) {
    if (bytes_per_second == null) {
        return '-'
    }
    if (bytes_per_second < 3000) {
        return `${bytes_per_second} bps`
    } []
    let kilobytes_per_second = bytes_per_second / 1024;
    if (kilobytes_per_second < 3000) {
        return `${kilobytes_per_second.toFixed(1)} Kbps`
    }
    let megabytes_per_second = kilobytes_per_second / 1024;
    return `${megabytes_per_second.toFixed(1)} Mbps`
}

function computeTraffic(bytes_received, bytes_sent) {
    return "⬇ " + convertBps(bytes_received) + "<br>⬆ " + convertBps(bytes_sent);
}

function add_debug_port_link(peer_network_addr) {
    // Assume rpc port is always 3030 and network port is always 24567 for the first neard process on a machine
    // Then, each subsequence neard process on the same machine will are strict increments of a fix constant from both ports
    // peer_num should only be > 0 for nearup's localnet and 0 otherwise
    // Reference to nearup's localnet port assumptions
    // https://github.com/near/nearup/blob/0b9a7b60236f3164dd32677b6fa58c531a586200/nearuplib/localnet.py#L93-L94
    // Reference to mainnet's assumption of network port
    // https://github.com/near/nearcore/blob/e61da3d26e3614d3f6c1b669d31fca7a12d55296/chain/network/src/raw/connection.rs#L153
    rpc_port_assumption = 3030
    network_port_assumption = 24567
    peer_network_port = peer_network_addr.split(":").pop()
    peer_num = peer_network_port - network_port_assumption
    peer_rpc_port = rpc_port_assumption + peer_num;
    peer_rpc_address = "http://" + peer_network_addr.replace(/:.*/, ":") + peer_rpc_port + "/debug"
    return $('<a>', {
        href: peer_rpc_address,
        text: peer_network_addr
    });
}

function displayHash(peer) {
    if (peer.is_highest_block_invalid) {
        return peer.block_hash + " (INVALID)"
    } else {
        return peer.block_hash + " (Valid)"
    }
}

function peerClass(current_height, peer_height) {
    if (peer_height > current_height + 5) {
        return 'peer_ahead_alot';
    }
    if (peer_height > current_height + 2) {
        return 'peer_ahead';
    }

    if (peer_height < current_height - 100) {
        return 'peer_far_behind';
    }
    if (peer_height < current_height - 10) {
        return 'peer_behind';
    }
    if (peer_height < current_height - 3) {
        return 'peer_behind_a_little';
    }
    return 'peer_in_sync';
}

function getIntersection(setA, setB) {
    const intersection = new Set(
        [...setA].filter(element => setB.has(element))
    );

    return intersection;
}

function getDifference(setA, setB) {
    return new Set(
        [...setA].filter(element => !setB.has(element))
    );
}

function to_human_time(seconds) {
    let result = "";
    if (seconds >= 60) {
        let minutes = Math.floor(seconds / 60);
        seconds = seconds % 60;
        if (minutes > 60) {
            let hours = Math.floor(minutes / 60);
            minutes = minutes % 60;
            if (hours > 24) {
                let days = Math.floor(hours / 24);
                hours = hours % 24;
                result += days + " days ";
            }
            result += hours + " h ";
        }
        result += minutes + " m ";
    }
    result += seconds + " s"
    return result;
}
