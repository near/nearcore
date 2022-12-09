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

function add_debug_port_link(peer_addr) {
    return $('<a>', {
        href: "http://" + peer_addr.replace(/:.*/, ":3030/debug"),
        text: peer_addr
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
