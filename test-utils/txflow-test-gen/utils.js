var txflow = require('./txflow.js');

var _rand_int = function(n) {
    return Math.floor(Math.random() * n);
}

var get_only_selected_node = function(nodes) {
    var ret = null;

    for (var node of nodes) {
        if (node.selected) {
            if (ret) return null;
            ret = node;
        }
    }

    return ret;
}


var _select_random_nodes = function(nodes) {
    if (nodes.length == 0) return [];

    if (_rand_int(2) == 0) {
        var ret = [];
        var prob = _rand_int(10);
        for (var i = 0; i < nodes.length; ++ i) {
            if (_rand_int(prob) == 0) {
                ret.push(nodes[i]);
            }
        }

        if (ret.length) return ret;
        return _select_random_nodes(nodes);
    }
    else {
        return [nodes[_rand_int(nodes.length)]];
    }
}

var stress_epoch_blocks = function(nodes, num_users, seconds) {
    if (nodes.length == 0) return "No nodes";
    seconds = seconds || 2;

    var started = new Date().getTime();
    var iter = 0;
    var longest_epoch_blocks = [];
    var longest_selected_node_ids = [];
    while (new Date().getTime() - started < seconds * 1000) {
        var selected_nodes = _select_random_nodes(nodes);
        var annotations = txflow.compute_annotations(selected_nodes, num_users);
        var epoch_blocks = [];
        var selected_node_ids = selected_nodes.map(x => x.uid);

        for (var a_idx = 0; a_idx < annotations.length; ++ a_idx) {
            var a = annotations[a_idx];
            if (a.epoch_block) {
                epoch_blocks.push([a.representative, 'v' + a.node.uid]);
            }
        }

        epoch_blocks.sort((x, y) => x[0] < y[0] ? -1 : x[0] > y[0] ? 1 : 0);

        var smaller = Math.min(longest_epoch_blocks.length, epoch_blocks.length);

        for (var i = 0; i < smaller; ++ i) {
            var left = longest_epoch_blocks[i];
            var right = epoch_blocks[i];

            if (left[0] != right[0] || left[1] != right[1]) {
                return `FAILED!\nLeft: nodes: ${longest_selected_node_ids}, epoch blocks: ${longest_epoch_blocks}\nRight: nodes: ${selected_node_ids}, epoch blocks: ${epoch_blocks}`;
            }
        }

        if (epoch_blocks.length > longest_epoch_blocks.length) {
            longest_epoch_blocks = epoch_blocks;
            longest_selected_node_ids = selected_node_ids;
        }

        ++ iter;
    }

    return `PASSED!\nRan stress for ${seconds} seconds. Completed ${iter} iterations.\nLongest epoch blocks: ${longest_epoch_blocks}`;
}

var stress_beacon = function(nodes, num_users, seconds) {
    seconds = seconds || 2;
    var started = new Date().getTime();
    var iter = 0;
    var existing_report = null;
    var existing_reporter = -1;
    while (new Date().getTime() - started < seconds * 1000) {
        var selected_node = nodes[_rand_int(nodes.length)];
        var beacon_chain_observation = compute_beacon_chain_observation(selected_node, num_users);

        var new_report = false;

        for (var key in beacon_chain_observation) {
            if (existing_report === null || existing_report[key] === undefined) {
                new_report = true;
            }
            else {
                if (existing_report[key] != beacon_chain_observation[key]) {
                    return `FAILED\nLeft node: ${existing_reporter}, Right node: ${selected_node.uid}\nLeft report: ${existing_report}\nRight report: ${beacon_chain_observation}`;
                }
            }
        }

        if (new_report) {
            existing_report = beacon_chain_observation;
            existing_reporter = selected_node.uid;
        }
        
        ++ iter;
    }
    return `PASSED!\nRan stress for ${seconds} seconds. Completed ${iter} iterations.\nLargest report: ${JSON.stringify(existing_report)}`;
}

var generate_random_graph = function(num_users, num_malicious, num_nodes, beacon_mode, show_html=true) {
    last_node_uid = 0;

    var mode_users_hanging = _rand_int(2);
    var mode_prefer_non_repr = _rand_int(2);

    var all_nodes = [];
    var nodes_as_seen = [];
    var users_hanging = [];
    var _random_user = function() {
        if (mode_users_hanging) {
            for (var i = 0; i < num_users; ++ i) {
                if (!users_hanging[i] && _rand_int(latencies[i] * 2) == 0) users_hanging[i] = true;
                if (users_hanging[i] && _rand_int(latencies[i] * 3) == 0) users_hanging[i] = false;
            }
        }
        var ret = _rand_int(num_users);
        if (users_hanging[ret]) return _random_user();
        return ret;
    }

    var _get_roots = function(nodes_with_ts, ts) {
        var mark = {};
        for (var pair of nodes_with_ts) {
            if (pair[1] <= ts) {
                for (var parent_ of pair[0].parents) mark[parent_.uid] = 1;
            }
        }
        var ret = [];
        for (var pair of nodes_with_ts) {
            if (pair[1] <= ts) {
                if (mark[pair[0].uid] === undefined) ret.push(pair[0]);
            }
        }
        return ret;
    }
    var _get_parents = function(user_id, ts) {
        if (user_id < num_malicious && _rand_int(3) == 0) {
            return _select_random_nodes(nodes_as_seen[user_id].map(x => x[0]));
        }
        else {
            return _get_roots(nodes_as_seen[user_id], ts);
        }
    }

    var latencies = [];
    var max_latency = Math.max(_rand_int(num_users * 5), _rand_int(num_users * 3));

    for (var i = 0; i < num_users; ++ i) {
        latencies.push(Math.max(_rand_int(max_latency + 1), _rand_int(max_latency + 1)));
        nodes_as_seen.push([]);
        users_hanging.push(false);
    }

    for (var i = 0; i < num_nodes; ++ i) {
        var user_id = _random_user();
        var parents = _get_parents(user_id, i);

        var node = txflow.create_node(user_id, parents);
        if (mode_prefer_non_repr && _rand_int(3) != 0) {
            var annotations = txflow.compute_annotations([node], num_users)
            var skip = false;
            for (var a of annotations) {
                if (a.node.uid == node.uid && a.representative != -1) {
                    skip = true;
                }
            }
            if (skip) { --i; continue; }
        }
        all_nodes.push([node, 0]);
        for (var j = 0; j < num_users; ++ j) {
            var latency = Math.max(_rand_int(latencies[j] + 1), _rand_int(latencies[j] + 1));
            if (j == user_id) latency = 0;
            nodes_as_seen[j].push([node, i + latency]);
        }
    }

    graph = _get_roots(all_nodes, 0);
    for (var node of graph) node.selected = true;

    if (beacon_mode) {
        var changed = true;
        var iters = 0;
        while (changed) {
            changed = false;
            ++ iters;
            if (iters >= 30) {
                console.error("Beacon mode randgen entered an infinite loop");
                break;
            }

            var beacon_states = [];
            for (var i = 0; i < num_users; ++ i) {
                beacon_states.push(-1);
            }

            var annotations = txflow.compute_annotations(graph, num_users)
            var toposorted = txflow.toposort(graph);
            add_beacon_annotations(toposorted, annotations, num_users);

            for (var a of annotations) {
                var node = a.node;
                if (a.state > beacon_states[node.owner]) {
                    var old_payload = node.payload_type || PAYLOAD_NONE;
                    beacon_states[node.owner] = a.state;
                    if (a.state == STATE_INIT) {
                        node.payload_type = PAYLOAD_PREVBLOCK;
                        node.prevblock = _rand_int(10);
                    }
                    else if (a.state == STATE_PREVDF) {
                        if (_rand_int(3) == 1) {
                            node.payload_type = PAYLOAD_VDF;
                        }
                        else {
                            -- beacon_states[node.owner];
                        }
                    }
                    else if (a.state == STATE_COMMIT) {
                        node.payload_type = PAYLOAD_COMMIT;
                    }
                    else if (a.state == STATE_SIGNATURE) {
                        node.payload_type = PAYLOAD_SIGNATURE;
                    }
                    var new_payload = node.payload_type || PAYLOAD_NONE;
                    if (new_payload != old_payload) {
                        changed = true;
                    }
                }
            }
        }
    }
    if (show_html) {
        refresh();
        serialize();
        alert(stress_epoch_blocks(all_nodes.map(x => x[0]), num_users, 0.5));
    }
    return graph;
}

if (typeof module !== 'undefined') {
    module.exports.generate_random_graph = generate_random_graph;
    module.exports.stress_epoch_blocks = stress_epoch_blocks;
}
