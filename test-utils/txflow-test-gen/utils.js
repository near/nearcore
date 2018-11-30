var _rand_int = function(n) {
    return parseInt(Math.random() * n);
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
    if (nodes.length == 0) return;
    seconds = seconds || 2;

    var started = new Date().getTime();
    var iter = 0;
    var longest_epoch_blocks = [];
    var longest_selected_node_ids = [];
    while (new Date().getTime() - started < seconds * 1000) {
        var selected_nodes = _select_random_nodes(nodes);
        var annotations = compute_annotations(selected_nodes, num_users);
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
                alert(`FAILED!\nLeft: nodes: ${longest_selected_node_ids}, epoch blocks: ${longest_epoch_blocks}\nRight: nodes: ${selected_node_ids}, epoch blocks: ${epoch_blocks}`);
                return;
            }
        }

        if (epoch_blocks.length > longest_epoch_blocks.length) {
            longest_epoch_blocks = epoch_blocks;
            longest_selected_node_ids = selected_node_ids;
        }

        ++ iter;
    }

    alert(`PASSED!\nRan stress for ${seconds} seconds. Completed ${iter} iterations.\nLongest epoch blocks: ${longest_epoch_blocks}`);
}

var generate_random_graph = function(num_users, num_malicious, num_nodes) {
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

        var node = create_node(user_id, parents);
        if (mode_prefer_non_repr && _rand_int(3) != 0) {
            var annotations = compute_annotations([node], num_users)
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

    refresh();
    serialize();

    stress_epoch_blocks(all_nodes.map(x => x[0]), num_users, 0.5);
}

