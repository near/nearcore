// Any functions that has an argument `nodes` expects nodes to be toposorted

var last_node_uid = 0;
var NODE_W = 72;
var NODE_H = 40;
var NODE_S = 16;
var NODE_US = 32;

var create_node = function(owner, parents) {
    var node = {};
    node.uid = last_node_uid;
    ++ last_node_uid;

    node.owner = owner;
    node.parents = parents;
    node.children = [];

    for (var parent_idx = 0; parent_idx < parents.length; ++ parent_idx) {
        parents[parent_idx].children.push(node);
    }

    return node;
}

var toposort = function(graph) {
    var ret = [];

    var q = [];
    var npr = {};

    var all_nodes = [];
    var visited = {};

    var dfs = function(node) {
        if (visited[node.uid] !== undefined) {
            return;
        }
        visited[node.uid] = true;

        for (var parent_idx = 0; parent_idx < node.parents.length; ++ parent_idx) {
            dfs(node.parents[parent_idx]);
        }

        all_nodes.push(node);
        npr[node.uid] = node.parents.length;
    };

    for (var i = 0; i < graph.length; ++ i) {
        dfs(graph[i]);
    }

    for (var i = 0; i < all_nodes.length; ++ i) {
        if (npr[all_nodes[i].uid] == 0) {
            q.push(all_nodes[i]);
        }
    }

    for (var i = 0; i < q.length; ++ i) {
        var node = q[i];
        for (var ch_idx = 0; ch_idx < node.children.length; ++ ch_idx) {
            var child = node.children[ch_idx];
            npr[child.uid] --;
            if (npr[child.uid] == 0) {
                q.push(child);
            }
        }
    }

    if (q.length != all_nodes.length) {
        console.error(all_nodes);
        console.error("q.length = " + q.length + ", all_nodes.length = " + all_nodes.length);
    }

    return q;
}

var annotate_user_levels = function(nodes) {
    var ret = {};
    for (var node_idx = 0; node_idx < nodes.length; ++ node_idx) {
        var fixed_node = nodes[node_idx];
        var user_id = fixed_node.owner;
        fixed_node.owner_level = 0;
        var visited = {};
        var dfs = function(node) {
            if (visited[node.uid] !== undefined) {
                return;
            }
            visited[node.uid] = true;

            for (var idx = 0; idx < node.parents.length; ++ idx) {
                dfs(node.parents[idx]);
            }

            if (node != fixed_node && node.owner == fixed_node.owner) {
                if (node.owner_level + 1 > fixed_node.owner_level) {
                    fixed_node.owner_level = node.owner_level + 1;
                }
            }
        }
        dfs(fixed_node);
        if (ret[user_id] === undefined) {
            ret[user_id] = [];
        }
        if (fixed_node.owner_level >= ret[user_id].length) {
            ret[user_id].push([]);
        }
        fixed_node.owner_level_nodes = ret[user_id][fixed_node.owner_level];
        fixed_node.owner_level_ord = ret[user_id][fixed_node.owner_level].length;
        ret[user_id][fixed_node.owner_level].push(fixed_node);
    }
    
    return ret;
}

var render_nodes_cb = function(nodes, user_l, cb) {
    for (var node_idx = 0; node_idx < nodes.length; ++ node_idx) {
        var node = nodes[node_idx];
        var w = NODE_W;
        var h = NODE_H;
        var l = user_l[node.owner] + (NODE_W + NODE_S) * node.owner_level_ord;
        var t = (NODE_H + NODE_S) * node.level;

        node.coords = {'l': l, 't': t, 'w': w, 'h': h};

        cb(node, l, t, w, h);
    }
}

var compute_annotations = function(graph, num_users) {
    var nodes = toposort(graph);
    annotate_user_levels(nodes);
    var annotations = [];
    var mapping = {};

    // This list contains objects of form [epoch, node] to enable easy sorting
    var epoch_blocks_endorsed = [];

    for (var node_idx = 0; node_idx < nodes.length; ++ node_idx) {
        var node = nodes[node_idx];
        var owner = node.owner;
        var a = {'node' : node, 'endorsements': {}, 'largest_kickout_promise': -1};

        annotations.push(a);
        mapping[node.uid] = a;

        var largest_epoch = [];
        var largest_kickout = [];
        for (var i = 0; i < num_users; ++ i) {
            largest_epoch[i] = -1;
            largest_kickout[i] = -1;
        }
        var largest_representative = -1;
        var largest_previous_kickout_promise = -1;

        var visited = {};
        var dfs = function(node) {
            if (visited[node.uid] !== undefined) {
                return;
            }
            visited[node.uid] = 1;

            for (var parent_idx = 0; parent_idx < node.parents.length; ++ parent_idx) {
                var parent_ = node.parents[parent_idx];
                dfs(parent_);

                if (largest_kickout[parent_.owner] > largest_kickout[node.owner]) {
                    largest_kickout[node.owner] = largest_kickout[parent_.owner];
                }
            }

            var a = mapping[node.uid];
            if (a.epoch > largest_epoch[node.owner]) {
                largest_epoch[node.owner] = a.epoch;
            }

            if (a.representative > largest_representative) {
                largest_representative = a.representative;
            }

            if (a.kickout) {
                if (a.epoch > largest_kickout[node.owner]) {
                    largest_kickout[node.owner] = a.epoch;
                }
            }

            if (node.owner == owner && a.largest_kickout_promise > largest_previous_kickout_promise) {
                largest_previous_kickout_promise = a.largest_kickout_promise;
            }
        }
        dfs(node);
        
        var same_epoch = 0;
        for (var i = 0; i < num_users; ++ i) {
            if (largest_epoch[i] >= largest_epoch[node.owner]) {
                ++ same_epoch;
            }
        }

        a.epoch = largest_epoch[node.owner];
        a.representative = -1;
        a.kickout = false;
        if (same_epoch > Math.floor(num_users * 2 / 3)) {
            a.epoch ++;
            if (a.epoch % num_users == node.owner) {
                if (largest_representative == a.epoch - 1 || a.epoch == 0) {
                    a.representative = a.epoch;
                }
                else if (largest_representative < a.epoch) {
                    a.kickout = true;

                    if (a.epoch > largest_kickout[owner]) {
                        largest_kickout[owner] = a.epoch;
                    }
                }
            }
        }

        if (largest_representative < largest_kickout[node.owner]) {
            // There are some kickout votes happening. Let's see if one
            //    of them is for the owner
            // TODO: consider the case when a full cycle of validators rotate
            //    without producing a single block
            var my_skipped_epoch = -1;
            for (var r = largest_representative + 1; r <= largest_kickout[node.owner]; ++ r) {
                if (r % num_users == node.owner && a.epoch >= r) {
                    // node.epoch >= r means the owner definitely posted
                    //    the kickout message
                    my_skipped_epoch = r;
                    break;
                }
            }
        
            if (my_skipped_epoch > -1) {
                // the user has posted the kickout, no representative was published since then
                //     the epoch increases if either 2/3 of participants approved the kickout message,
                //     or the previous representative is reachable
                if (largest_representative == my_skipped_epoch - 1) {
                    a.representative = my_skipped_epoch;
                }
                else {
                    total_kickouts = 0;
                    for (var i = 0; i < num_users; ++ i) {
                        if (largest_kickout[i] >= my_skipped_epoch) {
                            // approving a kickout for an epoch indicates
                            //     an intent to kickout all previous msgs too
                            ++ total_kickouts;
                        }
                    }
                    if (total_kickouts > Math.floor(num_users * 2 / 3)) {
                        a.representative = my_skipped_epoch;
                    }
                }
            }
        }

        var dfs_endorse = function(node) {
            if (visited[node.uid] !== 1) {
                return;
            }
            visited[node.uid] = 2;

            for (var parent_idx = 0; parent_idx < node.parents.length; ++ parent_idx) {
                var parent_ = node.parents[parent_idx];
                dfs_endorse(parent_);
            }

            if (mapping[node.uid].representative > -1) {
                var ok = largest_previous_kickout_promise <= mapping[node.uid].representative;
                for (var idx = 0; idx < node.owner_level_nodes.length; ++ idx ) {
                    var conflict_node = node.owner_level_nodes[idx];
                    if (conflict_node.uid != node.uid && visited[conflict_node.uid]) {
                        ok = false;
                    }
                }

                if (ok) {
                    var num_endorsements = 0;
                    for (var endorser in mapping[node.uid].endorsements) {
                        ++ num_endorsements;
                    }
                    var was_epoch_block = num_endorsements > Math.floor(num_users * 2 / 3);

                    if (mapping[node.uid].endorsements[owner] === undefined) {
                        ++ num_endorsements;
                    }
                    var is_epoch_block = num_endorsements > Math.floor(num_users * 2 / 3);

                    if (is_epoch_block && !was_epoch_block) {
                        epoch_blocks_endorsed.push([mapping[node.uid].representative, node]);
                    }

                    mapping[node.uid].endorsements[owner] = true;
                }
            }
        }
        dfs_endorse(node);

        a.largest_kickout_promise = largest_kickout[owner];
    }

    var last_epoch_block = -1;
    epoch_blocks_endorsed.sort((x, y) => x[0] < y[0] ? -1 : x[0] > y[0] ? 1 : 0);
    for (var i = 0; i < epoch_blocks_endorsed.length; ++ i) {
        var epoch = epoch_blocks_endorsed[i][0];
        var node = epoch_blocks_endorsed[i][1];
        var a = mapping[node.uid];

        a.epoch_block = true;

        var visited = {};
        var dfs_epoch = function(node) {
            var ret = [-1, null]
            if (visited[node.uid] !== undefined) {
                return visited[node.uid];
            }

            for (var parent_idx = 0; parent_idx < node.parents.length; ++ parent_idx) {
                var parent_ = node.parents[parent_idx];
                var cand = dfs_epoch(parent_);
                if (cand[0] > ret[0]) ret = cand;
            }
            
            if (mapping[node.uid].representative > last_epoch_block && !mapping[node.uid].epoch_block) {
                if (mapping[node.uid].representative > ret[0]) {
                    ret = [mapping[node.uid].representative, node];
                }
            }

            visited[node.uid] = ret;
            return ret;
        }

        var dfs_from = node;
        var iter = 0;
        while (dfs_from) {
            mapping[dfs_from.uid].epoch_block = true;
            var mp = dfs_epoch(dfs_from);
            visited = {};
            dfs_from = mp[1];
            ++ iter;
            if (iter > 1000) {
                console.error("Annotating epochs blocks entered an infinite loop?");
                break;
            }
        }

        last_epoch_block = epoch;
    }

    return annotations;
}

var render_graph = function(canvas, graph, num_users) {
    var ctx = canvas.getContext('2d');

    var nodes = toposort(graph);
    annotate_user_levels(nodes);

    var user_w = {};
    var user_l = {};

    for (var i = 0; i < num_users; ++ i) { user_w[i] = 1; }

    for (var node_idx = 0; node_idx < nodes.length; ++ node_idx) {
        var node = nodes[node_idx];

        if (user_w[node.owner] < node.owner_level_nodes.length) {
            user_w[node.owner] = node.owner_level_nodes.length;
        }
        
        node.level = 0;
        for (var cg_idx = 0; cg_idx < node.owner_level_nodes.length; ++ cg_idx) {
            var cg_node = node.owner_level_nodes[cg_idx];
            for (var cg_parent_idx = 0; cg_parent_idx < cg_node.parents.length; ++ cg_parent_idx) {
                var cg_parent = cg_node.parents[cg_parent_idx];
                if (cg_parent.level + 1 > node.level) {
                    node.level = cg_parent.level + 1;
                }
            }
        }
    }

    user_l[0] = 0;
    for (var i = 1; i <= num_users; ++ i) { user_l[i] = user_l[i - 1] + user_w[i - 1] * (NODE_W + NODE_S) + NODE_US; }

    var W = 0;
    var H = 0;
    render_nodes_cb(nodes, user_l, function(node, x, y, w, h) { if (x + w > W) W = x + w; if (y + h > H) H = y + h; } );

    W = user_l[num_users];

    canvas.style.width = W + 10 + "px";
    canvas.style.height = H + 200 + "px";
    canvas.width = PIXEL_RATIO * (W + 10);
    canvas.height = PIXEL_RATIO * (H + 200);

	ctx.setTransform(PIXEL_RATIO, 0, 0, PIXEL_RATIO, 0, 0);

    ctx.clearRect(0, 0, W, H);

    ctx.strokeStyle = "black";

    render_nodes_cb(nodes, user_l, function(node, x, y, w, h) {
        ctx.fillStyle = (node.selected) ? "silver" : "white";
        ctx.fillRect(x + 0.5, y + 0.5, w, h);
        ctx.strokeRect(x + 0.5, y + 0.5, w, h);

        for (var parent_idx = 0; parent_idx < node.parents.length; ++ parent_idx) {
            var parent_ = node.parents[parent_idx];

            ctx.beginPath();
            ctx.moveTo(node.coords.l + Math.floor(node.coords.w / 2) + 0.5, node.coords.t + 0.5);
            ctx.lineTo(parent_.coords.l + Math.floor(parent_.coords.w / 2) + 0.5, parent_.coords.t + parent_.coords.h + 0.5);
            ctx.stroke();
        }
    });

    return user_l;
}

var serialize_txflow = function(graph, beacon_mode) {
    var nodes = toposort(graph);

    var s = [];
    var mapping = {};
    for (var node_idx = 0; node_idx < nodes.length; ++ node_idx) {
        var node = nodes[node_idx];
        var snode = {};
        snode.owner = node.owner
        snode.parents = [];
        for (var parent_idx = 0; parent_idx < node.parents.length; ++ parent_idx) {
            snode.parents.push(mapping[node.parents[parent_idx].uid]);
        }

        if (beacon_mode) {
            snode.payload_type = node.payload_type || 0;
            if (snode.payload_type == 1) {
                snode.prevblock = node.prevblock || 0;
            }
        }

        s.push(snode);

        mapping[node.uid] = node_idx;
    }
    return s;
}

var deserialize_txflow = function(s, beacon_mode) {
    last_node_uid = 0;
    var nodes = [];
    for (var i = 0; i < s.length; ++ i) {
        var snode = s[i];
        var parents = [];
        for (var j = 0; j < snode.parents.length; ++ j) {
            parents.push(nodes[snode.parents[j]]);
        }
        var node = create_node(snode.owner, parents);
        if (beacon_mode) {
            node.payload_type = snode.payload_type;
            if (node.payload_type == 1) {
                node.prevblock = snode.prevblock;
            }
        }
        nodes.push(node);
    }
    var graph = [];
    for (var i = 0; i < nodes.length; ++ i) {
        if (nodes[i].children.length == 0) {
            graph.push(nodes[i]);
        }
    }
    return graph;
}

var gen_rust_endorsements = function(node, num_users) {
    var annotations = compute_annotations([node], num_users);
    var nodes = toposort([node]);

    var a_mapping = {};

    for (var aidx = 0; aidx < annotations.length; ++ aidx) {
        a_mapping[annotations[aidx].node.uid] = annotations[aidx];
    }

    var ret = [];

    for (var i = 0; i < nodes.length; ++ i) {
        var node = nodes[i];
        var a = a_mapping[node.uid];

        if (a.representative > -1) {
            for (var e in a.endorsements) {
                while (ret.length <= a.representative) ret.push([]);
                ret[a.representative].push(e);
            }
        }
    }

    var gen_list = function(arr) {
        arr.sort();
        var ret = [];
        for (var i = 0; i < arr.length; ++ i) {
            if (i == 0 || arr[i] != arr[i - 1]) {
                ret.push(arr[i]);
            }
        }
        return ret;
    }

    return '&[' + ret.map(x => "vec![" + gen_list(x).join(', ') + "]").join(', ') + ']';
}

var gen_rust = function(graph, num_users, fn_name, comment, serialized) {
    var annotations = compute_annotations(graph, num_users);
    var nodes = toposort(graph);

    var a_mapping = {};

    for (var aidx = 0; aidx < annotations.length; ++ aidx) {
        a_mapping[annotations[aidx].node.uid] = annotations[aidx];
    }

    var indent = "         ";
    var var_names = [];
    for (var i = 0; i < nodes.length; ++ i) {
        var_names.push("v" + nodes[i].uid);
    }
    var s = "    #[test]\n    fn generated_" + fn_name + "() {\n" + indent + "/* " + comment + " */\n\n" + indent + "/* " + serialized + " */\n"  + indent + "let arena = Arena::new();\n" + indent + "let selector = FakeNonContinuousWitnessSelector::new(" + num_users + ");\n"
    if (var_names.length > 1) {
        s += indent + "let (" + var_names.join(',') + ");\n";
    }
    else {
        s += indent + "let " + var_names[0] + ";\n";
    }
    s += indent + "let mut v = [None; " + var_names.length + "];\n";
    var nf = function(node) {
        var a = a_mapping[node.uid];
        var repr = a.representative < 0 ? 'None' : `Some(${a.representative})`;
        return `(${a.epoch}, ${repr}, ${a.kickout}, ${node.uid})`;
    };
    s += indent + 'let test = |v:&[_]| make_assertions(&v, &[' + nodes.map(nf).join(', ') + ']);\n';
    for (var i = 0; i < nodes.length; ++ i) {
        var node = nodes[i];
        s += indent + 'simple_messages!(0, &selector, arena [';
        if (node.parents.length > 0) {
            var parent_names = [];
            for (var parent_idx = 0; parent_idx < node.parents.length; ++ parent_idx) {
                parent_names.push('=> v' + node.parents[parent_idx].uid + '; ');
            }
            s += '[' + parent_names.join('') + '] => ';
        }
        // use 0 for epoch, and `true` to recompute the epochs
        s += node.owner + ', 0, true => ' + var_names[i] + ';]); v[' + i + '] = Some(' + var_names[i] + ');\n'
        s += indent + 'test(&v);\n';

        s += indent + 'test_endorsements(' + var_names[i] + ', ' + gen_rust_endorsements(node, num_users) + ', ' + num_users + ');\n';
    }

    s += "    }\n"
    return s;
}

if (typeof module !== 'undefined') {
    module.exports.deserialize_txflow = deserialize_txflow;
    module.exports.gen_rust = gen_rust;
    module.exports.create_node = create_node;
    module.exports.compute_annotations = compute_annotations;
    module.exports.toposort = toposort;
}

