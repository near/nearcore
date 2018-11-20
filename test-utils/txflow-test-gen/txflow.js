// Any functions that has an argument `nodes` expects nodes to be toposorted

var last_node_uid = 0;
var NODE_W = 72;
var NODE_H = 40;
var NODE_S = 16;
var NODE_US = 32;

var PIXEL_RATIO = (function () {
    var ctx = document.createElement("canvas").getContext("2d"),
        dpr = window.devicePixelRatio || 1,
        bsr = ctx.webkitBackingStorePixelRatio ||
              ctx.mozBackingStorePixelRatio ||
              ctx.msBackingStorePixelRatio ||
              ctx.oBackingStorePixelRatio ||
              ctx.backingStorePixelRatio || 1;
    return dpr / bsr;
})();

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
    
    for (var node_idx = 0; node_idx < nodes.length; ++ node_idx) {
        var node = nodes[node_idx];
        var owner = node.owner;
        var a = {'node' : node, 'endorsements': {}};

        annotations.push(a);
        mapping[node.uid] = a;

        var largest_epoch = [];
        var largest_kickout = [];
        for (var i = 0; i < num_users; ++ i) {
            largest_epoch[i] = -1;
            largest_kickout[i] = -1;
        }
        var largest_representative = 0;

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
        }
        dfs(node);
        
        var dfs_endorse = function(node) {
            if (visited[node.uid] !== 1) {
                return;
            }
            visited[node.uid] = 2;

            for (var parent_idx = 0; parent_idx < node.parents.length; ++ parent_idx) {
                var parent_ = node.parents[parent_idx];
                dfs_endorse(parent_);
            }

            var ok = true;
            for (var idx = 0; idx < node.owner_level_nodes.length; ++ idx ) {
                var conflict_node = node.owner_level_nodes[idx];
                if (conflict_node.uid != node.uid && visited[conflict_node.uid]) {
                    ok = false;
                }
            }

            if (ok) {
                mapping[node.uid].endorsements[owner] = true;
            }
        }
        dfs_endorse(node);

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

var serialize_txflow = function(graph) {
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

        s.push(snode);

        mapping[node.uid] = node_idx;
    }
    return s;
}

var deserialize_txflow = function(s) {
    var nodes = [];
    for (var i = 0; i < s.length; ++ i) {
        var snode = s[i];
        var parents = [];
        for (var j = 0; j < snode.parents.length; ++ j) {
            parents.push(nodes[snode.parents[j]]);
        }
        nodes.push(create_node(snode.owner, parents));
    }
    var graph = [];
    for (var i = 0; i < nodes.length; ++ i) {
        if (nodes[i].children.length == 0) {
            graph.push(nodes[i]);
        }
    }
    return graph;
}

