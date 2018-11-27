var STATE_INIT = 0;
var STATE_PREVDF = 1;
var STATE_BLOCK = 2;
var STATE_COMMIT = 3;
var STATE_SIGNATURE = 4;
var STATE_FINAL = 5;

var STATE_NAMES = ['init', 'pre', 'blk', 'com', 'sig', 'fin'];

var PAYLOAD_NONE = 0;
var PAYLOAD_PREVBLOCK = 1;
var PAYLOAD_VDF = 2;
var PAYLOAD_COMMIT = 3;
var PAYLOAD_SIGNATURE = 4;

var NUM_SUBSTATES = 2;

var state_eq = function(n1, n2) { return n1.state == n2.state && n1.substate == n2.substate; }
var state_gt = function(n1, n2) { return n1.state > n2.state || n1.state == n2.state && n1.substate > n2.substate; }

var add_beacon_annotations = function(nodes, annotations, num_users) {
    var mapping = {};

    for (var a of annotations) {
        mapping[a.node.uid] = a;
    }

    for (var node of nodes) {
        var a = mapping[node.uid];

        if (a === undefined) continue;

        var largest_state = {};
        var visited = {};
        
        var saw_invalid = false;
        var dfs_largest_state = function(n, first) {
            if (visited[n.uid] !== undefined) return;
            visited[n.uid] = true;

            for (var p of n.parents) dfs_largest_state(p, false);

            if (first) return;

            var a = mapping[n.uid];
            if (!a.valid) {
                saw_invalid = true;
                return;
            }
            if (largest_state[n.owner] === undefined || state_gt(a, largest_state[n.owner])) largest_state[n.owner] = a;
        }

        dfs_largest_state(node, true);

        var state_increase = false;
        if (largest_state[node.owner] !== undefined) {
            a.state = largest_state[node.owner].state;
            a.substate = largest_state[node.owner].substate;
        }
        else {
            state_increase = true;
            a.state = STATE_INIT;
            a.substate = 0;
        }

        if (a.state == STATE_PREVDF) {
            var increment = node.payload_type == PAYLOAD_VDF;
            if (!increment) {
                var num_vdfs = 0;
                for (var key in largest_state) {
                    if (largest_state[key].state >= STATE_BLOCK) {
                        ++ num_vdfs;
                    }
                }
                if (num_vdfs > Math.floor(num_users * 2 / 3)) {
                    increment = true;
                }
            }
            if (increment) {
                state_increase = true;
                a.state = STATE_BLOCK;
                a.substate = 0;
            }
        }
        else if (a.state == STATE_FINAL) {
            // can't increase the final state
        }
        else {
            var increment = false;
            var num_same = 0;
            for (var key in largest_state) {
                if (!state_gt(a, largest_state[key])) {
                    ++ num_same;
                }
            }
            if (num_same > Math.floor(num_users * 2 / 3)) {
                if (a.substate == NUM_SUBSTATES - 1) {
                    a.ready_to_increment = true;
                    var increment = false;
                    var visited = {};
                    var nas = compute_annotations([node], num_users);
                    for (var na of nas) {
                        // note that for `ready_to_incrememt` we need to use annotations from the outer scope
                        var pa = mapping[na.node.uid];
                        if (pa.state == a.state && pa.substate == a.substate && pa.ready_to_increment && na.epoch_block) {
                            increment = true;
                        }
                    }
                    if (increment) {
                        state_increase = true;
                        ++ a.state;
                        a.substate = 0;
                    }
                }
                else {
                    ++ a.substate;
                }
            }
        }

        a.valid = !saw_invalid;
        if (state_increase) {
            if (a.state == STATE_INIT && node.payload_type != PAYLOAD_PREVBLOCK) a.valid = false;
            if (a.state == STATE_COMMIT && node.payload_type != PAYLOAD_COMMIT) a.valid = false;
            if (a.state == STATE_SIGNATURE && node.payload_type != PAYLOAD_SIGNATURE) a.valid = false;
        }
    }
}

var compute_beacon_chain_observation = function(node, num_users) {
    var annotations = compute_annotations([node], num_users)
    var toposorted = toposort([node]);
    add_beacon_annotations(toposorted, annotations, num_users);

    ret = {};
    for (var a of annotations) {
        var node = a.node;

        if (a.epoch_block && a.state != STATE_PREVDF && a.state != STATE_FINAL) {
            var key = STATE_NAMES[a.state + 1];
            if (ret[key] === undefined || ret[key] > a.representative) {
                ret[key] = a.representative;
            }
        }
    }

    return ret;
}


