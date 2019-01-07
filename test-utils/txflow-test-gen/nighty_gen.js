var txflow = require('./txflow.js');
var utils = require('./utils.js');
const fs = require('fs');
const path = require('path');

maxUsers = 25;
maxNodes = 100;

function getRandomInt(max) {
    return Math.floor((Math.random()) * Math.floor(max)) + 1;
}

common_for_tests = fs.readFileSync(path.resolve(__dirname, 'common_rust_code_for_tests.txt'), 'utf8');

failed_rust = false;

while (true) {
    totalUsers = getRandomInt(maxUsers);
    maliciousUsers = getRandomInt(totalUsers / 3 - 1);
    totalNodes = getRandomInt(maxNodes);
    console.log("totalUsers: " + totalUsers + " maliciousUsers: " + maliciousUsers + " totalNodes: " + totalNodes);
    timeStamp = Math.floor(Date.now() / 1000);
    fn_name = "nightly_generated_test.rs";
    comment = "Test generated @" + timeStamp;
    serialized = false;
    beaconMode = false;
    graph = utils.generate_random_graph(totalUsers, maliciousUsers, totalNodes, beaconMode, show_html = false);
    result = utils.stress_epoch_blocks(txflow.toposort(graph), totalUsers);
    console.log(result);

    if (!result.includes('PASSED!')) {
        console.error("totalUsers: " + totalUsers + " maliciousUsers: " + maliciousUsers + " totalNodes: " + totalNodes);
        console.error(result);
        console.error(graph);
    }

    if (!failed_rust) {
        rust_test = txflow.gen_rust(graph, totalUsers, timeStamp, comment, serialized)

        fs.writeFileSync(path.resolve(__dirname, '../../core/txflow/src/dag/message/' + fn_name), common_for_tests + '\n' + rust_test + '}');

        const execSync = require('child_process').execSync;
        try {
            code = execSync('cargo test -p txflow dag::message::nightly_generated_test').toString();
            console.log("Rust Passed!");

        } catch (ex) {
            failed_rust = true;
            console.error("totalUsers: " + totalUsers + " maliciousUsers: " + maliciousUsers + " totalNodes: " + totalNodes);
            console.error(ex.output.toString());
        }
    }
}