var lineReader = require('readline').createInterface({
    input: require('fs').createReadStream('tests.jsonl')
});

var txflow = require('./txflow.js');

lineReader.on('line', function (line) {
    if (!line.trim()) return

    var j = JSON.parse(line);
    var graph = txflow.deserialize_txflow(j.s);
    var num_users = j.n;

    console.log(txflow.gen_rust(graph, num_users, j.f, j.c, JSON.stringify(j)));
});

