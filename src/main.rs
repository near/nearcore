mod network;

use network::create_connection_handler;
use network::optimize_memory_usage;

fn main() {
    let connection_handler = create_connection_handler();
    // ...
    optimize_memory_usage();
}