use std::env;
use std::io;

use actix;

use tokio::sync::mpsc;
use tracing_subscriber::filter::LevelFilter;
use tracing_subscriber::EnvFilter;

use near_indexer;

fn init_logging(verbose: bool) {
    let mut env_filter = EnvFilter::new("tokio_reactor=info,near=info,stats=info,telemetry=info");

    if verbose {
        env_filter = env_filter
            .add_directive("cranelift_codegen=warn".parse().unwrap())
            .add_directive("cranelift_codegen=warn".parse().unwrap())
            .add_directive("h2=warn".parse().unwrap())
            .add_directive("trust_dns_resolver=warn".parse().unwrap())
            .add_directive("trust_dns_proto=warn".parse().unwrap());

        env_filter = env_filter.add_directive(LevelFilter::DEBUG.into());
    } else {
        env_filter = env_filter.add_directive(LevelFilter::WARN.into());
    }

    if let Ok(rust_log) = env::var("RUST_LOG") {
        for directive in rust_log.split(',').filter_map(|s| match s.parse() {
            Ok(directive) => Some(directive),
            Err(err) => {
                eprintln!("Ignoring directive `{}`: {}", s, err);
                None
            }
        }) {
            env_filter = env_filter.add_directive(directive);
        }
    }
    tracing_subscriber::fmt::Subscriber::builder()
        .with_env_filter(env_filter)
        .with_writer(io::stderr)
        .init();
}

async fn listen_blocks(mut stream: mpsc::Receiver<near_indexer::BlockResponse>) {
    while let Some(block) = stream.recv().await {
        // TODO: handle data as you need
        eprintln!("{:#?}", block);
    }
}

fn main() {
    init_logging(true);
    let indexer = near_indexer::Indexer::new(None);
    let stream = indexer.streamer();
    actix::spawn(listen_blocks(stream));
    indexer.start();
}
