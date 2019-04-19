use clap::{Arg, ArgMatches};

const DEFAULT_RPC_PORT: &str = "3030";

#[derive(Clone)]
pub struct RPCConfig {
    pub rpc_port: u16,
}

impl Default for RPCConfig {
    fn default() -> Self {
        Self { rpc_port: DEFAULT_RPC_PORT.parse::<u16>().unwrap() }
    }
}

pub fn get_args<'a, 'b>() -> Vec<Arg<'a, 'b>> {
    vec![Arg::with_name("rpc_port")
        .short("r")
        .long("rpc_port")
        .value_name("PORT")
        .help("Specify the rpc protocol TCP port.")
        .default_value(DEFAULT_RPC_PORT)
        .takes_value(true)]
}

pub fn from_matches(matches: &ArgMatches) -> RPCConfig {
    RPCConfig { rpc_port: matches.value_of("rpc_port").map(|x| x.parse::<u16>().unwrap()).unwrap() }
}
