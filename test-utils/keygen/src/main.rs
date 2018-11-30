extern crate clap;
extern crate primitives;

use clap::{App, Arg, ArgMatches, SubCommand};
use primitives::signature::get_keypair;
use std::process;
use std::path::Path;
use std::fs;

fn generate_keys(matches: &ArgMatches) {
    let private_key_path = matches
        .value_of("output_file")
        .map(|x| Path::new(x))
        .unwrap();

    let public_key_path = private_key_path.with_extension("pub");

    if private_key_path.exists() {
        eprintln!("Private key file already exists: {:?}", &private_key_path);
        process::exit(1);
    } else if public_key_path.exists() {
        eprintln!("Public key file already exists: {:?}", &public_key_path);
        process::exit(1);
    }

    let (public_key, secret_key) = get_keypair();
    fs::write(public_key_path, public_key.to_string()).unwrap();
    fs::write(private_key_path, secret_key.to_string()).unwrap();
}

fn main() {
    let matches = App::new("keystore")
        .subcommand(SubCommand::with_name("keygen")
            .arg(Arg::with_name("output_file")
                .short("o")
                .long("output-file")
                .value_name("OUTPUT_FILE")
                .help("Sets a file location for private key file")
                .default_value("near_key")
                .required(true)
                .takes_value(true)
            )
        ).get_matches();

    if let Some(sub) = matches.subcommand_matches("keygen") {
        generate_keys(sub);
    } else {
        eprintln!("Incorrect usage. See usage with: keystore --help");
        process::exit(1);
    }
}
