extern crate primitives;

use primitives::signature::get_keypair;

fn main() {
    let (public_key, secret_key) = get_keypair();
    println!("Public key: {}", public_key);
    println!("Secret key: {}", secret_key);
}
