extern crate client;
extern crate service;
extern crate storage;

use client::Client;
use service::Service;
use storage::Storage;

pub fn run() {
    // TODO: add argument parsing into service/config.rs.
    let storage = Storage::new("storage/db/");
    let client = Client::new(storage);
    let service = Service::new(client);
    service.run()
}
