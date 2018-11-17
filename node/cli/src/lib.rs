extern crate client;
extern crate service;

use client::Client;
use service::Service;

pub fn run() {
    let client = Client::new("storage/db/");
    let service = Service::new(client);
    service.run()
}
