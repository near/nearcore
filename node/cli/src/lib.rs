extern crate service;
extern crate client;

use client::Client;
use service::Service;

pub fn run() {
    let client = Client::new();
    let service = Service::new(client);
    service.run()
}
