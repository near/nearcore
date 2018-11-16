extern crate service;

use service::Service;

pub fn run() {
    let service = Service::default();
    service.run()
}
