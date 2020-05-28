#[cfg(test)]
extern crate quickcheck;

use actix::System;
use serde_json;
use tokio;

use near_logger_utils::init_test_logger;

#[macro_use]
pub mod test_utils;

#[test]
fn test_invalid_method_name_returns_errors() {
    init_test_logger();

    static mut NODE_ADDR: Option<String> = None;

    std::thread::spawn(|| {
        System::run(|| {
            let (_view_client_addr, addr) =
                test_utils::start_all(test_utils::NodeType::NonValidator);
            unsafe {
                NODE_ADDR = Some(addr);
            }
        })
        .unwrap();
    });

    static RUNTIME: once_cell::sync::Lazy<std::sync::Mutex<tokio::runtime::Runtime>> =
        once_cell::sync::Lazy::new(|| {
            std::sync::Mutex::new(
                tokio::runtime::Builder::new()
                    .basic_scheduler()
                    .threaded_scheduler()
                    .enable_all()
                    .build()
                    .unwrap(),
            )
        });

    fn client_call(method: String, id: String) -> bool {
        RUNTIME.lock().unwrap().block_on(async move {
            let post_data = serde_json::json!({
                "jsonrpc": "2.0",
                "method": method,
                "params": "",
                "id": id,
            });

            let client = reqwest::Client::new();
            let response = client
                .post(&format!("http://{}", unsafe { NODE_ADDR.as_ref().unwrap() }))
                .json(&post_data)
                .send()
                .await
                .unwrap();

            if response.status() != 200 {
                return false;
            }

            let result_or_error: serde_json::Value = response.json().await.unwrap();
            result_or_error["error"] != serde_json::json!(null)
        })
    }

    std::thread::sleep(std::time::Duration::from_secs(3)); // ensure node have enough time to start
    quickcheck::quickcheck(client_call as fn(String, String) -> bool);
}
