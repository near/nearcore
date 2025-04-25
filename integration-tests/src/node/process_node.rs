use std::ops::ControlFlow;
use std::path::PathBuf;
use std::process::{Child, Command};
use std::sync::Arc;
use std::time::Duration;
use std::{env, thread};

use rand::Rng;
use tracing::error;

use near_chain_configs::Genesis;
use near_crypto::{InMemorySigner, Signer};
use near_primitives::types::AccountId;
use nearcore::config::NearConfig;

use crate::node::Node;
use crate::user::User;
use crate::user::rpc_user::RpcUser;
use actix::System;
use near_jsonrpc_client_internal::new_client;
use near_network::test_utils::wait_or_timeout;

pub enum ProcessNodeState {
    Stopped,
    Running(Child),
}

pub struct ProcessNode {
    pub work_dir: PathBuf,
    pub config: NearConfig,
    pub state: ProcessNodeState,
    pub signer: Arc<Signer>,
    account_id: AccountId,
}

impl Node for ProcessNode {
    fn genesis(&self) -> &Genesis {
        &self.config.genesis
    }

    fn account_id(&self) -> Option<AccountId> {
        self.config.validator_signer.get().map(|vs| vs.validator_id().clone())
    }

    fn start(&mut self) {
        match self.state {
            ProcessNodeState::Stopped => {
                unsafe {
                    std::env::set_var("ADVERSARY_CONSENT", "1");
                }
                let child =
                    self.get_start_node_command().spawn().expect("start node command failed");
                self.state = ProcessNodeState::Running(child);
                let client_addr = format!("http://{}", self.config.rpc_addr().unwrap());
                thread::sleep(Duration::from_secs(3));
                near_actix_test_utils::run_actix(async move {
                    wait_or_timeout(1000, 30000, || async {
                        let res = new_client(&client_addr).status().await;
                        if res.is_err() {
                            return ControlFlow::Continue(());
                        }
                        ControlFlow::Break(())
                    })
                    .await
                    .unwrap();
                    System::current().stop()
                });
            }
            ProcessNodeState::Running(_) => panic!("Node is already running"),
        }
    }

    fn kill(&mut self) {
        match self.state {
            ProcessNodeState::Running(ref mut child) => {
                child.kill().expect("kill failed");
                thread::sleep(Duration::from_secs(1));
                self.state = ProcessNodeState::Stopped;
            }
            ProcessNodeState::Stopped => panic!("Invalid state"),
        }
    }

    fn signer(&self) -> Arc<Signer> {
        self.signer.clone()
    }

    fn is_running(&self) -> bool {
        match self.state {
            ProcessNodeState::Stopped => false,
            ProcessNodeState::Running(_) => true,
        }
    }

    fn user(&self) -> Box<dyn User> {
        Box::new(RpcUser::new(
            &self.config.rpc_addr().unwrap(),
            self.account_id.clone(),
            self.signer.clone(),
        ))
    }

    fn as_process_ref(&self) -> &ProcessNode {
        self
    }

    fn as_process_mut(&mut self) -> &mut ProcessNode {
        self
    }
}

impl ProcessNode {
    /// Side effect: reset_storage
    pub fn new(config: NearConfig) -> ProcessNode {
        let mut rng = rand::thread_rng();
        let work_dir = env::temp_dir().join(format!("process_node_{}", rng.r#gen::<u64>()));
        let account_id = config.validator_signer.get().unwrap().validator_id().clone();
        let signer = Arc::new(InMemorySigner::test_signer(&account_id));
        let result =
            ProcessNode { config, work_dir, state: ProcessNodeState::Stopped, signer, account_id };
        result.reset_storage();
        result
    }

    /// Clear storage directory and run key gen
    pub fn reset_storage(&self) {
        Command::new("rm").arg("-r").arg(&self.work_dir).spawn().unwrap().wait().unwrap();
        self.config.save_to_dir(&self.work_dir);
    }

    /// Side effect: writes chain spec file
    pub fn get_start_node_command(&self) -> Command {
        if std::env::var("NIGHTLY_RUNNER").is_err() {
            let mut command = Command::new("cargo");
            command.args(&["run", "-p", "neard"]);
            #[cfg(feature = "nightly")]
            command.args(&["--features", "nightly"]);
            command.args(&["--bin", "neard", "--", "--home"]);
            command.arg(&self.work_dir);
            command.arg("run");
            command
        } else {
            let mut command = Command::new("target/debug/neard");
            command.arg("--home");
            command.arg(&self.work_dir);
            command.arg("run");
            command
        }
    }
}

impl Drop for ProcessNode {
    fn drop(&mut self) {
        match self.state {
            ProcessNodeState::Running(ref mut child) => {
                let _ = child.kill().map_err(|_| error!("child process died"));
                std::fs::remove_dir_all(&self.work_dir).unwrap();
            }
            ProcessNodeState::Stopped => {}
        }
    }
}
