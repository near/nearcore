use std::path::Path;
use std::process::{Child, Command};
use std::sync::Arc;
use std::time::Duration;
use std::{env, thread};

use log::error;
use rand::Rng;

use near::config::NearConfig;
use near_primitives::crypto::signer::EDSigner;
use near_primitives::types::AccountId;

use crate::node::Node;
use crate::user::rpc_user::RpcUser;
use crate::user::User;

pub enum ProcessNodeState {
    Stopped,
    Running(Child),
}

pub struct ProcessNode {
    pub work_dir: String,
    pub config: NearConfig,
    pub state: ProcessNodeState,
}

impl Node for ProcessNode {
    fn account_id(&self) -> Option<AccountId> {
        match &self.config.block_producer {
            Some(bp) => Some(bp.account_id.clone()),
            None => None,
        }
    }

    fn start(&mut self) {
        match self.state {
            ProcessNodeState::Stopped => {
                let child =
                    self.get_start_node_command().spawn().expect("start node command failed");
                self.state = ProcessNodeState::Running(child);
                thread::sleep(Duration::from_secs(3));
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

    fn signer(&self) -> Arc<dyn EDSigner> {
        self.config.block_producer.clone().unwrap().signer.clone()
    }

    fn is_running(&self) -> bool {
        match self.state {
            ProcessNodeState::Stopped => false,
            ProcessNodeState::Running(_) => true,
        }
    }

    fn user(&self) -> Box<dyn User> {
        Box::new(RpcUser::new(&self.config.rpc_config.addr))
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
        let work_dir = format!(
            "{}process_node_{}",
            env::temp_dir().as_path().to_str().unwrap(),
            rng.gen::<u64>()
        );
        let result = ProcessNode { config, work_dir, state: ProcessNodeState::Stopped };
        result.reset_storage();
        result
    }

    /// Clear storage directory and run keygen
    pub fn reset_storage(&self) {
        Command::new("rm").args(&["-r", &self.work_dir]).spawn().unwrap().wait().unwrap();
        self.config.save_to_dir(Path::new(&self.work_dir));
    }

    /// Side effect: writes chain spec file
    pub fn get_start_node_command(&self) -> Command {
        let mut command = Command::new("cargo");
        command.args(&["run", "-p", "near", "--", "--home", &self.work_dir, "run"]);
        command
    }
}

impl Drop for ProcessNode {
    fn drop(&mut self) {
        match self.state {
            ProcessNodeState::Running(ref mut child) => {
                let _ = child.kill().map_err(|_| error!("child process died"));
                std::fs::remove_dir_all(self.work_dir.clone()).unwrap();
            }
            ProcessNodeState::Stopped => {}
        }
    }
}
