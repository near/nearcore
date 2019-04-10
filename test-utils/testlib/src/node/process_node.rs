use crate::node::{LocalNodeConfig, Node};
use crate::user::{RpcUser, User};
use log::error;
use primitives::crypto::signer::InMemorySigner;
use primitives::types::AccountId;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::process::{Child, Command};
use std::sync::Arc;
use std::time::Duration;
use std::{fs, thread};

pub enum ProcessNodeState {
    Stopped,
    Running(Child),
}

pub struct ProcessNode {
    pub config: LocalNodeConfig,
    pub state: ProcessNodeState,
}

impl Node for ProcessNode {
    fn account_id(&self) -> Option<&AccountId> {
        self.config.client_cfg.account_id.as_ref()
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

    fn signer(&self) -> Arc<InMemorySigner> {
        let account_id = &self.config.client_cfg.account_id.clone().expect("Must have signer");
        Arc::new(InMemorySigner::from_seed(account_id, account_id))
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

    fn as_process_ref(&self) -> &ProcessNode {
        self
    }

    fn as_process_mut(&mut self) -> &mut ProcessNode {
        self
    }

    fn is_running(&self) -> bool {
        match self.state {
            ProcessNodeState::Stopped => false,
            ProcessNodeState::Running(_) => true,
        }
    }

    fn user(&self) -> Box<User> {
        Box::new(RpcUser::new(SocketAddr::new(
            IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
            self.config.rpc_cfg.rpc_port,
        )))
    }
}

impl ProcessNode {
    /// Side effect: reset_storage
    pub fn new(config: LocalNodeConfig) -> ProcessNode {
        let result = ProcessNode { config, state: ProcessNodeState::Stopped };
        result.reset_storage();
        result
    }

    /// Clear storage directory and run keygen
    pub fn reset_storage(&self) {
        let keygen_path = self.config.client_cfg.base_path.join("storage/keystore");
        Command::new("rm")
            .args(&["-r", self.config.client_cfg.base_path.to_str().unwrap()])
            .spawn()
            .unwrap()
            .wait()
            .unwrap();
        Command::new("cargo")
            .args(&[
                "run",
                "--package",
                "keystore",
                "--",
                "keygen",
                "--test-seed",
                self.config.client_cfg.account_id.clone().expect("Must have account").as_str(),
                "-p",
                keygen_path.to_str().unwrap(),
            ])
            .spawn()
            .expect("keygen command failed")
            .wait()
            .expect("keygen command failed");
    }

    /// Side effect: writes chain spec file
    pub fn get_start_node_command(&self) -> Command {
        let account_name = self.config.client_cfg.account_id.clone().expect("Must have account");
        let pubkey = InMemorySigner::from_seed(&account_name, &account_name).public_key;
        let chain_spec = &self.config.client_cfg.chain_spec;
        let chain_spec_path = self.config.client_cfg.base_path.join("chain_spec.json");
        if !self.config.client_cfg.base_path.exists() {
            fs::create_dir_all(&self.config.client_cfg.base_path).unwrap();
        }
        chain_spec.write_to_file(&chain_spec_path);

        let mut start_node_command = Command::new("cargo");
        start_node_command.args(&[
            "run",
            "--",
            "--rpc_port",
            format!("{}", self.config.rpc_cfg.rpc_port).as_str(),
            "--base-path",
            self.config.client_cfg.base_path.to_str().unwrap(),
            "--test-network-key-seed",
            format!("{}", self.config.peer_id_seed).as_str(),
            "--chain-spec-file",
            chain_spec_path.to_str().unwrap(),
            "-a",
            account_name.as_str(),
            "-k",
            format!("{}", pubkey).as_str(),
        ]);
        if let Some(ref addr) = self.config.node_info.addr {
            start_node_command.args(&["--addr", format!("{}", addr).as_str()]);
        }
        if !self.config.network_cfg.boot_nodes.is_empty() {
            let boot_node = format!("{}", self.config.network_cfg.boot_nodes[0]);
            start_node_command.args(&["--boot-nodes", boot_node.as_str()]);
        }
        start_node_command
    }
}

impl Drop for ProcessNode {
    fn drop(&mut self) {
        match self.state {
            ProcessNodeState::Running(ref mut child) => {
                let _ = child.kill().map_err(|_| error!("child process died"));
            }
            ProcessNodeState::Stopped => {}
        }
    }
}
