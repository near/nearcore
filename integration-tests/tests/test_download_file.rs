use std::collections::HashMap;
use std::fs::File;
use std::io::{Read, Write};
use std::path::Path;
use testcontainers::*;

#[derive(Default, Debug, Clone)]
pub struct NginxArgs {}

impl IntoIterator for NginxArgs {
    type Item = String;
    type IntoIter = ::std::vec::IntoIter<String>;

    fn into_iter(self) -> Self::IntoIter {
        vec![].into_iter()
    }
}

#[derive(Debug, Default)]
pub struct Nginx<'a> {
    arguments: NginxArgs,
    env_vars: HashMap<String, String>,
    version: String,
    static_content_path: Option<&'a Path>,
}

impl Image for Nginx<'_> {
    type Args = NginxArgs;
    type EnvVars = HashMap<String, String>;
    type Volumes = HashMap<String, String>;
    type EntryPoint = std::convert::Infallible;

    fn descriptor(&self) -> String {
        format!("nginx:{}", self.version)
    }

    fn wait_until_ready<D: Docker>(&self, _container: &Container<'_, D, Self>) {}

    fn args(&self) -> Self::Args {
        self.arguments.clone()
    }

    fn env_vars(&self) -> Self::EnvVars {
        self.env_vars.clone()
    }

    fn volumes(&self) -> Self::Volumes {
        let mut volumes = HashMap::new();
        match self.static_content_path {
            None => {}
            Some(static_content_path) => {
                volumes.insert(
                    static_content_path.to_str().unwrap().into(),
                    "/usr/share/nginx/html".into(),
                );
                ()
            }
        }
        volumes
    }

    fn with_args(self, arguments: Self::Args) -> Self {
        Self { arguments, ..self }
    }
}

#[test]
fn test_file_download() {
    // Set up a temporary folder holding static file that is going to be served by nginx
    let tmp_static_content_dir = tempfile::tempdir().unwrap();
    let static_file_name: String = "file_to_download".into();
    let tmp_static_file_path = tmp_static_content_dir.path().join(&static_file_name);
    // Create a static file that holds 1MB of zeros
    let mut tmp_static_file = File::create(tmp_static_file_path).unwrap();
    let zeros: [u8; 1024 * 1024] = [0; 1024 * 1024];
    tmp_static_file.write_all(&zeros).unwrap();

    let mut nginx = Nginx::default();
    nginx.version = "1.21".to_string();
    nginx.static_content_path = Some(tmp_static_content_dir.path());

    let docker = clients::Cli::default();
    let container = docker.run(nginx);
    // Port 80 is exposed dynamically using one of the free ports on the host machine, so we need
    // to figure out what it is
    let container_port = container.get_host_port(80).unwrap();

    let tmp_downloaded_file = tempfile::NamedTempFile::new().unwrap();
    let tmp_downloaded_file_path = tmp_downloaded_file.path();
    nearcore::config::download_file(
        &format!("http://localhost:{}/{}", container_port, static_file_name),
        tmp_downloaded_file_path,
    )
    .unwrap();

    let mut downloaded_file = File::open(tmp_downloaded_file_path).unwrap();
    let mut downloaded_file_content: Vec<u8> = Vec::new();
    downloaded_file.read_to_end(&mut downloaded_file_content).unwrap();

    assert_eq!(downloaded_file_content, zeros.to_vec());
}
