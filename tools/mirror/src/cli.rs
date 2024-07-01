use anyhow::{Context, Result};
use near_primitives::types::BlockHeight;
use std::path::PathBuf;
use tokio::runtime::Runtime;

#[derive(clap::Parser)]
pub struct MirrorCommand {
    #[clap(subcommand)]
    subcmd: SubCommand,
}

#[derive(clap::Parser)]
enum SubCommand {
    Prepare(PrepareCmd),
    Run(RunCmd),
}

#[derive(clap::Parser)]
struct RunCmd {
    #[clap(long)]
    source_home: PathBuf,

    #[clap(long)]
    target_home: PathBuf,

    #[clap(long)]
    secret_file: Option<PathBuf>,

    #[clap(long)]
    no_secret: bool,

    #[clap(long)]
    online_source: bool,

    #[clap(long)]
    stop_height: Option<BlockHeight>,

    #[clap(long)]
    config_path: Option<PathBuf>,
}

impl RunCmd {
    //returning a Result instead of unwrapping, to handle any error properly
    //===================================
    //The RunCmd::run method is changed to asynchronous and properly awaits the asynchronous operations.
    async fn run(self) -> Result<()> {
        openssl_probe::init_ssl_cert_env_vars();
        let runtime = Runtime::new().context("failed to start tokio runtime")?;
        let secret = if let Some(secret_file) = &self.secret_file {
            let secret = crate::secret::load(secret_file)
                .with_context(|| format!("Failed to load secret from {:?}", secret_file))?;
            if secret.is_some() && self.no_secret {
                anyhow::bail!(
                    "--no-secret given with --secret-file indicating that a secret should be used"
                );
            }
            secret
        } else {
            if !self.no_secret {
                anyhow::bail!("Please give either --secret-file or --no-secret");
            }
            None
        };
        
        let system = new_actix_system(runtime);
        system.block_on(async move {
            let _subscriber_guard = near_o11y::default_subscriber(
                near_o11y::EnvFilterBuilder::from_env().finish().unwrap(),
                &near_o11y::Options::default(),
            )
            .global();
            
            /*
            actix::spawn(crate::run(
                    self.source_home,
                    self.target_home,
                    secret,
                    self.stop_height,
                    self.online_source,
                    self.config_path,
                ))
                .await
            })
            .unwrap()
            changes will be made to the above code to handle the Result properly
            */
            crate::run(
                self.source_home,
                self.target_home,
                secret,
                self.stop_height,
                self.online_source,
                self.config_path,
            ).await
        })?;
        
        Ok(())
    }
}

#[derive(clap::Parser)]
struct PrepareCmd {
    #[clap(long)]
    records_file_in: PathBuf,

    #[clap(long)]
    records_file_out: PathBuf,

    #[clap(long)]
    no_secret: bool,

    #[clap(long)]
    secret_file_out: PathBuf,
}

impl PrepareCmd {
    fn run(self) -> Result<()> {
        crate::genesis::map_records(
            &self.records_file_in,
            &self.records_file_out,
            self.no_secret,
            &self.secret_file_out,
        )
    }
}

//using tokio::runtime::Runtime directly
//ensuring consistent error handling are also made.
fn new_actix_system(runtime: Runtime) -> actix::SystemRunner {
    let runtime_cell = std::cell::Cell::new(Some(runtime));
    actix::System::with_tokio_rt(|| {
        let r = std::cell::Cell::new(None);
        runtime_cell.swap(&r);
        r.into_inner().unwrap()
    })
}

//MirrorCommand::run is now asynchronous, and it awaits the result of the RunCmd::run method when applicable.
impl MirrorCommand {
    pub async fn run(self) -> Result<()> {
        tracing::warn!(target: "mirror", "the mirror command is not stable, and may be removed or changed arbitrarily at any time");

        match self.subcmd {
            SubCommand::Prepare(r) => r.run(),
            SubCommand::Run(r) => r.run().await,
        }
    }
}
