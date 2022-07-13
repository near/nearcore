use anyhow::Context;
use clap::Parser;
use mirror::TxMirror;
use std::cell::Cell;
use std::path::PathBuf;

#[derive(Parser)]
struct Cli {
    #[clap(subcommand)]
    subcmd: SubCommand,
}

#[derive(Parser)]
enum SubCommand {
    Prepare(PrepareCmd),
    Run(RunCmd),
}

#[derive(Parser)]
struct RunCmd {
    #[clap(long)]
    source_home: PathBuf,
    #[clap(long)]
    target_home: PathBuf,
    #[clap(long)]
    secret_file: PathBuf,
}

impl RunCmd {
    fn run(self, runtime: tokio::runtime::Runtime) -> anyhow::Result<()> {
        openssl_probe::init_ssl_cert_env_vars();

        let system = new_actix_system(runtime);
        let res: anyhow::Result<()> = system.block_on(async move {
            let m = TxMirror::new(&self.source_home, &self.target_home, &self.secret_file)?;
            actix::spawn(m.run());
            Ok(())
        });
        res.context("failed to start main loop future")?;
        system.run()?;
        Ok(())
    }
}

#[derive(Parser)]
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
    fn run(self) -> anyhow::Result<()> {
        mirror::genesis::map_records(
            &self.records_file_in,
            &self.records_file_out,
            self.no_secret,
            &self.secret_file_out,
        )
    }
}

// copied from neard/src/cli.rs
fn new_actix_system(runtime: tokio::runtime::Runtime) -> actix::SystemRunner {
    // `with_tokio_rt()` accepts an `Fn()->Runtime`, however we know that this function is called exactly once.
    // This makes it safe to move out of the captured variable `runtime`, which is done by a trick
    // using a `swap` of `Cell<Option<Runtime>>`s.
    let runtime_cell = Cell::new(Some(runtime));
    actix::System::with_tokio_rt(|| {
        let r = Cell::new(None);
        runtime_cell.swap(&r);
        r.into_inner().unwrap()
    })
}

fn main() -> anyhow::Result<()> {
    let args = Cli::parse();

    let runtime = tokio::runtime::Runtime::new().context("failed to start tokio runtime")?;
    let _subscriber = runtime.block_on(async {
        near_o11y::default_subscriber(
            near_o11y::EnvFilterBuilder::from_env().finish().unwrap(),
            &Default::default(),
        )
        .await
        .global()
    });

    match args.subcmd {
        SubCommand::Prepare(r) => r.run(),
        SubCommand::Run(r) => r.run(runtime),
    }
}
