mod apply_chain_range;
mod cli;
mod commands;
mod epoch_info;
mod state_dump;

fn main() {
    cli::StateViewerCmd::parse_and_run();
}
