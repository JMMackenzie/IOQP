use structopt::StructOpt;
use tracing::*;
use tracing_subscriber::fmt::format::FmtSpan;

use ioqp;

#[derive(StructOpt, Debug)]
#[structopt(name = "create", about = "create ioqp indexes")]
struct Args {
    /// Path to ciff input file
    #[structopt(short, long, parse(from_os_str))]
    input: std::path::PathBuf,
    /// Path to ioqp index output file
    #[structopt(short, long, parse(from_os_str))]
    output: std::path::PathBuf,
}

fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_target(false)
        .with_span_events(FmtSpan::ENTER | FmtSpan::CLOSE)
        .with_timer(tracing_subscriber::fmt::time::uptime())
        .with_level(true)
        .init();

    let args = Args::from_args();
    info!(args = ?args);

    let index = ioqp::Index::from_ciff_file(args.input)?;

    index.write_to_file(args.output)
}
