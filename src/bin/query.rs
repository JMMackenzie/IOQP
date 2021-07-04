use structopt::StructOpt;
use tracing::*;
use tracing_subscriber::fmt::format::FmtSpan;

use ioqp;

#[derive(StructOpt, Debug)]
#[structopt(name = "query", about = "query ioqp indexes")]
struct Args {
    /// Path to ioqp input file
    #[structopt(short, long, parse(from_os_str))]
    index: std::path::PathBuf,
    /// Path to query file
    #[structopt(short, long, parse(from_os_str))]
    queries: std::path::PathBuf,
}

fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_target(false)
        .with_span_events(FmtSpan::ENTER | FmtSpan::EXIT)
        .with_timer(tracing_subscriber::fmt::time::uptime())
        .with_level(true)
        .init();

    let args = Args::from_args();
    info!(args = ?args);

    let index = ioqp::Index::read_from_file(args.index)?;

    let mut searcher = index.searcher();

    let qry = vec!["who", "is", "nicola", "tesla"];
    searcher.query_rho(&qry, 0.1);

    Ok(())
}
