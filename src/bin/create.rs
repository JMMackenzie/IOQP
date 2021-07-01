use structopt::StructOpt;
use tracing::*;
use tracing_bunyan_formatter::*;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::*;

use ioqp;

/// Main command line options for the ukko_broker binary
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
    let formatting_layer = BunyanFormattingLayer::new("IOQP".into(), std::io::stdout);
    let subscriber = Registry::default()
        .with(JsonStorageLayer)
        .with(formatting_layer);
    tracing::subscriber::set_global_default(subscriber)
        .expect("Failed to set tracing log subscriber");

    let args = Args::from_args();
    info!(args = ?args);

    let index = ioqp::Index::from_ciff_file(args.input)?;

    {
        let _span = span!(Level::INFO, "write index to disk");
        let output_file = std::fs::File::create(args.output)?;
        let output_file = std::io::BufWriter::new(output_file);
        bincode::serialize_into(output_file, &index)?;
    }

    Ok(())
}
