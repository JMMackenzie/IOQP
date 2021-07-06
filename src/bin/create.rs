use structopt::StructOpt;

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
    let args = Args::from_args();

    let index = ioqp::Index::from_ciff_file(args.input)?;

    index.write_to_file(args.output)
}
