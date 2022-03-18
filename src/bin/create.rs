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
    /// Do indexes require quantization
    #[structopt(long)]
    quantize: bool,
    /// BM25 k1 parameter
    #[structopt(long, default_value = "0.9")]
    bm25_k1: f32,
    /// BM25 b parameter
    #[structopt(long, default_value = "0.4")]
    bm25_b: f32,
}

fn main() -> anyhow::Result<()> {
    let args = Args::from_args();

    let index = if args.quantize {
        ioqp::Index::<ioqp::SimdBPandStreamVbyte>::quantize_from_ciff_file(args.input, args.bm25_k1, args.bm25_b)
    } else {
         ioqp::Index::<ioqp::SimdBPandStreamVbyte>::from_ciff_file(args.input)
    }?;
    index.write_to_file(args.output)
}
