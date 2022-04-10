use structopt::StructOpt;

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
    /// Number of bits to use for index quantization
    #[structopt(short, long, default_value = "8")]
    quant_bits: u32,
}

fn main() -> anyhow::Result<()> {
    let args = Args::from_args();

    let index = if args.quantize {
        let scorer = ioqp::score::BM25::new(args.bm25_k1, args.bm25_b);
        ioqp::Index::<ioqp::SimdBPandStreamVbyte>::from_ciff_file(
            args.input,
            args.quant_bits,
            scorer,
        )
    } else {
        let scorer = ioqp::score::Identity::new();
        ioqp::Index::<ioqp::SimdBPandStreamVbyte>::from_ciff_file(
            args.input,
            args.quant_bits,
            scorer,
        )
    }?;

    index.write_to_file(args.output)?;

    Ok(())
}
