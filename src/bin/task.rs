use structopt::StructOpt;

#[derive(StructOpt, Debug)]
#[structopt(name = "query", about = "query ioqp indexes")]
struct Args {
    /// Path to ioqp input file
    #[structopt(short, long, parse(from_os_str))]
    index: std::path::PathBuf,
    /// Whether to read the index or not
    #[structopt(short, long)]
    with_index: bool,
}

// A block of work
pub fn do_work() -> i32 {
    let mut x = 0;
    for _ in 0..10000000 {
        x = 2 * x + 4 - 3 * x + 2 - 5 * x + 5;
    }
    x as i32
}

fn main() -> anyhow::Result<()> {
    //let (_non_blocking, _guard) = tracing_appender::non_blocking(std::io::stdout());

    let args = Args::from_args();

    // Toggle this on/off to observe behavior changes
    if args.with_index {
        let _index = ioqp::Index::<ioqp::SimdBPandStreamVbyte>::read_from_file(&args.index)?;
        let _index_2 = ioqp::Index::<ioqp::SimdBPandStreamVbyte>::read_from_file(&args.index)?;
        let _index_3 = ioqp::Index::<ioqp::SimdBPandStreamVbyte>::read_from_file(&args.index)?;
    }

    // Some constant number of runs
    let upper_bound = 100000;

    let mut hist = Vec::with_capacity(upper_bound);

    let mut x = 0;
    for _i in 0..upper_bound {
        let start = std::time::Instant::now();
        let y = do_work();
        let elapsed = start.elapsed().as_micros() as u64;
        hist.push(elapsed);
        // Let 5ms be an abitrary "slow" time -- platform specific
        x += y;
    }

    hist.sort_unstable();
    let n = hist.len() as f32;
    let total_time = hist.iter().sum::<u64>();
    println!("# of samples: {}", hist.len());
    println!("  50'th percntl.: {}µs", hist[(n * 0.5) as usize]);
    println!("  90'th percntl.: {}µs", hist[(n * 0.9) as usize]);
    println!("  99'th percntl.: {}µs", hist[(n * 0.99) as usize]);
    println!("99.9'th percntl.: {}µs", hist[(n * 0.999) as usize]);
    println!("            max.: {}µs", hist.last().unwrap());
    println!("       mean time: {:.1}µs", total_time as f32 / n);

    println!("Dummy: {}", x);

    Ok(())
}
