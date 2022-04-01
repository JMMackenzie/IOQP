use indicatif::ProgressIterator;
use structopt::StructOpt;

#[derive(Debug)]
enum QueryMode {
    Fraction(f32),
    Fixed(u64),
}

impl std::str::FromStr for QueryMode {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let parts: Vec<&str> = s.split('-').collect();
        if parts.len() != 2 {
            return Err(anyhow::anyhow!("invalid query mode"));
        }
        match parts[0] {
            "fraction" => {
                let rho = parts[1].parse::<f32>()?;
                if (0.0..=1.0).contains(&rho) {
                    Ok(QueryMode::Fraction(rho))
                } else {
                    Err(anyhow::anyhow!("Rho must be in range [0.0, 1.0]"))
                }
            }
            "fixed" => {
                let budget = parts[1].parse::<u64>()?;
                Ok(QueryMode::Fixed(budget))
            }
            _ => Err(anyhow::anyhow!("invalid query mode")),
        }
    }
}

#[derive(StructOpt, Debug)]
#[structopt(name = "query", about = "query ioqp indexes")]
struct Args {
    /// Path to ioqp input file
    #[structopt(short, long, parse(from_os_str))]
    index: std::path::PathBuf,
    /// Path to query file
    #[structopt(short, long, parse(from_os_str))]
    queries: std::path::PathBuf,
    /// Query mode
    #[structopt(short, long)]
    mode: QueryMode,
    /// Top-k depth
    #[structopt(short, long, default_value = "10")]
    k: std::num::NonZeroUsize,
    /// num_queries to run
    #[structopt(short, long)]
    num_queries: Option<usize>,
    /// trec output file
    #[structopt(short, long)]
    output_file: std::path::PathBuf,
    /// Run warmup round
    #[structopt(long)]
    warmup: bool,
    /// Number of warmup queries
    #[structopt(long)]
    warmup_num: Option<usize>,
    /// Whether or not to obey query weights
    #[structopt(long)]
    weighted: bool,
}

fn main() -> anyhow::Result<()> {
    let args = Args::from_args();

    let qrys = ioqp::query::read_queries(args.queries, args.weighted)?;

    let index = ioqp::Index::<ioqp::SimdBPandStreamVbyte>::read_from_file(args.index)?;

    let out_handle = std::fs::File::create(args.output_file).expect("can not open output file");

    let num_queries = match args.num_queries {
        Some(num_queries) => num_queries,
        None => qrys.len(),
    };
    let mut hist = Vec::with_capacity(num_queries);
    let pb = ioqp::util::progress_bar("process_queries", num_queries);
    if args.warmup {
        let warmup_num = match args.warmup_num {
            Some(warmup_num) => warmup_num,
            None => num_queries,
        };
        let pb_warmup = ioqp::util::progress_bar("warmup queries", warmup_num);
        let devnull = std::fs::File::create("/dev/null").expect("can not open /dev/null");
        for qry in qrys
            .iter()
            .cycle()
            .take(warmup_num)
            .progress_with(pb_warmup)
        {
            // fixed budget postings and `k`
            let result = index.query_fixed(&qry.tokens, 10, Some(qry.id), 10);
            result.to_trec_file(index.docmap(), &devnull);
        }
    }
    match args.mode {
        QueryMode::Fraction(rho) => {
            pb.reset();
            for qry in qrys.iter().cycle().take(num_queries).progress_with(pb) {
                let result =
                    index.query_fraction(&qry.tokens, rho, Some(qry.id), usize::from(args.k));
                hist.push(result.took.as_micros() as u64);
                result.to_trec_file(index.docmap(), &out_handle);
            }
        }
        QueryMode::Fixed(budget) => {
            pb.reset();
            for qry in qrys.iter().cycle().take(num_queries).progress_with(pb) {
                let result = index.query_fixed(
                    &qry.tokens,
                    budget as i64,
                    Some(qry.id),
                    usize::from(args.k),
                );
                hist.push(result.took.as_micros() as u64);
                result.to_trec_file(index.docmap(), &out_handle);
            }
        }
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

    Ok(())
}
