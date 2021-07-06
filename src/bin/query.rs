use std::io::BufRead;

use indicatif::ProgressIterator;
use structopt::StructOpt;

use ioqp;

#[derive(Debug, parse_display::Display, parse_display::FromStr)]
#[display("{}-{0}")]
#[display(style = "snake_case")]
enum QueryMode {
    Rho(f32),
    Budget(u64),
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
}

pub struct Query {
    pub id: usize,
    pub tokens: Vec<String>,
}

impl std::str::FromStr for Query {
    type Err = std::num::ParseIntError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let parts: Vec<&str> = s.split(':').collect();
        let id = parts[0].parse::<usize>()?;
        let tokens: Vec<String> = parts[1].split_whitespace().map(|s| s.to_owned()).collect();
        Ok(Query { id, tokens })
    }
}

pub fn read_queries<P: AsRef<std::path::Path> + std::fmt::Debug>(
    qry_file: P,
) -> anyhow::Result<Vec<Query>> {
    let qry_file = std::fs::File::open(qry_file)?;
    let qry_file = std::io::BufReader::new(qry_file);
    let queries = qry_file
        .lines()
        .filter_map(|l| l.ok())
        .filter_map(|l| l.parse::<Query>().ok())
        .collect();
    Ok(queries)
}

fn main() -> anyhow::Result<()> {
    let args = Args::from_args();

    let qrys = read_queries(args.queries)?;

    let index = ioqp::Index::read_from_file(args.index)?;

    let mut searcher = index.searcher();
    use hdrhistogram::Histogram;
    let mut hist = Histogram::<u64>::new_with_bounds(1, 10 * 1000 * 1000, 2).unwrap();
    let progress = ioqp::util::progress_bar("process_queries", qrys.len());
    match args.mode {
        QueryMode::Rho(rho) => {
            for qry in qrys.iter().progress_with(progress) {
                let result = searcher.query_rho(&qry.tokens, rho, usize::from(args.k));
                hist += result.took.as_micros() as u64;
            }
        }
        QueryMode::Budget(budget) => {
            for qry in qrys.iter().progress_with(progress) {
                let result = searcher.query_budget(&qry.tokens, budget as i64, usize::from(args.k));
                hist += result.took.as_micros() as u64;
            }
        }
    }

    println!("# of samples: {}", hist.len());
    println!("  50'th percntl.: {}µs", hist.value_at_quantile(0.50));
    println!("  90'th percntl.: {}µs", hist.value_at_quantile(0.90));
    println!("  99'th percntl.: {}µs", hist.value_at_quantile(0.99));
    println!("99.9'th percntl.: {}µs", hist.value_at_quantile(0.999));
    println!("mean time: {:.1}µs", hist.mean());

    Ok(())
}
