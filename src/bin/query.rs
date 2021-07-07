use std::io::BufRead;

use indicatif::ProgressIterator;
use structopt::StructOpt;

use ioqp;

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
                if rho >= 0.0 && rho <= 1.0 {
                    Ok(QueryMode::Fraction(rho))
                } else{
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

    let index = ioqp::Index::<ioqp::SimdBPandStreamVbyte>::read_from_file(args.index)?;

    let mut searcher = index.searcher();
    let num_queries = match args.num_queries {
        Some(num_queries) => num_queries,
        None => qrys.len(),
    };
    let mut hist = Vec::with_capacity(num_queries);
    let pb = ioqp::util::progress_bar("process_queries", num_queries);
    match args.mode {
        QueryMode::Fraction(rho) => {
            for qry in qrys.iter().cycle().take(num_queries).progress_with(pb) {
                let result = searcher.query_fraction(&qry.tokens, rho, usize::from(args.k));
                hist.push(result.took.as_micros() as u64);
            }
        }
        QueryMode::Fixed(budget) => {
            for qry in qrys.iter().cycle().take(num_queries).progress_with(pb) {
                let result = searcher.query_fixed(&qry.tokens, budget as i64, usize::from(args.k));
                hist.push(result.took.as_micros() as u64);
            }
        }
    }

    hist.sort();
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
