use structopt::StructOpt;
use tracing::*;

#[derive(serde::Serialize, Debug, Clone, Copy)]
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
    #[structopt(short, long, default_value = "http://localhost:3000/search")]
    endpoint: url::Url,
    /// Path to query file
    #[structopt(short, long, parse(from_os_str))]
    queries: std::path::PathBuf,
    /// Query mode
    #[structopt(short, long)]
    mode: QueryMode,
    /// Top-k depth
    #[structopt(short, long, default_value = "10")]
    k: std::num::NonZeroUsize,
    /// Whether or not to obey query weights
    #[structopt(long, default_value = "300")]
    duration_secs: std::num::NonZeroUsize,
    /// Whether or not to obey query weights
    #[structopt(long)]
    tps: std::num::NonZeroU32,
    /// Whether or not to obey query weights
    #[structopt(long)]
    weighted: bool,
}

#[derive(serde::Serialize, Clone)]
struct QueryPayLoad {
    query: ioqp::query::Query,
    k: std::num::NonZeroUsize,
    query_mode: QueryMode,
}

async fn process_query(
    http_client: reqwest::Client,
    end_point: &url::Url,
    query: QueryPayLoad,
) -> Result<ioqp::Results, reqwest::Error> {
    http_client
        .post(end_point.as_str())
        .json(&query)
        .send()
        .await?
        .json::<ioqp::Results>()
        .await
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    let args = Args::from_args();
    info!("args = {:?}", &args);

    let http_client = reqwest::Client::builder().build()?;

    let qrys = ioqp::query::read_queries(args.queries, args.weighted)?;
    info!("read queries = {}", qrys.len());

    // transform to objects we will serialize later
    let k = args.k;
    let query_mode = args.mode;
    let qrys: Vec<QueryPayLoad> = qrys
        .into_iter()
        .map(|query| QueryPayLoad {
            query,
            k,
            query_mode,
        })
        .collect();

    let total_req_duration = std::time::Duration::from_secs(args.duration_secs.get() as u64);
    let queries = qrys.iter().cycle();

    let query_stream = futures::stream::iter(queries);
    use futures::StreamExt;
    use governor::state::direct::StreamRateLimitExt;
    use governor::{Quota, RateLimiter};

    let end_point = &args.endpoint;
    let rate_limitter = std::sync::Arc::new(RateLimiter::direct(Quota::per_second(args.tps)));
    let rate_limited_queries = query_stream.ratelimit_stream(&rate_limitter);
    let rate_limited_requests = rate_limited_queries
        .map(|query| process_query(http_client.clone(), end_point, query.clone()));
    let mut buffered_rate_limited_qrys = rate_limited_requests.buffered(args.tps.get() as usize);

    let mut num_errors: usize = 0;
    let mut num_processed: usize = 0;
    let mut hist = Vec::new();
    let limit = total_req_duration.as_secs() * args.tps.get() as u64;
    let pb = indicatif::ProgressBar::new(limit);
    pb.set_draw_delta(total_req_duration.as_secs() as u64 / 200);
    pb.set_style(indicatif::ProgressStyle::default_bar().template(&format!(
        "{}: {}",
        "running query load",
        "{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] ({pos}/{len}, ETA {eta}, TPS: {per_sec}, {msg})"
    )));
    pb.set_message(format!("ERRORS: {}", num_errors));
    let start = std::time::Instant::now();
    while let Some(qry_resp) = buffered_rate_limited_qrys.next().await {
        match qry_resp {
            Ok(results) => {
                if num_processed > 10000 {
                    // we ignore the first 10k for warmup
                    hist.push(results.took.as_micros());
                }
            }
            Err(err) => {
                num_errors += 1;
                pb.set_message(format!("ERRORS: {}", num_errors));
                if num_errors > 10000 {
                    tracing::error!("Too many errors! stopping...");
                    break;
                }
                error!("Error processing request: {}", err);
            }
        }
        num_processed += 1;
        pb.inc(1);
        if start.elapsed() > total_req_duration {
            break;
        }
    }
    pb.finish_and_clear();

    hist.sort_unstable();
    let n = hist.len() as f32;
    let total_time = hist.iter().sum::<u128>();
    info!("# of samples: {}", hist.len());
    info!("  50'th percntl.: {}µs", hist[(n * 0.5) as usize]);
    info!("  90'th percntl.: {}µs", hist[(n * 0.9) as usize]);
    info!("  99'th percntl.: {}µs", hist[(n * 0.99) as usize]);
    info!("99.9'th percntl.: {}µs", hist[(n * 0.999) as usize]);
    info!("            max.: {}µs", hist.last().unwrap());
    info!("       mean time: {:.1}µs", total_time as f32 / n);

    Ok(())
}
