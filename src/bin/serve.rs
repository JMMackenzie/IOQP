use axum::{
    extract::Query,
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use std::num::NonZeroUsize;
use std::sync::Arc;
use structopt::StructOpt;
use tracing::info;

#[derive(StructOpt, Debug)]
#[structopt(name = "query", about = "serve ioqp indexes")]
struct Args {
    /// Path to ioqp input file
    #[structopt(short, long, parse(from_os_str))]
    index: std::path::PathBuf,
    /// Port to bind
    #[structopt(long, default_value = "3000")]
    port: u16,
    // /// Worker threads
    // #[structopt(long, default_value = "4")]
    // worker_threads: u16,
    /// Max blocking threads
    #[structopt(long, default_value = "8")]
    max_blocking_threads: u16,
}

#[derive(serde::Deserialize)]
enum QueryMode {
    Fraction(f32),
    Fixed(i64),
}

#[derive(serde::Deserialize)]
struct QueryPayLoad {
    query: ioqp::query::Query,
    k: NonZeroUsize,
    query_mode: QueryMode,
}

enum ServeError {
    JoinWorkerError,
}

impl IntoResponse for ServeError {
    fn into_response(self) -> Response {
        let (status, error_message) = match self {
            ServeError::JoinWorkerError => (
                StatusCode::INTERNAL_SERVER_ERROR,
                "Join worker thread error",
            ),
        };
        let body = Json(serde_json::json!({
            "error": error_message,
        }));
        (status, body).into_response()
    }
}
type IndexType = ioqp::Index<ioqp::SimdBPandStreamVbyte>;


// #[tokio::main(flavor = "current_thread")]
// async fn main() -> anyhow::Result<()> {
fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    let args = Args::from_args();
    info!("args = {:?}", &args);

    info!("loading index from file {}", args.index.display());
    let index = IndexType::read_from_file(args.index)?;
    let index = Arc::new(index);
    let app = Router::new()
        .route(
            "/search",
            post({
                let index = Arc::clone(&index);
                move |body| search_post(body, Arc::clone(&index))
            }),
        )
        .route(
            "/search",
            get({
                let index = Arc::clone(&index);
                move |path| search(path, Arc::clone(&index))
            }),
        );

    let addr = format!("0.0.0.0:{}", args.port).parse()?;
    info!("start http endpoint at {}", &addr);
    // axum::Server::bind(&addr)
    //     .serve(app.into_make_service())
    //     .await?;
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        // .worker_threads(args.worker_threads.into())
        .max_blocking_threads(args.max_blocking_threads.into())
        .build()
        .unwrap()
        .block_on(async {
            axum::Server::bind(&addr)
                .serve(app.into_make_service())
                .await
                .map_err(axum::Error::new)?;
            Result::<(), axum::Error>::Ok(())
        })?;

    Ok(())
}

async fn search_post(
    Json(query): Json<QueryPayLoad>,
    index: Arc<IndexType>,
) -> Result<Json<ioqp::Results>, ServeError> {
    let result = tokio::task::spawn_blocking(move || match query.query_mode {
        QueryMode::Fraction(rho) => {
            index.query_fraction(&query.query.tokens, rho, None, query.k.get())
        }
        QueryMode::Fixed(postings_budget) => {
            index.query_fixed(&query.query.tokens, postings_budget, None, query.k.get())
        }
    })
    .await
    .map_err(|_| ServeError::JoinWorkerError)?;

    Ok(Json(result))
}

async fn search(
    query: Query<QueryPayLoad>,
    index: Arc<IndexType>,
) -> Result<Json<ioqp::Results>, ServeError> {
    let query: QueryPayLoad = query.0;
    let result = tokio::task::spawn_blocking(move || match query.query_mode {
        QueryMode::Fraction(rho) => {
            index.query_fraction(&query.query.tokens, rho, None, query.k.get())
        }
        QueryMode::Fixed(postings_budget) => {
            index.query_fixed(&query.query.tokens, postings_budget, None, query.k.get())
        }
    })
    .await
    .map_err(|_| ServeError::JoinWorkerError)?;

    Ok(Json(result))
}
