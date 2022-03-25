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

#[derive(StructOpt, Debug)]
#[structopt(name = "query", about = "serve ioqp indexes")]
struct Args {
    /// Path to ioqp input file
    #[structopt(short, long, parse(from_os_str))]
    index: std::path::PathBuf,
    /// Port to bind
    #[structopt(long)]
    port: u16,
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

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::from_args();

    let index = IndexType::read_from_file(args.index)?;
    let index = Arc::new(index);
    let app = Router::new()
        .route(
            "/search",
            post({
                let index = Arc::clone(&index);
                move |body| saerch_post(body, Arc::clone(&index))
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
    axum::Server::bind(&addr)
        .serve(app.into_make_service())
        .await?;

    Ok(())
}

async fn saerch_post(
    Json(query): Json<QueryPayLoad>,
    index: Arc<IndexType>,
) -> Result<Json<ioqp::SearchResults>, ServeError> {
    let result = tokio::task::spawn_blocking(move || {
        match query.query_mode {
            QueryMode::Fraction(rho) => {
                index.query_fraction(&query.query.tokens, rho, None, query.k.get())
            }
            QueryMode::Fixed(postings_budget) => {
                index.query_fixed(&query.query.tokens, postings_budget, None, query.k.get())
            }
        }
    })
    .await
    .map_err(|_| ServeError::JoinWorkerError)?;

    Ok(Json(result))
}

async fn search(
    query: Query<QueryPayLoad>,
    index: Arc<IndexType>,
) -> Result<Json<ioqp::SearchResults>, ServeError> {
    let query: QueryPayLoad = query.0;
    let result = tokio::task::spawn_blocking(move || {
        match query.query_mode {
            QueryMode::Fraction(rho) => {
                index.query_fraction(&query.query.tokens, rho, None, query.k.get())
            }
            QueryMode::Fixed(postings_budget) => {
                index.query_fixed(&query.query.tokens, postings_budget, None, query.k.get())
            }
        }
    })
    .await
    .map_err(|_| ServeError::JoinWorkerError)?;

    Ok(Json(result))
}
