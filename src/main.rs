mod api;
mod store;
mod stores;

use futures::{try_join, TryFutureExt};
use std::sync::Arc;

use axum::{
    extract::State,
    http::StatusCode,
    routing::{get, post, put},
    Router,
};
use block_id::{Alphabet, BlockId};
use eyre::{eyre, Report, Result};
use tracing_subscriber::{
    filter::{EnvFilter, LevelFilter},
    fmt,
    prelude::*,
};

use api::{ApiError, Json};
use store::{Conceal, Store, KEY_GENERATOR};
use stores::mem::MemoryStore;
use structures::{CompleteTask, Execution, InsertTask, Task};

static DEFAULT_KEY_SEED: u128 = 220232566797978763445376627431768261475;
static DEFAULT_KEY_MIN_LENGTH: u8 = 4;

type Context = Arc<dyn Store>;

async fn push(
    State(context): State<Context>,
    Json(task): Json<InsertTask>,
) -> Result<(StatusCode, Json<Task>), ApiError> {
    let task = context.push(task.try_into()?).await?;
    tracing::info!(id = ?task.0.id, name = %task.0.name, "Queued task");
    Ok((StatusCode::OK, Json(task.conceal()?)))
}

async fn pop(State(context): State<Context>) -> Result<(StatusCode, Json<Execution>), ApiError> {
    let execution = context.pop().await?;
    tracing::info!(id = ?execution.0.task.0.id, name = %execution.0.task.0.name, deadline = %execution.0.deadline, "Dequeued task");
    Ok((StatusCode::OK, Json(execution.conceal()?)))
}

#[axum_macros::debug_handler]
async fn complete(
    State(context): State<Context>,
    Json(CompleteTask { id }): Json<CompleteTask>,
) -> Result<StatusCode, ApiError> {
    let id = id.try_into()?;
    context.complete(id).await?;
    tracing::info!(?id, "Task completed");
    Ok(StatusCode::OK)
}

#[tokio::main]
async fn main() -> Result<()> {
    let tracing_builder = tracing_subscriber::registry().with(fmt::layer());
    if std::env::var(EnvFilter::DEFAULT_ENV).is_ok() {
        tracing_builder.with(EnvFilter::from_default_env())
    } else {
        tracing_builder.with(EnvFilter::default().add_directive(LevelFilter::INFO.into()))
    }
    .init();

    let seed = std::env::var("KEY_SEED").map_or(Ok(DEFAULT_KEY_SEED), |s| s.parse())?;
    let min_length =
        std::env::var("KEY_MIN_LENGTH").map_or(Ok(DEFAULT_KEY_MIN_LENGTH), |s| s.parse())?;
    KEY_GENERATOR
        .set(BlockId::new(Alphabet::alphanumeric(), seed, min_length))
        .map_err(|_| eyre!("OnceCell was already full"))?;

    let state: Context = Arc::new(MemoryStore::new());
    let app = Router::new()
        .route("/v1/push", put(push))
        .route("/v1/pop", get(pop))
        .route("/v1/complete", post(complete))
        .with_state(state.clone());

    let monitor_task = tokio::spawn(async move {
        tracing::info!("Task monitor running");
        state.monitor().await
    });

    let address_str = std::env::var("LISTEN_ADDRESS")
        .ok()
        .unwrap_or("0.0.0.0:3000".to_string());
    let address = address_str.parse()?;
    tracing::info!(%address, "Taskie listening");
    let http_task = axum::Server::bind(&address).serve(app.into_make_service());

    try_join!(
        monitor_task.map_err(|e| Into::<Report>::into(e)),
        http_task.map_err(|e| e.into())
    )?
    .0?;
    Ok(())
}
