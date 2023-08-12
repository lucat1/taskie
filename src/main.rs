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
use eyre::{Report, Result};
use serde::Deserialize;
use tracing_subscriber::{
    filter::{EnvFilter, LevelFilter},
    fmt,
    prelude::*,
};

use api::{ApiError, Json};
use store::{InsertTask, Store, Task, TaskKey};
use stores::mem::MemoryStore;

type Context = Arc<dyn Store>;

async fn push(
    State(context): State<Context>,
    Json(task): Json<InsertTask>,
) -> Result<(StatusCode, Json<Task>), ApiError> {
    let name = task.name.to_owned();
    let task = context.push(task).await?;
    tracing::info!(id = %task.id, %name, "Queued task");
    Ok((StatusCode::OK, Json(task)))
}

async fn pop(State(context): State<Context>) -> Result<(StatusCode, Json<Task>), ApiError> {
    let task = context.pop().await?;
    tracing::info!(id = %task.id, name = %task.name, "Dequeued task");
    Ok((StatusCode::OK, Json(task)))
}

#[derive(Deserialize)]
struct CompletePayload {
    id: TaskKey,
}

#[axum_macros::debug_handler]
async fn complete(
    State(context): State<Context>,
    Json(CompletePayload { id }): Json<CompletePayload>,
) -> Result<StatusCode, ApiError> {
    context.complete(id).await?;
    tracing::info!(%id, "Task completed");
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
