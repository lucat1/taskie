mod api;
mod store;
mod stores;

use std::sync::Arc;

use axum::{extract::State, http::StatusCode, routing::post, Router};
use eyre::Result;
use tracing_subscriber::{
    filter::{EnvFilter, LevelFilter},
    fmt,
    prelude::*,
};

use api::{ApiError, Json};
use store::{InsertTask, Store, Task};
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

#[axum_macros::debug_handler]
async fn pop(State(context): State<Context>) -> Result<(StatusCode, Json<Task>), ApiError> {
    let task = context.pop().await?;
    tracing::info!(id = %task.id, name = %task.name, "Dequeued task");
    Ok((StatusCode::OK, Json(task)))
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

    let app = Router::new()
        .route("/v1/push", post(push))
        .route("/v1/pop", post(pop))
        .with_state(Arc::new(MemoryStore::new()) as Context);

    let address_str = std::env::var("LISTEN_ADDRESS")
        .ok()
        .unwrap_or("0.0.0.0:3000".to_string());
    let address = address_str.parse()?;
    tracing::info!(%address, "Taskie listening");
    Ok(axum::Server::bind(&address)
        .serve(app.into_make_service())
        .await?)
}
