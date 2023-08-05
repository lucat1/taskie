mod api;
mod store;
mod stores;

use std::sync::Arc;

use axum::{extract::State, http::StatusCode, routing::post, Router};
use eyre::Result;
use tokio::sync::RwLock;

use api::{ApiError, Json};
use store::{InsertTask, Store, Task};
use stores::mem::MemoryStore;

type Context = Arc<RwLock<dyn Store>>;

#[axum_macros::debug_handler]
async fn push(
    State(context): State<Context>,
    Json(task): Json<InsertTask>,
) -> Result<(StatusCode, Json<Task>), ApiError> {
    let name = task.name.to_owned();
    let mut context = context.write().await;
    let task = context.push(task).await?;
    tracing::info!(id = %task.id, %name, "Queued task");
    Ok((StatusCode::OK, Json(task)))
}

#[tokio::main]
async fn main() -> Result<()> {
    // construct a subscriber that prints formatted traces to stdout
    let subscriber = tracing_subscriber::FmtSubscriber::new();
    // use that subscriber to process traces emitted after this point
    tracing::subscriber::set_global_default(subscriber)?;

    let app = Router::new()
        .route("/v1/push", post(push))
        .with_state(Arc::new(RwLock::new(MemoryStore::new())) as Context);

    let address_str = std::env::var("LISTEN_ADDRESS")
        .ok()
        .unwrap_or("0.0.0.0:3000".to_string());
    let address = address_str.parse()?;
    tracing::info!(%address, "Taskie listening");
    Ok(axum::Server::bind(&address)
        .serve(app.into_make_service())
        .await?)
}
