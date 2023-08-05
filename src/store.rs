use axum::{async_trait, http::StatusCode};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use serde_with::{serde_as, DurationSeconds};
use thiserror::Error;
use time::Duration;

use crate::stores::mem::CycleError;

pub type TaskKey = usize;

fn default_duration() -> Duration {
    Duration::new(60, 0)
}

#[serde_as]
#[derive(Deserialize)]
pub struct InsertTask {
    pub name: String,
    pub payload: Option<Value>,
    #[serde(default = "Vec::new")]
    pub depends_on: Vec<TaskKey>,
    #[serde_as(as = "DurationSeconds<i64>")]
    #[serde(default = "default_duration")]
    pub duration: Duration,
}

#[serde_as]
#[derive(Clone, Serialize)]
pub struct Task {
    pub id: TaskKey,
    pub name: String,
    pub payload: Option<Value>,
    pub depends_on: Vec<TaskKey>,
    #[serde_as(as = "DurationSeconds<i64>")]
    pub duration: Duration,
}

#[derive(Error, Debug)]
pub enum PushError {
    #[error("Missing task to depend upon: {dependency:?}; it could be either non-existant or already finished")]
    MissingDependency { dependency: TaskKey },
    #[error("Adding a task with the given dependencies would create a dependency cycle")]
    Cycle(#[from] CycleError),
}

impl PushError {
    pub fn status(&self) -> StatusCode {
        match self {
            PushError::MissingDependency { .. } => StatusCode::BAD_REQUEST,
            PushError::Cycle(_) => StatusCode::BAD_REQUEST,
        }
    }
}

#[derive(Error, Debug)]
pub enum PopError {
    #[error("Invalid task id to be popped")]
    InvalidID,
}

#[async_trait]
pub trait Store: Send + Sync {
    async fn push(&mut self, insert_task: InsertTask) -> Result<Task, PushError>;
    async fn pop(&mut self) -> Result<Task, PopError>;
}
