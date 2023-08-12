use axum::{async_trait, http::StatusCode};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use serde_with::{serde_as, DurationSeconds};
use thiserror::Error;
use time::{serde::iso8601::option as iso8601_option, Duration, OffsetDateTime};

use crate::stores::mem::CycleError;

pub type TaskKey = usize;
pub static DEFAULT_DURATION: Duration = Duration::new(5, 0);

fn default_duration() -> Duration {
    DEFAULT_DURATION
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
    #[serde(with = "iso8601_option", skip_serializing_if = "Option::is_none")]
    pub deadline: Option<OffsetDateTime>,
}

#[derive(Error, Debug)]
pub enum MonitorError {
    #[error("Monitoring channel dropped")]
    ChannelDropped,
    #[error("Recevied a non executing task id for a timeout or complete signal: {}", .0)]
    InvalidTask(TaskKey),
    #[error("Could not cancel the timeout for task: {}", .0)]
    CancelTimeout(TaskKey),
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
pub enum CompleteError {
    #[error("Invalid task id to be completed: {}", .0)]
    InvalidTaskId(TaskKey),
    #[error("Communication with the store monitor failed")]
    MonitorCommunication,
}

#[derive(Error, Debug)]
pub enum PopError {
    #[error("Invalid task id to be popped: {}", .0)]
    InvalidTaskId(TaskKey),
    #[error("Communication with the store monitor failed")]
    MonitorCommunication,
}

impl PopError {
    pub fn status(&self) -> StatusCode {
        match self {
            PopError::InvalidTaskId(_) => StatusCode::BAD_REQUEST,
            PopError::MonitorCommunication => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}

#[async_trait]
pub trait Store: Send + Sync {
    async fn monitor(&self) -> Result<(), MonitorError>;
    async fn push(&self, insert_task: InsertTask) -> Result<Task, PushError>;
    async fn complete(&self, task_id: TaskKey) -> Result<(), CompleteError>;
    async fn pop(&self) -> Result<Task, PopError>;
}
