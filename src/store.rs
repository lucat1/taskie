use std::fmt;

use axum::{async_trait, http::StatusCode};
use block_id::BlockId;
use once_cell::sync::OnceCell;
use thiserror::Error;

use crate::stores::mem::CycleError;

pub static KEY_GENERATOR: OnceCell<BlockId<char>> = OnceCell::new();

#[derive(Error, Debug)]
pub enum ConcealError {
    #[error("Missing key decoder/generator")]
    MissingGenerator,
    #[error("Encoding error")]
    InvalidKey,
}

impl ConcealError {
    pub fn status(&self) -> StatusCode {
        match self {
            ConcealError::MissingGenerator => StatusCode::INTERNAL_SERVER_ERROR,
            ConcealError::InvalidKey => StatusCode::BAD_REQUEST,
        }
    }
}

pub trait Conceal {
    type Concealed;

    fn conceal(self) -> Result<Self::Concealed, ConcealError>;
}

#[derive(Error, Debug)]
pub enum KeyDecodeError {
    #[error("Missing key decoder/generator")]
    MissingGenerator,
    #[error("Invalid key: {}", .0)]
    InvalidKey(String),
}

impl KeyDecodeError {
    pub fn status(&self) -> StatusCode {
        match self {
            KeyDecodeError::MissingGenerator => StatusCode::INTERNAL_SERVER_ERROR,
            KeyDecodeError::InvalidKey(_) => StatusCode::BAD_REQUEST,
        }
    }
}

#[derive(Clone, Copy, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct TaskKey(pub u64);

impl TryFrom<taskie_structures::TaskKey> for TaskKey {
    type Error = KeyDecodeError;

    fn try_from(value: taskie_structures::TaskKey) -> Result<Self, Self::Error> {
        KEY_GENERATOR
            .get()
            .ok_or(KeyDecodeError::MissingGenerator)?
            .decode_string(&value)
            .map(TaskKey)
            .ok_or(KeyDecodeError::InvalidKey(value))
    }
}

impl Conceal for TaskKey {
    type Concealed = String;

    fn conceal(self) -> Result<Self::Concealed, ConcealError> {
        KEY_GENERATOR
            .get()
            .ok_or(ConcealError::MissingGenerator)?
            .encode_string(self.0)
            .ok_or(ConcealError::InvalidKey)
    }
}

impl fmt::Display for TaskKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}",
            self.conceal().unwrap_or("broken concealer".to_string())
        )
    }
}

impl fmt::Debug for TaskKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}({})",
            self.0,
            self.conceal().unwrap_or("broken concealer".to_string())
        )
    }
}

pub struct InsertTask(pub taskie_structures::InsertTask<TaskKey>);

impl TryFrom<taskie_structures::InsertTask> for InsertTask {
    type Error = KeyDecodeError;

    fn try_from(value: taskie_structures::InsertTask) -> Result<Self, Self::Error> {
        Ok(Self(taskie_structures::InsertTask {
            name: value.name,
            payload: value.payload,
            duration: value.duration,
            depends_on: value
                .depends_on
                .into_iter()
                .map(|k| k.try_into())
                .collect::<Result<Vec<TaskKey>, KeyDecodeError>>()?,
        }))
    }
}

#[derive(Clone)]
pub struct Task(pub taskie_structures::Task<TaskKey>);

impl Conceal for Task {
    type Concealed = taskie_structures::Task;

    fn conceal(self) -> Result<Self::Concealed, ConcealError> {
        let Task(task) = self;
        Ok(taskie_structures::Task {
            id: task.id.conceal()?,
            depends_on: task
                .depends_on
                .into_iter()
                .map(|k| k.conceal())
                .collect::<Result<Vec<taskie_structures::TaskKey>, ConcealError>>()?,
            name: task.name,
            duration: task.duration,
            payload: task.payload,
        })
    }
}

pub struct Execution(pub taskie_structures::Execution<Task>);

impl Conceal for Execution {
    type Concealed = taskie_structures::Execution;

    fn conceal(self) -> Result<Self::Concealed, ConcealError> {
        let Execution(execution) = self;
        Ok(taskie_structures::Execution {
            task: execution.task.conceal()?,
            deadline: execution.deadline,
        })
    }
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
    #[error("Missing task to depend upon: {dependency}; it could be either non-existant or already finished")]
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
    async fn pop(&self) -> Result<Execution, PopError>;
}
