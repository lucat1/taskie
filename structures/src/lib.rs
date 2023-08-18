use serde::{Deserialize, Serialize};
use serde_json::Value;
use serde_with::{serde_as, DurationSeconds};
use time::{serde::iso8601, Duration, OffsetDateTime};

#[derive(Clone, Serialize, Deserialize)]
pub struct Error {
    pub status: u16,
    pub message: String,
}

pub type TaskKey = String;
pub type TaskName = String;
pub static DEFAULT_DURATION: Duration = Duration::new(30, 0);

fn default_duration() -> Duration {
    DEFAULT_DURATION
}

#[serde_as]
#[derive(Clone, Serialize, Deserialize)]
pub struct InsertTask<N = TaskName, K = TaskKey> {
    pub name: N,
    pub payload: Option<Value>,
    #[serde(default = "Vec::new")]
    pub depends_on: Vec<K>,
    #[serde_as(as = "DurationSeconds<i64>")]
    #[serde(default = "default_duration")]
    pub duration: Duration,
}

#[serde_as]
#[derive(Clone, Serialize, Deserialize)]
pub struct Task<N = TaskName, K = TaskKey> {
    pub id: K,
    pub name: N,
    pub payload: Option<Value>,
    pub depends_on: Vec<K>,
    #[serde_as(as = "DurationSeconds<i64>")]
    pub duration: Duration,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct Execution<T = Task<TaskName, TaskKey>> {
    pub task: T,
    #[serde(with = "iso8601")]
    pub deadline: OffsetDateTime,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct CompleteTask<K = TaskKey> {
    pub id: K,
}
