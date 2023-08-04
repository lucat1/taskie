use std::collections::HashMap;

use axum::async_trait;
use deadqueue::unlimited::Queue;

use crate::store::{InsertTask, PopError, PushError, Store, Task, TaskKey};

pub struct MemoryStore {
    next_key: TaskKey,
    tasks: HashMap<TaskKey, Task>,

    queue: Queue<TaskKey>,
    edges: Vec<(TaskKey, TaskKey)>,
}

impl MemoryStore {
    pub fn new() -> Self {
        MemoryStore {
            next_key: 0 as TaskKey,
            tasks: HashMap::new(),

            queue: Queue::new(),
            edges: Vec::new(),
        }
    }

    fn add_edge(&mut self, parent: TaskKey, child: TaskKey) {
        let pair = (parent, child);
        // TODO
        // bfs to check for loops
    }
}

#[async_trait]
impl Store for MemoryStore {
    async fn push(&mut self, insert_task: InsertTask) -> Result<Task, PushError> {
        let id = self.next_key;
        self.next_key += 1;

        let task = Task {
            id,
            name: insert_task.name,
        };
        self.tasks.insert(task.id, task.clone());
        if insert_task.depends_on.is_empty() {
            // if the task doesn't have any dependencies, we can just enqueue
            // it, ready to be consumed by workers
            self.queue.push(id);
        } else {
            for parent in insert_task.depends_on.into_iter() {
                if !self.tasks.contains_key(&parent) {
                    return Err(PushError::MissingDependency { dependency: parent });
                }
                self.dag.add_edge(task.id.into(), parent.into(), ())?;
            }
        }

        tracing::info!(dag = ?self.dag, "dag");
        Ok(task)
    }

    async fn pop(&mut self) -> Result<Task, PopError> {
        let id = self.queue.pop().await;
        self.tasks.remove(&id).ok_or(PopError::InvalidID)
    }
}
