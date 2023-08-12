use std::{
    collections::{HashMap, VecDeque},
    vec,
};

use axum::async_trait;
use deadqueue::unlimited::Queue;
use thiserror::Error;
use tokio::sync::RwLock;

use crate::store::{InsertTask, PopError, PushError, Store, Task, TaskKey};

pub struct MemoryStore {
    next_key: RwLock<TaskKey>,
    tasks: RwLock<HashMap<TaskKey, Task>>,

    queue: Queue<TaskKey>,
    edges: RwLock<HashMap<TaskKey, Vec<TaskKey>>>,
}

#[derive(Error, Debug)]
pub struct CycleError;

impl std::fmt::Display for CycleError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "A cycle in the DAG has been detected")
    }
}

static EMPTY_VEC: Vec<TaskKey> = vec![];

impl MemoryStore {
    pub fn new() -> Self {
        MemoryStore {
            next_key: RwLock::new(0 as TaskKey),
            tasks: RwLock::new(HashMap::new()),

            queue: Queue::new(),
            edges: RwLock::new(HashMap::new()),
        }
    }

    async fn get_edges<'a>(
        edges_map: &'a HashMap<TaskKey, Vec<TaskKey>>,
        node: &'a TaskKey,
    ) -> &'a [TaskKey] {
        match edges_map.get(node) {
            Some(v) => v,
            None => &EMPTY_VEC,
        }
    }

    async fn add_edge(
        &self,
        parent: TaskKey,
        child: TaskKey,
        tasks: &HashMap<TaskKey, Task>,
    ) -> Result<(), CycleError> {
        let mut edges = self.edges.write().await;
        if !edges.contains_key(&parent) {
            edges.insert(parent, Vec::new());
        }
        // Add the edge to the adjacency matrix
        let parent_edges = edges.get_mut(&parent).unwrap();
        parent_edges.push(child);

        // Check for loops in the graph using topological ordering
        let mut in_degree: HashMap<TaskKey, usize> = tasks.iter().map(|(k, _)| (*k, 0)).collect();
        for node in edges.keys() {
            for dest in MemoryStore::get_edges(&edges, node).await.iter() {
                in_degree.insert(*dest, in_degree.get(dest).unwrap() + 1);
            }
        }

        let mut queue = VecDeque::with_capacity(tasks.len());
        for (node, in_deg) in in_degree.iter() {
            if *in_deg == 0 {
                queue.push_back(*node);
            }
        }

        let mut count = queue.len();
        while let Some(node) = queue.pop_front() {
            for dest in MemoryStore::get_edges(&edges, &node).await.iter() {
                let updated = in_degree.get(dest).unwrap() - 1;
                in_degree.insert(*dest, updated);
                if updated == 0 {
                    queue.push_back(*dest);
                    count += 1;
                }
            }
        }

        if count != tasks.len() {
            Err(CycleError)
        } else {
            Ok(())
        }
    }
}

#[async_trait]
impl Store for MemoryStore {
    async fn push(&self, insert_task: InsertTask) -> Result<Task, PushError> {
        let mut next_key = self.next_key.write().await;
        let id = *next_key;
        *next_key += 1;

        let task = Task {
            id,
            payload: insert_task.payload,
            name: insert_task.name,
            duration: insert_task.duration,
            depends_on: insert_task.depends_on.clone(),
        };
        let mut tasks = self.tasks.write().await;
        tasks.insert(task.id, task.clone());
        if insert_task.depends_on.is_empty() {
            // if the task doesn't have any dependencies, we can just enqueue
            // it, ready to be consumed by workers
            self.queue.push(id);
        } else {
            for parent in insert_task.depends_on.into_iter() {
                if !tasks.contains_key(&parent) {
                    return Err(PushError::MissingDependency { dependency: parent });
                }
                self.add_edge(task.id, parent, &tasks).await?;
            }
        }

        tracing::debug!(nodes = ?tasks.keys(), edges = ?self.edges, "Dependency after task insertion");
        Ok(task)
    }

    async fn pop(&self) -> Result<Task, PopError> {
        let task_id = self.queue.pop().await;
        let mut tasks = self.tasks.write().await;
        let task = tasks
            .remove(&task_id)
            .ok_or(PopError::InvalidTaskId(task_id))?;

        // We should also do
        // > self.edges.remove(&task_id);
        // but it is not necesasry, as any node that is on the queue does not
        // have any pending dependency.
        // So, instead we do:
        let mut edges = self.edges.write().await;
        assert!(!edges.contains_key(&task_id));

        // A vector for the tasks which become ready once the current one is popped
        let mut ready = vec![];
        for (node, node_edges) in edges.iter_mut() {
            node_edges.retain(|&dest| dest != task_id);
            if node_edges.is_empty() {
                ready.push(*node);
            }
        }

        // Put any ready task on the queue
        for node in ready.into_iter() {
            tracing::debug!(id = %node, "Task has become ready");
            edges.remove(&node);
            self.queue.push(node);
        }

        // TODO: check if there were tasks waiting on this and they now have 0 deps
        Ok(task)
    }
}
