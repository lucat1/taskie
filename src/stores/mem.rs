use std::collections::{HashMap, VecDeque};

use axum::async_trait;
use deadqueue::unlimited::Queue;
use thiserror::Error;

use crate::store::{InsertTask, PopError, PushError, Store, Task, TaskKey};

pub struct MemoryStore {
    next_key: TaskKey,
    tasks: HashMap<TaskKey, Task>,

    queue: Queue<TaskKey>,
    edges: HashMap<TaskKey, Vec<TaskKey>>,
}

#[derive(Error, Debug)]
pub struct CycleError;

impl std::fmt::Display for CycleError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "A cycle in the DAG has been detected")
    }
}

static EMPTY_VEC: [TaskKey; 0] = [];

impl MemoryStore {
    pub fn new() -> Self {
        MemoryStore {
            next_key: 0 as TaskKey,
            tasks: HashMap::new(),

            queue: Queue::new(),
            edges: HashMap::new(),
        }
    }

    fn get_edges(&self, node: &TaskKey) -> &[TaskKey] {
        match self.edges.get(node) {
            Some(v) => v,
            None => &EMPTY_VEC,
        }
    }

    fn add_edge(&mut self, parent: TaskKey, child: TaskKey) -> Result<(), CycleError> {
        if !self.edges.contains_key(&parent) {
            self.edges.insert(parent, Vec::new());
        }
        // Add the edge to the adjacency matrix
        let parent_edges = self.edges.get_mut(&parent).unwrap();
        parent_edges.push(child);

        // Check for loops in the graph using topological ordering
        let mut in_degree: HashMap<TaskKey, usize> =
            self.tasks.iter().map(|(k, _)| (*k, 0)).collect();
        for node in self.edges.keys() {
            for dest in self.get_edges(node).iter() {
                in_degree.insert(*dest, in_degree.get(dest).unwrap() + 1);
            }
        }

        let mut queue = VecDeque::with_capacity(self.tasks.len());
        for (node, in_deg) in in_degree.iter() {
            if *in_deg == 0 {
                queue.push_back(*node);
            }
        }

        let mut count = queue.len();
        while let Some(node) = queue.pop_front() {
            for dest in self.get_edges(&node).iter() {
                let updated = in_degree.get(dest).unwrap() - 1;
                in_degree.insert(*dest, updated);
                if updated == 0 {
                    queue.push_back(*dest);
                    count += 1;
                }
            }
        }

        if count != self.tasks.len() {
            Err(CycleError)
        } else {
            Ok(())
        }
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
                self.add_edge(task.id, parent)?;
            }
        }

        tracing::debug!(nodes = ?self.tasks.keys(), edges = ?self.edges, "Dependency after task insertion");
        Ok(task)
    }

    async fn pop(&mut self) -> Result<Task, PopError> {
        let id = self.queue.pop().await;
        self.tasks.remove(&id).ok_or(PopError::InvalidID)
    }
}
