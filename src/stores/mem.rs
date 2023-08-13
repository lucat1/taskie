use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
    vec,
};

use axum::async_trait;
use deadqueue::unlimited::Queue;
use thiserror::Error;
use time::OffsetDateTime;
use tokio::sync::{
    mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
    oneshot::{self as oneshot, Sender},
    Mutex, RwLock,
};
use tokio::time::timeout;

use crate::store::{
    CompleteError, Execution, InsertTask, MonitorError, PopError, PushError, Store, Task, TaskKey,
};

#[derive(Clone)]
enum MonitorMessage {
    Popped(Task),
    Completed(TaskKey),
    TimedOut(TaskKey),
}

pub struct MemoryStore {
    next_key: RwLock<TaskKey>,
    tasks: RwLock<HashMap<TaskKey, Task>>,
    processing: RwLock<HashMap<TaskKey, (Task, Sender<()>)>>,
    queue: Queue<TaskKey>,
    edges: RwLock<HashMap<TaskKey, Vec<TaskKey>>>,
    chan: (
        UnboundedSender<MonitorMessage>,
        Mutex<UnboundedReceiver<MonitorMessage>>,
    ),
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
        let (tx, rx) = unbounded_channel();

        MemoryStore {
            next_key: RwLock::new(TaskKey(1)),
            tasks: RwLock::new(HashMap::new()),
            processing: RwLock::new(HashMap::new()),
            queue: Queue::new(),
            edges: RwLock::new(HashMap::new()),
            chan: (tx, Mutex::new(rx)),
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
    async fn monitor(&self) -> Result<(), MonitorError> {
        let mut rx = self.chan.1.lock().await;
        let tx = Arc::new(self.chan.0.clone());

        while let Some(msg) = rx.recv().await {
            match msg {
                MonitorMessage::Popped(task) => {
                    let Task(task) = task;
                    // The task has been popped off of the queue and we have to set a
                    // timeout to wait for when the task.
                    let (ttx, rx) = oneshot::channel::<()>();
                    {
                        let mut processing = self.processing.write().await;
                        processing.insert(task.id, (Task(task.clone()), ttx));
                    }
                    let tx = tx.clone();
                    tokio::spawn(async move {
                        if let Err(_) = timeout(task.duration.unsigned_abs(), rx).await {
                            if let Err(err) = tx.send(MonitorMessage::TimedOut(task.id)) {
                                tracing::error!(id = %task.id, ?err, "Timeout task cannot communicate with store monitor");
                            }
                        }
                    });
                }
                MonitorMessage::Completed(task_id) => {
                    tracing::info!(id = %task_id, "Task execution complete");
                    {
                        let mut processing = self.processing.write().await;
                        let (_, ttx) = processing
                            .remove(&task_id)
                            .ok_or(MonitorError::InvalidTask(task_id))?;
                        ttx.send(())
                            .map_err(|_| MonitorError::CancelTimeout(task_id))?;
                    }
                }
                MonitorMessage::TimedOut(task_id) => {
                    tracing::info!(id = %task_id, "Task execution timed out");
                    {
                        let mut processing = self.processing.write().await;
                        let (task, _) = processing
                            .remove(&task_id)
                            .ok_or(MonitorError::InvalidTask(task_id))?;

                        let mut tasks = self.tasks.write().await;
                        tasks.insert(task_id, task);
                        self.queue.push(task_id);
                    }
                }
            }
        }
        Err(MonitorError::ChannelDropped)
    }

    async fn push(&self, insert_task: InsertTask) -> Result<Task, PushError> {
        let InsertTask(insert_task) = insert_task;
        let mut next_key = self.next_key.write().await;
        let TaskKey(id) = *next_key;
        *next_key = TaskKey(id + 1);

        let task = Task(structures::Task {
            id: TaskKey(id),
            payload: insert_task.payload,
            name: insert_task.name,
            duration: insert_task.duration,
            depends_on: insert_task.depends_on.clone(),
        });
        let mut tasks = self.tasks.write().await;
        tasks.insert(TaskKey(id), task.clone());
        if insert_task.depends_on.is_empty() {
            // if the task doesn't have any dependencies, we can just enqueue
            // it, ready to be consumed by workers
            self.queue.push(TaskKey(id));
        } else {
            for parent in insert_task.depends_on.into_iter() {
                if !tasks.contains_key(&parent) {
                    return Err(PushError::MissingDependency { dependency: parent });
                }
                self.add_edge(TaskKey(id), parent, &tasks).await?;
            }
        }

        tracing::debug!(nodes = ?tasks.keys(), edges = ?self.edges, "Dependency after task insertion");
        Ok(task)
    }

    async fn pop(&self) -> Result<Execution, PopError> {
        let (tx, _) = &self.chan;
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
        let edges = self.edges.read().await;
        assert!(!edges.contains_key(&task_id));

        tx.send(MonitorMessage::Popped(task.clone()))
            .map_err(|_| PopError::MonitorCommunication)?;
        Ok(Execution(structures::Execution {
            deadline: OffsetDateTime::now_utc() + task.0.duration,
            task,
        }))
    }

    async fn complete(&self, task_id: TaskKey) -> Result<(), CompleteError> {
        let processing = self.processing.read().await;
        if !processing.contains_key(&task_id) {
            return Err(CompleteError::InvalidTaskId(task_id));
        }

        let (tx, _) = &self.chan;
        tx.send(MonitorMessage::Completed(task_id))
            .map_err(|_| CompleteError::MonitorCommunication)?;

        let mut edges = self.edges.write().await;
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
        Ok(())
    }
}
