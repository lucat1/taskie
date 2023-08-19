use reqwest::StatusCode;
use thiserror::Error;

pub use taskie_structures::*;

pub struct Client {
    host: url::Url,
    client: reqwest::Client,
}

#[derive(Error, Debug)]
pub enum ClientError {
    #[error("Could not build the url for the action from the host parameter")]
    ParseUrl(#[from] url::ParseError),
    #[error("Error while sending HTTP request")]
    Request(#[from] reqwest::Error),
    #[error("Request failed with status code: {}", .0)]
    Unsuccessful(StatusCode),
}
impl Client {
    pub fn new(host: url::Url) -> Self {
        Client {
            host,
            client: reqwest::Client::new(),
        }
    }

    pub async fn push<N, K>(&self, task: &InsertTask<N>) -> Result<Task<N, K>, ClientError>
    where
        N: serde::Serialize + for<'a> serde::Deserialize<'a>,
        K: for<'a> serde::Deserialize<'a>,
    {
        let push_url = self.host.join("/v1/push")?;
        Ok(self
            .client
            .put(push_url.clone())
            .json(task)
            .send()
            .await?
            .json()
            .await?)
    }

    pub async fn pop<N, K>(&self) -> Result<Execution<Task<N, K>>, ClientError>
    where
        N: for<'a> serde::Deserialize<'a>,
        K: for<'a> serde::Deserialize<'a>,
    {
        let pop_url = self.host.join("/v1/pop")?;
        loop {
            let response = self.client.get(pop_url.clone()).send().await;
            match response {
                Err(e) => {
                    if !e.is_timeout() {
                        return Err(e.into());
                    }
                }
                Ok(response) => return Ok(response.json().await?),
            }
        }
    }

    pub async fn complete<K: serde::Serialize>(&self, task_id: K) -> Result<(), ClientError> {
        let complete_url = self.host.join("/v1/complete")?;
        let response = self
            .client
            .post(complete_url.clone())
            .json(&CompleteTask { id: task_id })
            .send()
            .await?;
        if response.status().is_success() {
            Ok(())
        } else {
            Err(ClientError::Unsuccessful(response.status()))
        }
    }
}
