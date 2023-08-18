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

    pub async fn pop(&self) -> Result<Execution, ClientError> {
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

    pub async fn complete(&self, task_id: TaskKey) -> Result<(), ClientError> {
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
