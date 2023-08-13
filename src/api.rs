use axum::{
    async_trait,
    body::HttpBody,
    extract::{rejection::JsonRejection, FromRequest, Json as AxumJson},
    http::{Request, StatusCode},
    response::{IntoResponse, Response},
    BoxError,
};
use serde::{de::DeserializeOwned, Serialize};
use thiserror::Error;

use crate::store::{CompleteError, ConcealError, KeyDecodeError, PopError, PushError};
use structures::Error as SerializedError;

#[derive(Error, Debug)]
pub enum ApiError {
    #[error("Could not parse JSON input {}", .0.body_text())]
    Parse(#[from] JsonRejection),

    #[error("Could not parse Task key: {}", .0)]
    KeyDecode(#[from] KeyDecodeError),

    #[error("Could not conceal the Task key: {}", .0)]
    KeyEncode(#[from] ConcealError),

    #[error("Error while pushing a new task: {}", .0)]
    Push(#[from] PushError),

    #[error("Error while popping from the queue: {}", .0)]
    Pop(#[from] PopError),

    #[error("Error while setting a task as completed: {}", .0)]
    Complete(#[from] CompleteError),
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, message) = match self {
            ApiError::Parse(err) => (err.status(), err.to_string()),
            ApiError::KeyDecode(err) => (err.status(), err.to_string()),
            ApiError::KeyEncode(err) => (err.status(), err.to_string()),
            ApiError::Push(err) => (err.status(), err.to_string()),
            ApiError::Pop(err) => (err.status(), err.to_string()),
            ApiError::Complete(err) => (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()),
        };

        let err = AxumJson(SerializedError {
            status: status.as_u16(),
            message,
        });

        (status, err).into_response()
    }
}

pub struct Json<T>(pub T);

#[async_trait]
impl<T, S, B> FromRequest<S, B> for Json<T>
where
    T: DeserializeOwned,
    B: HttpBody + Send + 'static,
    B::Data: Send,
    B::Error: Into<BoxError>,
    S: Send + Sync,
{
    type Rejection = ApiError;

    async fn from_request(req: Request<B>, state: &S) -> Result<Self, Self::Rejection> {
        let AxumJson(t) = AxumJson::from_request(req, state).await?;
        Ok(Json(t))
    }
}

impl<T> IntoResponse for Json<T>
where
    T: Serialize,
{
    fn into_response(self) -> Response {
        let Json(data) = self;
        AxumJson(data).into_response()
    }
}
