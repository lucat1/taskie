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

use crate::store::{PopError, PushError};

#[derive(Error, Debug)]
pub enum ApiError {
    #[error("Could not parse JSON input {}", .0.body_text())]
    ParseError(#[from] JsonRejection),

    #[error("")]
    PushError(#[from] PushError),

    #[error("")]
    PopError(#[from] PopError),
}

#[derive(Serialize)]
struct SerializedError {
    status: u16,
    message: String,
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, message) = match self {
            ApiError::ParseError(err) => (err.status(), err.to_string()),
            ApiError::PushError(err) => (err.status(), err.to_string()),
            ApiError::PopError(err) => (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()),
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
