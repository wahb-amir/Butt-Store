use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde::Serialize;

#[derive(Debug)]
pub enum AppError {
    Unauthorized,
    BadRequest,
    NotFound,
    Duplicate,
    Internal,
}

pub struct AppErrorWithMessage {
    error: AppError,
    message: String,
}

impl AppError {
    pub fn with_message(self, message: impl Into<String>) -> AppErrorWithMessage {
        AppErrorWithMessage {
            error: self,
            message: message.into(),
        }
    }
}

#[derive(Serialize)]
struct ErrorResponse {
    error: String,
}

fn status_code(error: &AppError) -> StatusCode {
    match error {
        AppError::Unauthorized => StatusCode::UNAUTHORIZED,
        AppError::BadRequest => StatusCode::BAD_REQUEST,
        AppError::NotFound => StatusCode::NOT_FOUND,
        AppError::Duplicate => StatusCode::CONFLICT,
        AppError::Internal => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let (status, message) = match self {
            AppError::Unauthorized => (
                StatusCode::UNAUTHORIZED,
                "Unauthorized".to_owned(),
            ),
            AppError::BadRequest => (
                StatusCode::BAD_REQUEST,
                "Bad request".to_owned(),
            ),
            AppError::NotFound => (
                StatusCode::NOT_FOUND,
                "Resource not found".to_owned(),
            ),
            AppError::Duplicate => (
                StatusCode::CONFLICT,
                "Resource already exists".to_owned(),
            ),
            AppError::Internal => (
                StatusCode::INTERNAL_SERVER_ERROR,
                "Internal server error".to_owned(),
            ),
        };

        (status, Json(ErrorResponse {
            error: message,
        }))
            .into_response()
    }
}

impl IntoResponse for AppErrorWithMessage {
    fn into_response(self) -> Response {
        (
            status_code(&self.error),
            Json(ErrorResponse {
                error: self.message,
            }),
        )
            .into_response()
    }
}