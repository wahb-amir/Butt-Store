use axum::http::HeaderMap;
use crate::utils::jwt;

use crate::errors::{AppError, AppErrorWithMessage};

pub async fn me(headers: HeaderMap) -> Result<jwt::Payload, AppErrorWithMessage> {
    let value = headers
        .get("authorization")
        .ok_or_else(|| AppError::BadRequest.with_message("Authorization header is required".to_string()))?;

    let value = value
        .to_str()
        .map_err(|_| AppError::BadRequest.with_message("Invalid authorization header".to_string()))?;

    let token = value
        .strip_prefix("Bearer ")
        .ok_or_else(|| AppError::BadRequest.with_message("Invalid authorization scheme".to_string()))?;

    let result = jwt::verify_token(token.to_string()).map_err(|_| {
        AppError::BadRequest.with_message("Unable to verify the token".to_string())
    })?;

    Ok(result)
}
