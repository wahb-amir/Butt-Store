use crate::utils::jwt;
use crate::{
    errors::{AppError, AppErrorWithMessage},
    state::AppState,
    types::users::UserStatus,
};
use axum::http::HeaderMap;
use sqlx::query_as;

use super::models::UserLookup;
use super::utils::db_err;

pub(crate) async fn authenticate(
    state: &AppState,
    headers: &HeaderMap,
) -> Result<UserLookup, AppErrorWithMessage> {
    let value = headers.get("authorization").ok_or_else(|| {
        AppError::Unauthorized.with_message("Authorization header is required".to_string())
    })?;

    let value = value.to_str().map_err(|_| {
        AppError::Unauthorized.with_message("Invalid authorization header".to_string())
    })?;

    let token = value.strip_prefix("Bearer ").ok_or_else(|| {
        AppError::Unauthorized.with_message("Invalid authorization scheme".to_string())
    })?;

    let payload: jwt::Payload = jwt::verify_token(token.to_string()).map_err(|_| {
        AppError::Unauthorized.with_message("Unable to verify the token".to_string())
    })?;

    let user = query_as::<_, UserLookup>(
        r#"
        SELECT id, status
        FROM users
        WHERE id = $1
        "#,
    )
    .bind(&payload.sub)
    .fetch_optional(&state.db)
    .await
    .map_err(db_err)?
    .ok_or_else(|| AppError::Unauthorized.with_message("Invalid user".to_string()))?;

    if !matches!(&user.status, UserStatus::Verified) {
        return Err(AppError::FORBIDDEN.with_message("User is not verified".to_string()));
    }

    Ok(user)
}
