use axum::{
    extract::{Json, State},
    http::StatusCode,
};
use serde::{Deserialize, Serialize};
use sqlx::query_as;
use validator::Validate;
use crate::utils::password::verify_password;
use crate::utils::jwt;
use crate::{
    errors::{AppError, AppErrorWithMessage},
    state::AppState,
    utils::validate,
};
use crate::types::users::User;
#[derive(Deserialize, Validate)]
pub struct LoginRequest {
    #[validate(email(message = "Invalid email format"))]
    pub email: String,
    pub password: String,
}

#[derive(Serialize)]
pub struct LoginResponse {
    pub message: String,
    access_token:String,
    refresh_token:String
}

pub async fn login(
    State(state): State<AppState>,
    Json(payload): Json<LoginRequest>,
) -> Result<(StatusCode, Json<LoginResponse>), AppErrorWithMessage> {
    if let Err(_e) = payload.validate() {
        return Err(AppError::BadRequest.with_message("Invalid email".to_string()));
    }

    if let Err(e) = validate::validate_password(&payload.password) {
        return Err(e);
    }

    let user: Option<User> = query_as::<_, User>(
        r#"
        SELECT
            id,
            email,
            status,
            CASE
                WHEN status IN ('banned', 'suspended')
                THEN NULL
                ELSE password
            END AS password_hash
        FROM users
        WHERE email = $1
        "#,
    )
    .bind(&payload.email)
    .fetch_optional(&state.db)
    .await
    .map_err(|e| {
        eprintln!("Database error: {:?}", e);
        AppError::Internal.with_message("Database error")
    })?;

    let user = user.ok_or_else(|| {
        AppError::BadRequest.with_message("Invalid email or password".to_string())
    })?;

    let password_hash = user.password_hash.ok_or_else(|| {
        AppError::BadRequest.with_message("Account suspended or banned".to_string())
    })?;

    let is_password_valid = verify_password(&payload.password, &password_hash)
        .map_err(|e| {
            eprintln!("Password verification error: {:?}", e);
            AppError::Internal.with_message("Unable to verify password")
        })?;
    
    if !is_password_valid {
        return Err(AppError::BadRequest.with_message("Invalid email or password".to_string()));
    }
    let access_token = jwt::create_token(jwt::TokenType::Access);
    let refresh_token = jwt::create_token(jwt::TokenType::Refresh);
    Ok((
        StatusCode::OK,
        Json(LoginResponse {
            message: "Login successful".to_string(),
            access_token:access_token,
            refresh_token:refresh_token
        }),
    ))
}