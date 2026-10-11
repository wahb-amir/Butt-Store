use crate::types::users::User;
use crate::utils::jwt;
use crate::utils::password::hash_password;
use crate::{
    errors::{AppError, AppErrorWithMessage},
    state::AppState,
    utils::validate,
};
use axum::{
    extract::{Json, State},
    http::StatusCode,
};
use serde::{Deserialize, Serialize};
use sqlx::query_as;
use uuid::Uuid;
use validator::Validate;

#[derive(Serialize, Deserialize, Validate)]
pub struct RegisterRequest {
    #[validate(email(message = "Invalid email format"))]
    pub email: String,
    pub name: String,
    pub password: String,
}

#[derive(Serialize)]
pub struct RegisterResponse {
    pub message: String,
    pub access_token: String,
    pub refresh_token: String,
}
// TODO : Skiping the email verification flow for now implment it later!
pub async fn register(
    State(state): State<AppState>,
    Json(payload): Json<RegisterRequest>,
) -> Result<(StatusCode, Json<RegisterResponse>), AppErrorWithMessage> {
    if let Err(_e) = payload.validate() {
        return Err(AppError::BadRequest.with_message("Invalid email".to_string()));
    }
    let password: &String = &payload.password;

    if let Err(e) = validate::validate_password(password) {
        return Err(e);
    }

    let password_hash: String = hash_password(password).await.map_err(|e| {
        eprintln!("Password hashing error: {:?}", e);
        AppError::Internal.with_message("Unable to hash password")
    })?;
    let user_id: Uuid = Uuid::new_v4();
    query_as::<_, User>(
        r#"
        INSERT INTO users (
            id,
            email,
            name,
            password,
            status
        )
        VALUES ($1, $2, $3, $4, $5::user_status)
        "#,
    )
    .bind(user_id)
    .bind(&payload.email)
    .bind(&payload.name)
    .bind(&password_hash)
    .bind("verified")
    .fetch_one(&state.db)
    .await
    .map_err(|e: sqlx::Error| {
        if let sqlx::Error::Database(db_error) = &e {
            if db_error.code().as_deref() == Some("23505")
                && matches!(
                    db_error.constraint(),
                    Some("users_email_unique") | Some("users_email_key")
                )
            {
                return AppError::Duplicate
                    .with_message("An account with this email already exists");
            }
        }

        eprintln!("Database error: {:?}", e);
        AppError::Internal.with_message("Database error")
    })?;

    let access_token = jwt::create_token(user_id, jwt::TokenType::Access);

    let refresh_token = jwt::create_token(user_id, jwt::TokenType::Refresh);

    Ok((
        StatusCode::OK,
        Json(RegisterResponse {
            message: "Registered successful".to_string(),
            access_token: access_token,
            refresh_token: refresh_token,
        }),
    ))
}
