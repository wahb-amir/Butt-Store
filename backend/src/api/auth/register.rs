use axum::{
    extract::{Json, State},
    http::StatusCode,
};
use serde::{Deserialize, Serialize};
use sqlx::query_as;
use validator::Validate;
use crate::utils::password::hash_password;
use crate::utils::jwt;
use uuid::Uuid;
use crate::{
    errors::{AppError, AppErrorWithMessage},
    state::AppState,
    utils::validate,
};
use crate::types::users::User;

#[derive(Serialize, Deserialize, Validate)]
pub struct RegisterRequest {
    #[validate(email(message = "Invalid email format"))]
    pub email: String,
    pub name : String,
    pub password: String,
}

#[derive(Serialize)]
pub struct RegisterResponse {
    pub message: String,
    pub access_token:String,
    pub refresh_token:String
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
    let password_hash: String = hash_password(password).map_err(|e| {
        eprintln!("Password hashing error: {:?}", e);
        AppError::Internal.with_message("Unable to hash password")
    })?;
    
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
        RETURNING
            id,
            email,
            name,
            status,
            password AS password_hash
        "#,
    )
    .bind(Uuid::new_v4())
    .bind(&payload.email)
    .bind(&payload.name)
    .bind(&password_hash)
    .bind("verified")
    .fetch_one(&state.db)
    .await
    .map_err(|e| {
        eprintln!("Database error: {:?}", e);

        if let sqlx::Error::Database(db_error) = &e {
            if db_error.constraint() == Some("users_email_unique") {
                return AppError::Duplicate
                    .with_message("An account with this email already exists");
            }
        }

        AppError::Internal.with_message("Database error")
    })?;

   
    let access_token = jwt::create_token(jwt::TokenType::Access);
    let refresh_token = jwt::create_token(jwt::TokenType::Refresh);
    Ok((
        StatusCode::OK,
        Json(RegisterResponse {
            message: "Registered successful".to_string(),
            access_token:access_token,
            refresh_token:refresh_token
        }),
    ))
}