pub mod login;
pub mod register;
pub mod me;
use axum::{
    http::HeaderMap,
    response::{IntoResponse, Json},
    routing::{post,get},
    Router,
};

use crate::state::AppState;

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/login", post(login::login))
        .route("/register", post(register::register))
        .route("/me", get(me_handler))
}

async fn me_handler(headers: HeaderMap) -> impl IntoResponse {
    match me::me(headers).await {
        Ok(payload) => Json(payload).into_response(),
        Err(error) => error.into_response(),
    }
}