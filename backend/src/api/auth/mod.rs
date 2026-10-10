pub mod login;
pub mod me;
pub mod register;
use axum::{
    Router,
    http::HeaderMap,
    response::{IntoResponse, Json},
    routing::{get, post},
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
