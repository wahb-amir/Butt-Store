pub mod login;
pub mod register;
use axum::{
    routing::{post},
    Router,
};

use crate::state::AppState;

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/login", post(login::login))
        .route("/register", post(register::register))
}