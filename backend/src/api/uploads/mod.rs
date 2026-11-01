mod auth;
pub mod config;
mod models;
pub mod upload;
#[allow(non_snake_case)]
mod uploadComplete;
mod utils;
use axum::{Router, routing::post};

use crate::state::AppState;

pub fn router() -> Router<AppState> {
    Router::<AppState>::new()
        .route("/upload", post(upload::upload))
        .route("/uploads/{upload_id}/sas", post(upload::upload_sas))
        .route(
            "/uploads/{upload_id}/complete",
            post(uploadComplete::complete_upload),
        )
}
