mod auth;
mod complete;
mod config;
mod models;
pub mod upload;
mod utils;
use axum::{Router, routing::post};

use crate::state::AppState;

pub fn router() -> Router<AppState> {
    Router::<AppState>::new()
        .route("/upload", post(upload::upload))
        .route("/uploads/{upload_id}/sas", post(upload::upload_sas))
        .route("/uploads/{upload_id}/complete", post(complete::complete_upload))
}
