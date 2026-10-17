pub mod upload;
use axum::{routing::post, Router};

use crate::state::AppState;

pub fn router() -> Router<AppState> {
    Router::<AppState>::new()
        .route("/upload", post(upload::upload))
        .route("/uploads/{upload_id}/sas", post(upload::upload_sas))
}
