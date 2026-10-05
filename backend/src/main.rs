use axum::{
    routing::{get},
    Json, Router,
};
pub mod config;
use config::Config;
use serde::{Serialize};
pub mod utils;
pub mod db;
pub mod state;
pub mod types;
mod api;
pub mod errors;
use state::AppState;
#[derive(Serialize)]
struct HelloResponse {
    message: String,
}

async fn hello() -> Json<HelloResponse> {
    Json(HelloResponse {
        message: "Hello from Rust!".to_string(),
    })
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config: Config = Config::from_env();
    let database_url = config.database_url;
    let pool = db::create_pool(&database_url).await?;

    // Put it into shared application state
    let state = AppState {
        db: pool,
    };
    let app: Router = Router::new()
        .nest("/auth", api::auth::router())
        .route("/", get(hello))
        .with_state(state);

    let listener: tokio::net::TcpListener = tokio::net::TcpListener::bind(format!("0.0.0.0:{}", config.port))
        .await
        .unwrap();

    println!("Server running on http://0.0.0.0:{}", config.port);

    axum::serve(listener, app)
        .await
        .unwrap();

    Ok(())
}