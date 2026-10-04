use axum::{
    routing::{get},
    Json, Router,
};
pub mod config;
use config::Config;
use serde::{Serialize};
pub mod utils;
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
async fn main() {
    let config: Config = Config::from_env();
    let app: Router = Router::new()
        .route("/", get(hello));

    let listener: tokio::net::TcpListener = tokio::net::TcpListener::bind(format!("0.0.0.0:{}", config.port))
        .await
        .unwrap();

    println!("Server running on http://0.0.0.0:{}", config.port);

    axum::serve(listener, app)
        .await
        .unwrap();
}