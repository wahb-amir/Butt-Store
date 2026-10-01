use axum::{
    routing::{get},
    Json, Router,
};
mod config;
use config::Config;
use serde::{Serialize};

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

    let listener: tokio::net::TcpListener = tokio::net::TcpListener::bind(format!("127.0.0.1:{}", config.port))
        .await
        .unwrap();

    println!("Server running on http://127.0.0.1:{}", config.port);

    axum::serve(listener, app)
        .await
        .unwrap();
}