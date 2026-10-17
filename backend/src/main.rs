use axum::{Router, routing::get};
pub mod config;
#[path = "lib/mod.rs"]
mod storage;
use config::Config;
mod api;
pub mod db;
pub mod errors;
pub mod state;
pub mod types;
pub mod utils;
use futures_util::StreamExt;
use state::AppState;

async fn hello() -> String {
    "Hello World!".to_string()
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Load .env file for local development
    dotenvy::dotenv().ok();

    let config: Config = Config::from_env();
    let database_url = config.database_url;

    let pool = match db::create_pool(&database_url).await {
        Ok(pool) => {
            println!("Successfully connected to the database");
            Some(pool)
        }
        Err(error) => {
            eprintln!("Failed to connect to the database: {error}");
            None
        }
    };

    // Test Azure connection by consuming the first item from the stream
    let azure_connected = match storage::azure::init_blob_service() {
        Ok(service_client) => match service_client.list_containers(None) {
            Ok(mut stream) => match stream.next().await {
                Some(Ok(_)) => {
                    println!("Successfully connected and authenticated with Azure Blob Service!");
                    true
                }
                Some(Err(error)) => {
                    eprintln!(
                        "Failed to authenticate or communicate with Azure: {:#?}",
                        error
                    );
                    false
                }
                None => {
                    println!(
                        "Successfully connected, but the storage account has zero containers."
                    );
                    true
                }
            },
            Err(error) => {
                eprintln!("Failed to request Azure containers: {error}");
                false
            }
        },
        Err(error) => {
            eprintln!("Failed to initialize Azure client configuration: {error}");
            false
        }
    };

    let pool = match pool {
        Some(pool) if azure_connected => pool,
        _ => {
            return Err(
                "Backend startup aborted because a required service failed to initialize".into(),
            );
        }
    };

    // Put it into shared application state
    let state = AppState { db: pool };
    let app: Router = Router::new()
        .nest("/auth", api::auth::router())
        .merge(api::uploads::router())
        .route("/", get(hello))
        .with_state(state);

    let listener: tokio::net::TcpListener =
        tokio::net::TcpListener::bind(format!("0.0.0.0:{}", config.port))
            .await
            .unwrap();

    println!("Server running on http://0.0.0.0:{}", config.port);
    axum::serve(listener, app).await.unwrap();

    Ok(())
}
