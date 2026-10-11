use std::env;

#[derive(Clone)]
pub struct Config {
    pub port: u16,
    pub secret: String,
    pub database_url: String,
    pub azure_storage_account_name: String,
    pub azure_storage_container_name: String,
    pub azure_tenant_id: String,
    pub azure_client_id: String,
    pub azure_client_secret: String,
}

impl Config {
    pub fn from_env() -> Self {
        dotenvy::dotenv().ok();

        Self {
            port: env::var("PORT")
                .unwrap_or_else(|_| "8080".to_string())
                .parse()
                .expect("PORT must be a valid number"),
            secret: env::var("JWT_SECRET").expect("JWT_SECRET environment variable must be set"),
            database_url: env::var("DATABASE_URL")
                .expect("DATABASE_URL environment variable must be set"),
            azure_storage_account_name: env::var("AZURE_STORAGE_ACCOUNT_NAME")
                .expect("AZURE_STORAGE_ACCOUNT_NAME must be set"),
            azure_storage_container_name: env::var("AZURE_STORAGE_CONTAINER_NAME")
                .expect("AZURE_STORAGE_CONTAINER_NAME must be set"),
            azure_tenant_id: env::var("AZURE_TENANT_ID").expect("AZURE_TENANT_ID must be set"),
            azure_client_id: env::var("AZURE_CLIENT_ID").expect("AZURE_CLIENT_ID must be set"),
            azure_client_secret: env::var("AZURE_CLIENT_SECRET")
                .expect("AZURE_CLIENT_SECRET must be set"),
        }
    }
}
