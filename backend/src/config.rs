use std::env;

#[derive(Clone)]
pub struct Config {
    pub port : u16,
    pub secret: String,
}

impl Config {
    pub fn from_env() -> Self {
        dotenvy::dotenv().ok();

        Self {
            port: env::var("PORT")
                .unwrap_or_else(|_| "8080".to_string())
                .parse()
                .expect("PORT must be a valid number"),
            secret: env::var("JWT_SECRET")
                .expect("JWT_SECRET environment variable must be set"),
        }
    }
}