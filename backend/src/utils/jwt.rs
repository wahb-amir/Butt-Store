use jsonwebtoken::{decode, encode, DecodingKey, EncodingKey, Header, Validation};
use serde::{Deserialize, Serialize};
use crate::config::Config;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Serialize, Deserialize)]
pub struct Payload {
    sub: String,
    exp: usize,
    iat: usize,
}

pub enum TokenType {
    Access,
    Refresh,
}

fn config() -> Config {
    Config::from_env()
}


pub fn create_token(token_type: TokenType) -> String {
    const ACCESS_TOKEN_LIFETIME: u64 = 15 * 60;
    const REFRESH_TOKEN_LIFETIME: u64 = 7 * 24 * 60 * 60;

    let issued_at = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock is before Unix epoch")
        .as_secs();

    let lifetime = match token_type {
        TokenType::Access => ACCESS_TOKEN_LIFETIME,
        TokenType::Refresh => REFRESH_TOKEN_LIFETIME,
    };

    let payload = Payload {
        sub: "user_id".to_string(),
        iat: issued_at as usize,
        exp: (issued_at + lifetime) as usize,
    };
   

    encode(
        &Header::default(),
        &payload,
        &EncodingKey::from_secret(config().secret.as_bytes()),
    )
    .unwrap()
}

pub fn verify_token(token: String) -> Result<Payload, jsonwebtoken::errors::Error> {
    let result = decode::<Payload>(
        &token,
        &DecodingKey::from_secret(config().secret.as_bytes()),
        &Validation::default(),
    )?;

    Ok(result.claims)
}