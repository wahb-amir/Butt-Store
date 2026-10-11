use crate::config::Config;
use jsonwebtoken::{DecodingKey, EncodingKey, Header, Validation, decode, encode};
use serde::{Deserialize, Serialize};
use std::time::{SystemTime, UNIX_EPOCH};
use uuid::Uuid;
#[derive(Debug, Serialize, Deserialize)]
pub struct Payload {
    pub sub: Uuid,
    pub exp: usize,
    pub iat: usize,
}

pub enum TokenType {
    Access,
    Refresh,
}

fn config() -> Config {
    Config::from_env()
}

pub fn create_token(user_id: Uuid, token_type: TokenType) -> String {
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
        sub: user_id,
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
