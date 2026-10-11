use crate::config::Config;
use azure_core::credentials::Secret;
use azure_identity::ClientSecretCredential;
use azure_storage_blob::BlobServiceClient;
use std::sync::{Arc, OnceLock};
use url::Url;
use azure_storage_sas::SasBuilder;
use time::{Duration, OffsetDateTime};

pub type StorageResult<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

static BLOB_SERVICE: OnceLock<Result<Arc<BlobServiceClient>, String>> = OnceLock::new();

pub fn init_blob_service() -> StorageResult<Arc<BlobServiceClient>> {
    let service: &Result<Arc<BlobServiceClient>, String> = BLOB_SERVICE.get_or_init(|| {
        create_blob_service()
            .map(Arc::new)
            .map_err(|error| error.to_string())
    });

    match service {
        Ok(service) => Ok(Arc::clone(service)),
        Err(error) => Err(error.clone().into()),
    }
}

fn create_blob_service() -> StorageResult<BlobServiceClient> {
    let config: Config = Config::from_env();
    let account_name = &config.azure_storage_account_name;

    let service_url = Url::parse(&format!("https://{account_name}.blob.core.windows.net"))?;

    let credential: Arc<ClientSecretCredential> = ClientSecretCredential::new(
        &config.azure_tenant_id,
        config.azure_client_id,
        Secret::from(config.azure_client_secret),
        None,
    )?;

    let service_client = BlobServiceClient::new(service_url, Some(credential), None)?;

    Ok(service_client)
}

pub async fn generate_upload_sas_url(
    user_id: &str,
    upload_session_id: &str,
    blob_name: &str,
) -> StorageResult<(Url, String)> {
    let config = Config::from_env();
    let service_client = init_blob_service()?;

    // 2. Construct the exact hierarchical file path requested
    let blob_path = format!("{}/{}/{}", user_id, upload_session_id, blob_name);

    let starts_on = OffsetDateTime::now_utc();
    let expires_on = starts_on + Duration::minutes(20);

    // 4. Request a short-lived User Delegation Key from Azure via Entra ID
    let key_info = azure_storage_blob::models::KeyInfo {
        delegated_user_tid: Some(config.azure_tenant_id.clone()),
        start: Some(starts_on),
        expiry: Some(expires_on),
    };
    
    let user_delegation_key = service_client
        .get_user_delegation_key(key_info.try_into()?, None)
        .await?;
    let user_delegation_key = user_delegation_key.into_model()?;

    // 5. Build the SAS token with write permissions required for staging & committing blocks
    let sas_token = SasBuilder::new(
        &config.azure_storage_account_name,
        &user_delegation_key,
        expires_on,
    )?
    .blob(&config.azure_storage_container_name, &blob_path)
    .write() // Grants permission to stage blocks and commit block lists
    .build();

    // 6. Assemble the final URL with the SAS token query attached
    let mut sas_url = Url::parse(&format!(
        "https://{}.blob.core.windows.net/{}/{}",
        config.azure_storage_account_name,
        config.azure_storage_container_name,
        blob_path
    ))?;
    
    sas_url.set_query(Some(&sas_token));

    // Returns both the ready-to-use SAS URL and the blob path (useful to track in your database)
    Ok((sas_url, blob_path))
}