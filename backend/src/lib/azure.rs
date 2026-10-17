use crate::config::Config;
use azure_core::credentials::Secret;
use azure_identity::ClientSecretCredential;
use azure_storage_blob::BlobServiceClient;
use azure_storage_sas::SasBuilder;
use azure_core::http::{NoFormat, RequestContent, XmlFormat};
use azure_storage_blob::clients::BlockBlobClient;
use azure_storage_blob::models::BlockLookupList;
use std::sync::{Arc, OnceLock};
use time::{Duration, OffsetDateTime};
use url::Url;
use std::error::Error;
use std::io::{Error as IoError, ErrorKind};

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
    let blob_path = format!("{}/{}/{}", user_id, upload_session_id, blob_name);
    let (url, _) = generate_upload_sas_url_for_blob(&blob_path).await?;
    Ok((url, blob_path))
}

pub async fn generate_upload_sas_url_for_blob(
    blob_path: &str,
) -> StorageResult<(Url, OffsetDateTime)> {
    let config = Config::from_env();
    let service_client = init_blob_service()?;

    // 2. Construct the exact hierarchical file path requested
    let starts_on = OffsetDateTime::now_utc();
    let expires_on = starts_on + Duration::minutes(15);

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
    .blob(&config.azure_storage_container_name, blob_path)
    .create()
    .write()
    .build();

    // 6. Assemble the final URL with the SAS token query attached
    let mut sas_url = Url::parse(&format!(
        "https://{}.blob.core.windows.net/{}/{}",
        config.azure_storage_account_name, config.azure_storage_container_name, blob_path
    ))?;

    sas_url.set_query(Some(&sas_token));

    // Returns both the ready-to-use SAS URL and the blob path (useful to track in your database)
    Ok((sas_url, expires_on))
}

pub fn create_block_blob_client(blob_path: &str) -> StorageResult<BlockBlobClient> {
    let config = Config::from_env();
    let service_client = init_blob_service()?;

    // Derive the standard BlobClient from the service and container config
    let blob_client = service_client.blob_client(
        &config.azure_storage_container_name,
        blob_path,
    );

    // Convert it into a BlockBlobClient to perform block-level operations (like commit_upload)
    Ok(blob_client.block_blob_client())
}


const MAX_BLOCKS_PER_BLOB: u64 = 50_000;

/// Returns (block_index, expected_block_size_bytes).
fn expected_blocks(
    file_size: u64,
    chunk_size: u64,
) -> Result<Vec<(u64, u64)>, IoError> {
    if chunk_size == 0 {
        return Err(IoError::new(
            ErrorKind::InvalidInput,
            "Chunk size must be greater than zero",
        ));
    }

    if file_size == 0 {
        return Err(IoError::new(
            ErrorKind::InvalidInput,
            "Empty uploads are not supported",
        ));
    }

    // Integer ceiling division.
    let count = file_size / chunk_size
        + u64::from(file_size % chunk_size != 0);

    if count > MAX_BLOCKS_PER_BLOB {
        return Err(IoError::new(
            ErrorKind::InvalidInput,
            "Upload exceeds Azure's maximum block count",
        ));
    }

    Ok((0..count)
        .map(|index| {
            let expected_size =
                chunk_size.min(file_size - index * chunk_size);

            (index, expected_size)
        })
        .collect())
}

async fn commit_upload(
    blob: &BlockBlobClient,
    file_size: u64,
    chunk_size: u64,
) -> Result<(), Box<dyn Error>> {

    let blocks = expected_blocks(file_size, chunk_size)?;

    let ordered_ids: Vec<Vec<u8>> = blocks
        .iter()
        .map(|(index, _expected_size)| {
            format!("block-{index:08}").into_bytes()
        })
        .collect();

    let block_list = BlockLookupList {
        committed: None,
        latest: Some(ordered_ids),
        uncommitted: None,
    };

    let body: RequestContent<BlockLookupList, XmlFormat> =
        block_list.try_into()?;

    blob.commit_block_list(body, None).await?;

    Ok(())
}