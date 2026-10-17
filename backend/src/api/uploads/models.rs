use crate::types::users::UserStatus;
use sqlx::FromRow;
use time::OffsetDateTime;
use uuid::Uuid;

#[derive(Debug, FromRow)]
pub(crate) struct UserLookup {
    pub(crate) id: Uuid,
    pub(crate) status: UserStatus,
}

#[derive(Debug, FromRow)]
pub(crate) struct UploadRecord {
    pub(crate) id: Uuid,
    #[sqlx(rename = "user_id")]
    pub(crate) _user_id: Uuid,
    pub(crate) blob_name: String,
    pub(crate) original_filename: String,
    pub(crate) content_type: String,
    pub(crate) status: String,
    pub(crate) expected_size_bytes: i64,
    pub(crate) expires_at: OffsetDateTime,
}
