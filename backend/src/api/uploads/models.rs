use crate::types::users::UserStatus;
use sqlx::{FromRow, Type};
use time::OffsetDateTime;
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Type)]
#[sqlx(type_name = "upload_status", rename_all = "SCREAMING_SNAKE_CASE")]
pub enum UploadStatus {
    Pending,
    ProcessingPending,
    Completed,
    Failed,
    Expired,
    Cancelled,
    Quarantined,
    Deleted,
    PendingVerification,
    Verified,
    ReadyForDistribution,
}

impl UploadStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "PENDING",
            Self::ProcessingPending => "PROCESSING_PENDING",
            Self::Completed => "COMPLETED",
            Self::Failed => "FAILED",
            Self::Expired => "EXPIRED",
            Self::Cancelled => "CANCELLED",
            Self::Quarantined => "QUARANTINED",
            Self::Deleted => "DELETED",
            Self::PendingVerification => "PENDING_VERIFICATION",
            Self::Verified => "VERIFIED",
            Self::ReadyForDistribution => "READY_FOR_DISTRIBUTION",
        }
    }
}

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
    pub(crate) status: UploadStatus,
    pub(crate) expected_size_bytes: i64,
    #[sqlx(default)]
    pub(crate) actual_size_bytes: Option<i64>,
    #[sqlx(default)]
    pub(crate) etag: Option<String>,
    #[sqlx(default)]
    pub(crate) checksum_sha256: Option<Vec<u8>>,
    pub(crate) expires_at: OffsetDateTime,
}