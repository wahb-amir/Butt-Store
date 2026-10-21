use serde::{Deserialize, Serialize};
use sqlx::FromRow;
use time::OffsetDateTime;
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JobType {
    VerifyApk,
    PrepareDistribution,
}

impl JobType {
    pub fn as_str(&self) -> &'static str {
        match self {
            JobType::VerifyApk => "verify_apk",
            JobType::PrepareDistribution => "prepare_distribution",
        }
    }

    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "verify_apk" => Some(JobType::VerifyApk),
            "prepare_distribution" => Some(JobType::PrepareDistribution),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JobStatus {
    Pending,
    Processing,
    Completed,
    Failed,
}

impl JobStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            JobStatus::Pending => "pending",
            JobStatus::Processing => "processing",
            JobStatus::Completed => "completed",
            JobStatus::Failed => "failed",
        }
    }
}

#[derive(Debug, FromRow)]
pub struct BackgroundJob {
    pub id: Uuid,
    pub file_id: Uuid,
    pub job_type: String,
    pub status: String,
    pub attempts: i32,
    pub available_at: OffsetDateTime,
    pub locked_until: Option<OffsetDateTime>,
    pub last_error: Option<String>,
    pub created_at: OffsetDateTime,
    pub completed_at: Option<OffsetDateTime>,
}

impl BackgroundJob {
    pub fn job_type(&self) -> Option<JobType> {
        JobType::from_str(&self.job_type)
    }

    pub fn status(&self) -> Option<JobStatus> {
        match self.status.as_str() {
            "pending" => Some(JobStatus::Pending),
            "processing" => Some(JobStatus::Processing),
            "completed" => Some(JobStatus::Completed),
            "failed" => Some(JobStatus::Failed),
            _ => None,
        }
    }
}

#[derive(Debug, Serialize)]
pub struct JobResponse {
    pub id: Uuid,
    pub file_id: Uuid,
    pub job_type: String,
    pub status: String,
    pub attempts: i32,
    pub created_at: String,
    pub completed_at: Option<String>,
}