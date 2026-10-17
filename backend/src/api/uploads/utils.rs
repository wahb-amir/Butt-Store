use crate::errors::{AppError, AppErrorWithMessage};
use time::{OffsetDateTime, format_description::well_known::Rfc3339};

pub(crate) fn db_err(error: sqlx::Error) -> AppErrorWithMessage {
    eprintln!("Database error: {:?}", error);

    AppError::Internal.with_message("Database error")
}

pub(crate) fn format_timestamp(timestamp: OffsetDateTime) -> Result<String, AppErrorWithMessage> {
    timestamp.format(&Rfc3339).map_err(|error| {
        eprintln!("Timestamp formatting error: {error}");

        AppError::Internal.with_message("Unable to prepare upload")
    })
}
