use crate::{
    errors::{AppError, AppErrorWithMessage},
};
pub fn validate_password(password: &str) -> Result<(), AppErrorWithMessage> {
    if password.chars().count() < 8 {
        return Err(
            AppError::BadRequest
                .with_message("Password must be at least 8 characters")
        );
    }

    if password.len() > 256 {
        return Err(
            AppError::BadRequest
                .with_message("Password is too long")
        );
    }

    Ok(())
}