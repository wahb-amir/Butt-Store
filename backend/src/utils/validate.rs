use crate::errors::{AppError, AppErrorWithMessage};

const ALLOWED_MIME: &str = "application/vnd.android.package-archive";
pub fn validate_password(password: &str) -> Result<(), AppErrorWithMessage> {
    if password.chars().count() < 8 {
        return Err(AppError::BadRequest.with_message("Password must be at least 8 characters"));
    }

    if password.len() > 256 {
        return Err(AppError::BadRequest.with_message("Password is too long"));
    }

    Ok(())
}

pub fn sanitize_filename(filename: &str) -> String {
    const FALLBACK: &str = "unnamed_file";
    const MAX_FILENAME_BYTES: usize = 255;

    // Handle both Unix (/) and Windows (\) paths, regardless
    // of the operating system running this backend.
    let basename = filename
        .rsplit(|c| c == '/' || c == '\\')
        .next()
        .unwrap_or("")
        .trim();

    if basename.is_empty() {
        return FALLBACK.to_owned();
    }

    let mut sanitized = String::with_capacity(basename.len().min(MAX_FILENAME_BYTES));

    let mut previous_was_underscore = false;

    // Keep alphanumeric characters, dots, hyphens and underscores.
    // Replace other characters and collapse consecutive underscores.
    for c in basename.chars() {
        let c = if c.is_alphanumeric() || matches!(c, '.' | '-' | '_') {
            c
        } else {
            '_'
        };

        if c == '_' {
            if !previous_was_underscore {
                sanitized.push('_');
            }
            previous_was_underscore = true;
        } else {
            sanitized.push(c);
            previous_was_underscore = false;
        }
    }

    // Remove leading/trailing dots to avoid "." / ".." and
    // awkward trailing-dot filenames on Windows.
    let sanitized = sanitized.trim_matches('.').to_owned();

    if sanitized.is_empty() {
        return FALLBACK.to_owned();
    }

    // Windows reserves these names, even with file extensions.
    let stem = sanitized
        .split('.')
        .next()
        .unwrap_or("")
        .to_ascii_uppercase();

    let is_reserved = matches!(stem.as_str(), "CON" | "PRN" | "AUX" | "NUL")
        || (stem.len() == 4
            && (stem.starts_with("COM") || stem.starts_with("LPT"))
            && matches!(stem.as_bytes()[3], b'1'..=b'9'));

    let mut sanitized = sanitized;

    if is_reserved {
        sanitized.insert(0, '_');
    }

    // Limit the filename to 255 UTF-8 bytes without cutting
    // through a multibyte Unicode character.
    let mut bounded = String::with_capacity(sanitized.len().min(MAX_FILENAME_BYTES));

    for c in sanitized.chars() {
        if bounded.len() + c.len_utf8() > MAX_FILENAME_BYTES {
            break;
        }
        bounded.push(c);
    }

    // Truncation may leave a trailing dot.
    let bounded = bounded.trim_matches('.');

    if bounded.is_empty() {
        FALLBACK.to_owned()
    } else {
        bounded.to_owned()
    }
}

pub fn validate_file(filename: &str, mime: &str) -> Result<(), AppErrorWithMessage> {
    if filename.trim().is_empty() {
        return Err(AppError::BadRequest.with_message("Filename is required"));
    }

    if filename.chars().count() > 255 {
        return Err(AppError::BadRequest.with_message("Filename is too long"));
    }

    if mime.trim().is_empty() {
        return Err(AppError::BadRequest.with_message("File MIME type is required"));
    }

    let extension = filename
        .rsplit_once('.')
        .map(|(_, extension)| extension.to_ascii_lowercase());

    match extension.as_deref() {
        Some("apk") => {}
        _ => {
            return Err(AppError::BadRequest.with_message("Only APK files are allowed"));
        }
    }

    if mime.trim() != ALLOWED_MIME {
        return Err(AppError::BadRequest.with_message("Unsupported file MIME type"));
    }

    Ok(())
}
