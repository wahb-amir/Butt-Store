use anyhow::{anyhow, Result};
use std::{env, path::Path};

pub fn require_command(command: &str) -> Result<()> {
    let path = env::var_os("PATH")
        .ok_or_else(|| anyhow!("PATH is not set; required command '{command}' cannot be found"))?;

    if env::split_paths(&path).any(|directory| {
        let candidate = Path::new(&directory).join(command);
        candidate.is_file()
    }) {
        Ok(())
    } else {
        Err(anyhow!(
            "Required command '{command}' was not found on PATH"
        ))
    }
}
