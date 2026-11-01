mod clock;
mod hook;
mod lifecycle;
mod remote;
mod scheduler;

use std::{env, path::PathBuf};

use anyhow::{Context, Result};

fn runtime_socket() -> Result<PathBuf> {
    Ok(
        PathBuf::from(env::var_os("XDG_RUNTIME_DIR").context("XDG_RUNTIME_DIR is unset")?)
            .join("codex-saver/socket"),
    )
}

fn control_socket() -> Result<PathBuf> {
    let codex_home = env::var_os("CODEX_HOME")
        .map(PathBuf::from)
        .or_else(|| env::var_os("HOME").map(|home| PathBuf::from(home).join(".codex")))
        .context("HOME is unset")?;
    Ok(codex_home.join("app-server-control/app-server-control.sock"))
}

fn state_file() -> Result<PathBuf> {
    let state_home = env::var_os("XDG_STATE_HOME")
        .map(PathBuf::from)
        .or_else(|| env::var_os("HOME").map(|home| PathBuf::from(home).join(".local/state")))
        .context("HOME is unset")?;
    Ok(state_home.join("codex-saver/jobs.json"))
}

#[tokio::main]
async fn main() -> Result<()> {
    match env::args().nth(1).as_deref() {
        Some("hook") => {
            // A missing scheduler must never block or inject text into a Codex turn.
            let _ = hook::forward().await;
            Ok(())
        }
        Some("serve") => {
            let owner_pid: i32 = env::args().nth(2).context("missing daemon pid")?.parse()?;
            scheduler::serve(
                runtime_socket()?,
                control_socket()?,
                state_file()?,
                owner_pid,
            )
            .await
        }
        _ => anyhow::bail!("usage: codex-saver hook"),
    }
}
