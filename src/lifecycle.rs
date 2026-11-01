use std::{
    fs::{self, OpenOptions},
    os::unix::{
        fs::{OpenOptionsExt, PermissionsExt},
        process::CommandExt,
    },
    process::{Command, Stdio},
    time::Duration,
};

use anyhow::{Context, Result};
use tokio::{
    io::unix::AsyncFd,
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::UnixStream,
    time::{Instant, sleep, timeout},
};
use tokio_tungstenite::client_async;

async fn probe() -> Option<i32> {
    timeout(Duration::from_millis(100), async {
        let path = crate::runtime_socket().ok()?.with_file_name("prompt.sock");
        let stream = UnixStream::connect(path).await.ok()?;
        let (read, mut write) = stream.into_split();
        write.write_all(b"{\"probe\":true}\n").await.ok()?;
        let mut line = String::new();
        BufReader::new(read).read_line(&mut line).await.ok()?;
        let value: serde_json::Value = serde_json::from_str(&line).ok()?;
        i32::try_from(value["owner_pid"].as_i64()?).ok()
    })
    .await
    .ok()
    .flatten()
}

async fn daemon_pid() -> Result<i32> {
    timeout(Duration::from_millis(400), async {
        let stream = UnixStream::connect(crate::control_socket()?).await?;
        let pid = stream
            .peer_cred()?
            .pid()
            .context("daemon peer pid unavailable")?;
        let (mut websocket, _) = client_async("ws://localhost/", stream).await?;
        websocket.close(None).await?;
        Ok::<i32, anyhow::Error>(pid)
    })
    .await
    .context("daemon connection timed out")?
}

pub async fn watch_daemon(
    path: &std::path::Path,
    expected_pid: i32,
) -> Result<AsyncFd<rustix::fd::OwnedFd>> {
    timeout(Duration::from_millis(400), async {
        let stream = UnixStream::connect(path).await?;
        let peer_pid = stream
            .peer_cred()?
            .pid()
            .context("daemon peer pid unavailable")?;
        anyhow::ensure!(
            peer_pid == expected_pid,
            "app-server daemon changed during helper startup"
        );
        let pid = rustix::process::Pid::from_raw(peer_pid).context("invalid daemon pid")?;
        let pidfd = rustix::process::pidfd_open(pid, rustix::process::PidfdFlags::empty())?;
        let (mut websocket, _) = client_async("ws://localhost/", stream).await?;
        websocket.close(None).await?;
        // OwnedFd stays open and unchanged while registered with the reactor.
        Ok(unsafe { AsyncFd::register_with_interest(pidfd, tokio::io::Interest::READABLE) }?)
    })
    .await
    .context("daemon verification timed out")?
}

fn log_output() -> Stdio {
    let result = (|| -> Result<_> {
        let path = crate::state_file()?.with_file_name("helper.log");
        let directory = path.parent().context("log directory missing")?;
        fs::create_dir_all(directory)?;
        fs::set_permissions(directory, fs::Permissions::from_mode(0o700))?;
        Ok(OpenOptions::new()
            .create(true)
            .append(true)
            .mode(0o600)
            .open(path)?)
    })();
    result.map_or_else(|_| Stdio::null(), Stdio::from)
}

pub async fn ensure_running() -> Result<()> {
    if probe().await.is_some() {
        return Ok(());
    }
    let owner = daemon_pid().await?;
    Command::new(std::env::current_exe()?)
        .arg("serve")
        .arg(owner.to_string())
        .process_group(0)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(log_output())
        .spawn()?;
    let deadline = Instant::now() + Duration::from_millis(700);
    while Instant::now() < deadline {
        if probe().await == Some(owner) {
            return Ok(());
        }
        sleep(Duration::from_millis(10)).await;
    }
    anyhow::bail!("helper did not start")
}
