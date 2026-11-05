use std::{path::Path, time::Duration};

use anyhow::Result;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::{UnixDatagram, UnixStream},
    time::timeout,
};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Event {
    pub kind: String,
    pub thread_id: Uuid,
    pub session_id: Uuid,
    pub turn_id: Option<String>,
    pub transcript_path: Option<String>,
    pub subagent: bool,
    #[serde(default)]
    pub prompt_digest: Option<String>,
    #[serde(default)]
    pub observed_at: Option<u64>,
    #[serde(default)]
    pub observed_order: u64,
    #[serde(default)]
    pub plugin_id: Option<String>,
    #[serde(default)]
    pub cwd: Option<String>,
}

fn rollout_thread_id(path: &str) -> Option<Uuid> {
    let stem = Path::new(path).file_stem()?.to_str()?;
    Uuid::parse_str(stem.get(stem.len().checked_sub(36)?..)?).ok()
}

fn event(input: &serde_json::Value) -> Option<Event> {
    let kind = input.get("hook_event_name")?.as_str()?;
    if !matches!(
        kind,
        "Stop"
            | "SubagentStop"
            | "UserPromptSubmit"
            | "SubagentStart"
            | "PreCompact"
            | "PostModelRequest"
    ) {
        return None;
    }
    let session_id = Uuid::parse_str(input.get("session_id")?.as_str()?).ok()?;
    let agent_id = input.get("agent_id").and_then(|id| id.as_str());
    let kind = if kind == "PostModelRequest" {
        if input.get("status").and_then(|status| status.as_str()) != Some("failed") {
            return None;
        }
        if agent_id.is_some() {
            "SubagentStop"
        } else {
            "Stop"
        }
    } else {
        kind
    };
    let transcript_path = input.get("transcript_path").and_then(|path| path.as_str());
    let thread_id = match agent_id {
        Some(id) => Uuid::parse_str(id).ok()?,
        None => rollout_thread_id(transcript_path?)?,
    };
    Some(Event {
        kind: kind.to_owned(),
        thread_id,
        session_id,
        turn_id: input
            .get("turn_id")
            .and_then(|id| id.as_str())
            .map(str::to_owned),
        transcript_path: if agent_id.is_none() {
            transcript_path.map(str::to_owned)
        } else {
            None
        },
        subagent: agent_id.is_some(),
        observed_at: std::env::var("CODEX_SAVER_OBSERVED_AT")
            .ok()
            .and_then(|s| s.parse().ok()),
        observed_order: std::env::var("CODEX_SAVER_OBSERVED_ORDER")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or_else(crate::clock::centiseconds),
        plugin_id: std::env::var("CODEX_SAVER_PLUGIN_ID").ok(),
        cwd: input
            .get("cwd")
            .and_then(|value| value.as_str())
            .map(str::to_owned),
        prompt_digest: if kind == "UserPromptSubmit" && agent_id.is_none() {
            input
                .get("prompt")
                .and_then(|prompt| prompt.as_str())
                .map(|prompt| format!("{:x}", Sha256::digest(prompt.as_bytes())))
        } else {
            None
        },
    })
}

pub async fn forward() -> Result<()> {
    let mut input = Vec::new();
    tokio::io::stdin().read_to_end(&mut input).await?;
    let input = serde_json::from_slice(&input)?;
    if let Some(event) = event(&input) {
        let _ = crate::lifecycle::ensure_running().await;
        let payload = serde_json::to_vec(&event)?;
        if event.kind == "UserPromptSubmit" && event.prompt_digest.is_some() {
            let mut blocked = false;
            let exchange = async {
                let path = crate::runtime_socket()?.with_file_name("prompt.sock");
                let stream = UnixStream::connect(path).await?;
                let (read, mut write) = stream.into_split();
                write.write_all(&payload).await?;
                write.write_all(b"\n").await?;
                let mut reader = BufReader::new(read);
                let mut response = String::new();
                reader.read_line(&mut response).await?;
                let output: serde_json::Value = serde_json::from_str(&response)?;
                if output["decision"] == "block" {
                    let mut stdout = tokio::io::stdout();
                    stdout.write_all(response.as_bytes()).await?;
                    stdout.flush().await?;
                    blocked = true;
                    write.write_all(b"ack\n").await?;
                    let mut acknowledgement = String::new();
                    reader.read_line(&mut acknowledgement).await?;
                }
                Ok::<(), anyhow::Error>(())
            };
            if matches!(
                timeout(Duration::from_millis(1800), exchange).await,
                Ok(Ok(()))
            ) || blocked
            {
                return Ok(());
            }
        }
        let socket = UnixDatagram::unbound()?;
        let _ = timeout(
            Duration::from_millis(100),
            socket.send_to(&payload, crate::runtime_socket()?),
        )
        .await;
    }
    Ok(())
}
