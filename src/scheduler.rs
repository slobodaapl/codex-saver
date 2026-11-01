use std::{
    collections::HashMap,
    io::ErrorKind,
    os::unix::fs::{FileTypeExt, OpenOptionsExt, PermissionsExt},
    path::{Path, PathBuf},
    time::Duration,
};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::{UnixDatagram, UnixListener, UnixStream},
    sync::{mpsc, oneshot},
    task::JoinHandle,
    time::timeout,
};
use uuid::Uuid;

use crate::{
    hook::Event,
    remote::{self, Outcome, PromptAge},
};

const DEFAULT_DELAY: u64 = 25 * 60;
const DEFAULT_RETRY: u64 = 10 * 60;
const DEFAULT_WINDOW: u64 = 30 * 60;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Job {
    pub thread_id: Uuid,
    pub session_id: Uuid,
    pub turn_id: String,
    #[serde(default)]
    pub alternate_turn_ids: Vec<String>,
    pub transcript_path: Option<String>,
    pub subagent: bool,
    pub deadline: u64,
    #[serde(default)]
    pub expires_at: u64,
    pub generation: u64,
    #[serde(default)]
    pub boot_id: String,
    #[serde(default)]
    pub plugin_id: Option<String>,
    #[serde(default)]
    pub observed_order: u64,
    #[serde(default)]
    pub cwd: Option<String>,
    #[serde(default)]
    dispatched: bool,
    #[serde(skip)]
    inflight: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Activity {
    pub boot_id: String,
    pub turn_id: String,
    #[serde(default)]
    pub alternate_turn_ids: Vec<String>,
    pub observed_at: u64,
    #[serde(default)]
    pub observed_order: u64,
}

pub enum WorkerEvent {
    Dispatch(Uuid, u64, oneshot::Sender<bool>),
    Finished(Uuid, u64, Outcome),
    Prompt(Event, oneshot::Sender<PromptDecision>),
    PromptResult(
        Uuid,
        u64,
        Result<PromptAge, String>,
        oneshot::Sender<PromptDecision>,
    ),
    Confirm(Uuid, u64, String, oneshot::Sender<()>),
}

pub struct PromptDecision {
    reason: Option<String>,
    revision: u64,
}

pub(crate) fn now() -> u64 {
    crate::clock::now()
}

fn interval(name: &str, default: u64) -> Result<u64> {
    match std::env::var(name) {
        Ok(value) => value.parse().with_context(|| format!("invalid {name}")),
        Err(std::env::VarError::NotPresent) => Ok(default),
        Err(error) => Err(error.into()),
    }
}

async fn save<T: Serialize>(path: &Path, data: &T) -> Result<()> {
    let parent = path.parent().context("state file has no parent")?;
    tokio::fs::create_dir_all(parent).await?;
    let tmp = path.with_extension("tmp");
    tokio::fs::write(&tmp, serde_json::to_vec(data)?).await?;
    tokio::fs::rename(&tmp, path).await?;
    Ok(())
}

async fn bind(path: &Path) -> Result<UnixDatagram> {
    let parent = path.parent().context("socket has no parent")?;
    tokio::fs::create_dir_all(parent).await?;
    tokio::fs::set_permissions(parent, std::fs::Permissions::from_mode(0o700)).await?;
    if let Ok(metadata) = tokio::fs::symlink_metadata(path).await {
        anyhow::ensure!(
            metadata.file_type().is_socket(),
            "socket path is occupied by another file"
        );
        let probe = UnixDatagram::unbound()?;
        match probe.send_to(&[], path).await {
            Ok(_) => anyhow::bail!("another codex-saver is running"),
            Err(error)
                if matches!(
                    error.kind(),
                    ErrorKind::ConnectionRefused | ErrorKind::NotFound
                ) =>
            {
                let _ = tokio::fs::remove_file(path).await;
            }
            Err(error) => return Err(error.into()),
        }
    }
    let socket = UnixDatagram::bind(path)?;
    tokio::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)).await?;
    Ok(socket)
}

async fn bind_prompt(path: &Path) -> Result<UnixListener> {
    if let Ok(metadata) = tokio::fs::symlink_metadata(path).await {
        anyhow::ensure!(
            metadata.file_type().is_socket(),
            "prompt socket path is occupied"
        );
        match UnixStream::connect(path).await {
            Ok(_) => anyhow::bail!("another prompt listener is running"),
            Err(error)
                if matches!(
                    error.kind(),
                    ErrorKind::ConnectionRefused | ErrorKind::NotFound
                ) =>
            {
                let _ = tokio::fs::remove_file(path).await;
            }
            Err(error) => return Err(error.into()),
        }
    }
    let listener = UnixListener::bind(path)?;
    tokio::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)).await?;
    Ok(listener)
}

async fn handle_prompt(
    stream: UnixStream,
    events: mpsc::UnboundedSender<WorkerEvent>,
    owner_pid: i32,
) -> Result<()> {
    let (read, mut write) = stream.into_split();
    let mut reader = BufReader::new(read);
    let mut line = String::new();
    timeout(Duration::from_secs(2), reader.read_line(&mut line)).await??;
    let value: serde_json::Value = serde_json::from_str(&line)?;
    if value["probe"] == true {
        write
            .write_all(
                serde_json::json!({"owner_pid": owner_pid, "helper_pid": std::process::id()})
                    .to_string()
                    .as_bytes(),
            )
            .await?;
        write.write_all(b"\n").await?;
        return Ok(());
    }
    let event: Event = serde_json::from_value(value)?;
    anyhow::ensure!(
        event.kind == "UserPromptSubmit" && !event.subagent,
        "invalid prompt event"
    );
    let id = event.thread_id;
    let digest = event
        .prompt_digest
        .clone()
        .context("missing prompt fingerprint")?;
    let (send, receive) = oneshot::channel();
    events.send(WorkerEvent::Prompt(event, send))?;
    let decision = timeout(Duration::from_millis(1500), receive).await??;
    let response = match &decision.reason {
        Some(reason) => serde_json::json!({"decision":"block","reason":reason}),
        None => serde_json::json!({}),
    };
    write.write_all(response.to_string().as_bytes()).await?;
    write.write_all(b"\n").await?;
    if decision.reason.is_some() {
        line.clear();
        timeout(Duration::from_millis(300), reader.read_line(&mut line)).await??;
        anyhow::ensure!(line.trim() == "ack", "prompt acknowledgement missing");
        let (send, receive) = oneshot::channel();
        events.send(WorkerEvent::Confirm(id, decision.revision, digest, send))?;
        timeout(Duration::from_millis(300), receive).await??;
        write.write_all(b"ok\n").await?;
    }
    Ok(())
}

pub async fn serve(
    socket_path: PathBuf,
    control_path: PathBuf,
    state_path: PathBuf,
    owner_pid: i32,
) -> Result<()> {
    let runtime_dir = socket_path.parent().context("socket has no parent")?;
    std::fs::create_dir_all(runtime_dir)?;
    std::fs::set_permissions(runtime_dir, std::fs::Permissions::from_mode(0o700))?;
    let singleton = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .mode(0o600)
        .open(runtime_dir.join("helper.lock"))?;
    rustix::fs::flock(
        &singleton,
        rustix::fs::FlockOperation::NonBlockingLockExclusive,
    )
    .context("another helper is starting or running")?;
    let owner = crate::lifecycle::watch_daemon(&control_path, owner_pid).await?;
    let delay = interval("CODEX_SAVER_DELAY_SECONDS", DEFAULT_DELAY)?;
    let retry = interval("CODEX_SAVER_RETRY_SECONDS", DEFAULT_RETRY)?;
    let window = interval("CODEX_SAVER_WINDOW_SECONDS", DEFAULT_WINDOW)?;
    let boot_id = crate::clock::boot_id()?;
    let timer = crate::clock::Timer::new()?;
    let socket = bind(&socket_path).await?;
    let prompt_socket = bind_prompt(&socket_path.with_file_name("prompt.sock")).await?;
    let mut jobs: HashMap<Uuid, Job> = match tokio::fs::read(&state_path).await {
        Ok(bytes) => serde_json::from_slice(&bytes).context("invalid saved jobs")?,
        Err(error) if error.kind() == ErrorKind::NotFound => HashMap::new(),
        Err(error) => return Err(error.into()),
    };
    let activity_path = state_path.with_file_name("activity.json");
    let mut activities: HashMap<Uuid, Activity> = match tokio::fs::read(&activity_path).await {
        Ok(bytes) => serde_json::from_slice(&bytes).context("invalid saved activity")?,
        Err(error) if error.kind() == ErrorKind::NotFound => HashMap::new(),
        Err(error) => return Err(error.into()),
    };
    activities.retain(|_, activity| activity.boot_id == boot_id);
    jobs.retain(|id, job| {
        if job.dispatched {
            eprintln!("{id}: previous dispatched request has an unknown outcome");
            false
        } else if job.expires_at == 0 || job.boot_id != boot_id {
            eprintln!("{id}: saved job has no valid deadline for this boot");
            false
        } else {
            true
        }
    });
    save(&state_path, &jobs).await?;
    let mut generation = jobs
        .values()
        .map(|job| job.generation)
        .max()
        .unwrap_or_default();
    let (result_tx, mut result_rx) = mpsc::unbounded_channel::<WorkerEvent>();
    let mut workers: HashMap<Uuid, (u64, JoinHandle<()>)> = HashMap::new();
    let mut confirmations: HashMap<Uuid, String> = HashMap::new();
    let mut prompt_revisions: HashMap<Uuid, u64> = HashMap::new();
    let mut buffer = [0u8; 16384];

    loop {
        let next = jobs
            .values()
            .filter(|job| !job.inflight)
            .map(|job| job.deadline)
            .min();
        timer.set(next)?;
        tokio::select! {
            biased;
            result = owner.readable() => {
                let _ = result?;
                eprintln!("app-server daemon exited; stopping helper");
                return Ok(());
            }
            accepted = prompt_socket.accept() => {
                let (stream, _) = accepted?;
                let tx = result_tx.clone();
                tokio::spawn(async move { let _ = handle_prompt(stream, tx, owner_pid).await; });
            }
            received = socket.recv(&mut buffer) => {
                let n = received?;
                let Ok(event) = serde_json::from_slice::<Event>(&buffer[..n]) else { continue; };
                match event.kind.as_str() {
                    "Stop" | "SubagentStop" => {
                        if (event.kind == "SubagentStop") != event.subagent { continue; }
                        let Some(turn_id) = event.turn_id.filter(|id| !id.is_empty()) else { continue; };
                        let observed_at = event.observed_at.unwrap_or_else(now);
                        if observed_at > now() { continue; }
                        let observed_order = if event.observed_order == 0 { observed_at.saturating_mul(100) } else { event.observed_order };
                        if observed_order > crate::clock::centiseconds() { continue; }
                        if jobs.get(&event.thread_id).is_some_and(|job| job.observed_order > observed_order)
                            || activities.get(&event.thread_id).is_some_and(|activity| activity.observed_order > observed_order) { continue; }
                        let alternate_turn_ids = jobs.get(&event.thread_id)
                            .filter(|job| job.observed_order == observed_order && job.session_id == event.session_id && job.subagent == event.subagent && job.transcript_path == event.transcript_path)
                            .map(|job| {
                                let mut ids = job.alternate_turn_ids.clone();
                                if job.turn_id != turn_id && !ids.contains(&job.turn_id) { ids.push(job.turn_id.clone()); }
                                ids
                            }).unwrap_or_default();
                        if let Some((_, handle)) = workers.remove(&event.thread_id) { handle.abort(); }
                        confirmations.remove(&event.thread_id);
                        *prompt_revisions.entry(event.thread_id).or_default() += 1;
                        generation = generation.wrapping_add(1);
                        if !event.subagent {
                            activities.insert(event.thread_id, Activity {
                                boot_id: boot_id.clone(),
                                turn_id: turn_id.clone(),
                                alternate_turn_ids: alternate_turn_ids.clone(),
                                observed_at,
                                observed_order,
                            });
                            save(&activity_path, &activities).await?;
                        }
                        jobs.insert(event.thread_id, Job {
                            thread_id: event.thread_id,
                            session_id: event.session_id,
                            turn_id,
                            alternate_turn_ids,
                            transcript_path: event.transcript_path,
                            subagent: event.subagent,
                            deadline: observed_at.saturating_add(delay),
                            expires_at: observed_at.saturating_add(window),
                            generation,
                            boot_id: boot_id.clone(),
                            plugin_id: event.plugin_id,
                            observed_order,
                            cwd: event.cwd,
                            dispatched: false,
                            inflight: false,
                        });
                    }
                    "UserPromptSubmit" | "SubagentStart" | "PreCompact" => {
                        // Our own compaction invokes PreCompact after dispatch.
                        let own_compaction = event.kind == "PreCompact"
                            && jobs.get(&event.thread_id).is_some_and(|job| job.dispatched);
                        if !own_compaction {
                            if let Some((_, handle)) = workers.remove(&event.thread_id) { handle.abort(); }
                            jobs.remove(&event.thread_id);
                            confirmations.remove(&event.thread_id);
                            *prompt_revisions.entry(event.thread_id).or_default() += 1;
                        }
                    }
                    _ => continue,
                }
                save(&state_path, &jobs).await?;
            }
            Some(message) = result_rx.recv() => match message {
              WorkerEvent::Prompt(event, reply) => {
                  if let Some((_, handle)) = workers.remove(&event.thread_id) { handle.abort(); }
                  jobs.remove(&event.thread_id);
                  save(&state_path, &jobs).await?;
                  let revision = prompt_revisions.entry(event.thread_id).or_default();
                  *revision += 1;
                  let revision = *revision;
                  let Some(digest) = event.prompt_digest.clone() else { let _ = reply.send(PromptDecision { reason:None, revision }); continue; };
                  if confirmations.remove(&event.thread_id).as_deref() == Some(digest.as_str()) {
                      let _ = reply.send(PromptDecision { reason:None, revision });
                  } else {
                      let tx = result_tx.clone();
                      let path = control_path.clone();
                      let activity = activities.get(&event.thread_id).cloned();
                      tokio::spawn(async move {
                          let result = match timeout(Duration::from_millis(1200), remote::prompt_age(&path, &event, window, activity.as_ref())).await {
                              Ok(result) => result,
                              Err(_) => Err("prompt history check timed out".into()),
                          };
                          let _ = tx.send(WorkerEvent::PromptResult(event.thread_id, revision, result, reply));
                      });
                  }
              }
              WorkerEvent::PromptResult(id, revision, result, reply) => {
                  let reason = if prompt_revisions.get(&id) != Some(&revision) { None } else { match result {
                      Ok(PromptAge::Fresh) => None,
                      Ok(PromptAge::Stale) => Some("This thread has been idle past its cache window and has not been compacted. Use /clear to start fresh, or resubmit the same message to continue.".to_owned()),
                      Ok(PromptAge::Unknown) => Some("The age of this thread's context is unknown. Use /clear to start fresh, or resubmit the same message to continue.".to_owned()),
                      Err(error) => { eprintln!("{id}: prompt history unavailable: {error}"); None }
                  }};
                  let _ = reply.send(PromptDecision { reason, revision });
              }
              WorkerEvent::Confirm(id, revision, digest, reply) => {
                  if prompt_revisions.get(&id) == Some(&revision) {
                      confirmations.insert(id, digest);
                  }
                  let _ = reply.send(());
              }
              WorkerEvent::Dispatch(id, job_generation, reply) => {
                  let current = jobs.get(&id).is_some_and(|job| job.generation == job_generation);
                  if current {
                      jobs.get_mut(&id).expect("checked above").dispatched = true;
                      save(&state_path, &jobs).await?;
                  }
                  let _ = reply.send(current);
              }
              WorkerEvent::Finished(id, job_generation, outcome) => {
                if workers.get(&id).is_some_and(|(generation, _)| *generation == job_generation) {
                    workers.remove(&id);
                }
                let current = jobs.get(&id).is_some_and(|job| job.generation == job_generation);
                match &outcome {
                    Outcome::Done(action) => eprintln!("{id}: {action}"),
                    Outcome::Retry(reason) => eprintln!("{id}: retry in {retry}s: {reason}"),
                    Outcome::Discard(reason) => eprintln!("{id}: skipped: {reason}"),
                    Outcome::Unknown(reason) => eprintln!("{id}: {reason}"),
                }
                if current {
                    match outcome {
                        Outcome::Retry(_) => {
                            let job = jobs.get_mut(&id).expect("checked above");
                            job.deadline = now().saturating_add(retry);
                            job.inflight = false;
                            job.dispatched = false;
                        }
                        _ => { jobs.remove(&id); }
                    }
                    save(&state_path, &jobs).await?;
                }
              }
            },
            result = timer.wait() => {
                result?;
                jobs.retain(|id, job| {
                    let expired_root = !job.subagent && !job.inflight && now() >= job.expires_at;
                    if expired_root { eprintln!("{id}: cache window expired; skipping compaction"); }
                    !expired_root
                });
                save(&state_path, &jobs).await?;
                let due: Vec<_> = jobs.values_mut().filter(|job| !job.inflight && job.deadline <= now())
                    .map(|job| { job.inflight = true; job.clone() }).collect();
                for job in due {
                    let tx = result_tx.clone();
                    let path = control_path.clone();
                    let id = job.thread_id;
                    let job_generation = job.generation;
                    let handle = tokio::spawn(async move {
                        let outcome = remote::process(&path, &job, &tx).await;
                        let _ = tx.send(WorkerEvent::Finished(id, job_generation, outcome));
                    });
                    workers.insert(id, (job_generation, handle));
                }
            }
        }
    }
}
