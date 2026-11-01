use std::{
    collections::BTreeSet,
    fs,
    path::{Path, PathBuf},
    process::{Child, Command, Output, Stdio},
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};

use futures_util::{StreamExt, future::join_all};
use serde_json::{Value, json};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::{UnixListener, UnixStream},
    time::timeout,
};
use tokio_tungstenite::accept_async;

static NEXT: AtomicU64 = AtomicU64::new(0);

#[tokio::test]
async fn fixture_app_server() {
    let Some(path) = std::env::var_os("CODEX_SAVER_TEST_DAEMON_SOCKET") else {
        return;
    };
    let listener = UnixListener::bind(path).unwrap();
    loop {
        let (stream, _) = listener.accept().await.unwrap();
        tokio::spawn(async move {
            if let Ok(mut socket) = accept_async(stream).await {
                while socket.next().await.is_some() {}
            }
        });
    }
}

struct Harness {
    root: PathBuf,
    daemon: Option<Child>,
}

impl Harness {
    fn new() -> Self {
        let name = format!(
            "{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        );
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("target/l")
            .join(name);
        fs::create_dir_all(root.join("r")).unwrap();
        fs::create_dir_all(root.join("c/app-server-control")).unwrap();
        Self { root, daemon: None }
    }

    fn daemon_path(&self) -> PathBuf {
        self.root
            .join("c/app-server-control/app-server-control.sock")
    }

    async fn start_daemon(&mut self) {
        let path = self.daemon_path();
        let _ = fs::remove_file(&path);
        let child = Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "fixture_app_server", "--nocapture"])
            .env("CODEX_SAVER_TEST_DAEMON_SOCKET", &path)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        self.daemon = Some(child);
        for _ in 0..150 {
            if path.exists() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("fixture app-server socket did not appear");
    }

    fn stop_daemon(&mut self) {
        if let Some(mut child) = self.daemon.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
        let _ = fs::remove_file(self.daemon_path());
    }

    async fn hook(&self, event: Value) -> Output {
        let root = self.root.clone();
        tokio::task::spawn_blocking(move || {
            let child = spawn_hook(root, event, false);
            child.wait_with_output().unwrap()
        })
        .await
        .unwrap()
    }

    fn hook_in_process_group(&self, event: Value) -> Child {
        spawn_hook(self.root.clone(), event, true)
    }

    async fn probe(&self) -> Option<Value> {
        timeout(Duration::from_millis(200), async {
            let stream = UnixStream::connect(self.root.join("r/codex-saver/prompt.sock"))
                .await
                .ok()?;
            let (read, mut write) = stream.into_split();
            write.write_all(b"{\"probe\":true}\n").await.ok()?;
            let mut line = String::new();
            BufReader::new(read).read_line(&mut line).await.ok()?;
            serde_json::from_str(&line).ok()
        })
        .await
        .ok()
        .flatten()
    }

    fn state_path(&self) -> impl AsRef<Path> {
        self.root.join("s/codex-saver/jobs.json")
    }
}

fn spawn_hook(root: PathBuf, event: Value, isolated_process_group: bool) -> Child {
    use std::io::Write;

    let mut command = Command::new(env!("CARGO_BIN_EXE_codex-saver"));
    command
        .arg("hook")
        .env("XDG_RUNTIME_DIR", root.join("r"))
        .env("XDG_STATE_HOME", root.join("s"))
        .env("CODEX_HOME", root.join("c"))
        .env("CODEX_SAVER_DELAY_SECONDS", "3600")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    if isolated_process_group {
        use std::os::unix::process::CommandExt;
        command.process_group(0);
    }
    let mut child = command.spawn().unwrap();
    child
        .stdin
        .take()
        .unwrap()
        .write_all(event.to_string().as_bytes())
        .unwrap();
    child
}

fn kill_process_group(process_group: u32) -> std::io::Result<()> {
    unsafe extern "C" {
        fn kill(pid: i32, signal: i32) -> i32;
    }
    let result = unsafe { kill(-(process_group as i32), 9) };
    if result == 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error())
    }
}

fn process_group(pid: u64) -> Option<u64> {
    let stat = fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    let fields = stat[stat.rfind(')')? + 1..]
        .split_whitespace()
        .collect::<Vec<_>>();
    fields.get(2)?.parse().ok()
}

impl Drop for Harness {
    fn drop(&mut self) {
        self.stop_daemon();
        let _ = fs::remove_dir_all(&self.root);
    }
}

fn stop_event(index: u16) -> Value {
    let session_id = format!("01a19756-b2fe-76f1-bdec-aa909191{index:04x}");
    let turn_id = format!("01a197e3-1f9c-7381-a9d1-dfe47e2c{index:04x}");
    json!({
        "hook_event_name": "Stop",
        "session_id": session_id,
        "turn_id": turn_id,
        "transcript_path": format!("/tmp/rollout-{session_id}.jsonl"),
    })
}

fn event_ids(event: &Value) -> [String; 2] {
    [
        event["session_id"].as_str().unwrap().to_owned(),
        event["turn_id"].as_str().unwrap().to_owned(),
    ]
}

fn contains_json_string(value: &Value, expected: &str) -> bool {
    match value {
        Value::String(value) => value == expected,
        Value::Array(values) => values
            .iter()
            .any(|value| contains_json_string(value, expected)),
        Value::Object(values) => values
            .values()
            .any(|value| contains_json_string(value, expected)),
        _ => false,
    }
}

fn state_contains_ids(harness: &Harness, ids: &[String]) -> bool {
    let Ok(bytes) = fs::read(harness.state_path()) else {
        return false;
    };
    let Ok(state) = serde_json::from_slice::<Value>(&bytes) else {
        return false;
    };
    ids.iter().all(|id| contains_json_string(&state, id))
}

async fn wait_for_state_ids(harness: &Harness, ids: &[String]) {
    let committed = timeout(Duration::from_secs(5), async {
        loop {
            if state_contains_ids(harness, ids) {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .unwrap_or(false);
    assert!(committed, "jobs.json did not contain event IDs {ids:?}");
}

async fn wait_for_helper(harness: &Harness) -> Value {
    timeout(Duration::from_secs(5), async {
        loop {
            if let Some(probe) = harness.probe().await {
                return probe;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("helper did not become available")
}

async fn helper_stopped(harness: &Harness, helper_pid: u64) -> bool {
    let process_gone = timeout(Duration::from_secs(3), async {
        loop {
            if !Path::new(&format!("/proc/{helper_pid}")).exists() {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap_or(false);
    process_gone && harness.probe().await.is_none()
}

fn probe_pid(probe: &Value, name: &str) -> u64 {
    probe[name]
        .as_u64()
        .unwrap_or_else(|| panic!("probe response lacks numeric {name}: {probe}"))
}

fn assert_successful_silent_hook(output: &Output) {
    assert!(
        output.status.success(),
        "hook exited with {}",
        output.status
    );
    assert!(
        output.stdout.is_empty(),
        "hook wrote to stdout: {:?}",
        output.stdout
    );
    assert!(
        output.stderr.is_empty(),
        "hook wrote to stderr: {:?}",
        output.stderr
    );
}

#[tokio::test]
async fn hook_without_daemon_returns_successfully_and_silently() {
    let harness = Harness::new();
    let output = harness.hook(stop_event(1)).await;

    assert_successful_silent_hook(&output);
    assert!(
        harness.probe().await.is_none(),
        "hook started a helper without a daemon"
    );
}

#[tokio::test]
async fn hook_starts_helper_on_demand_and_delivers_its_event() {
    let mut harness = Harness::new();
    harness.start_daemon().await;
    let event = stop_event(2);
    let ids = event_ids(&event);

    let output = harness.hook(event).await;
    assert_successful_silent_hook(&output);

    let probe = wait_for_helper(&harness).await;
    let helper_pid = probe_pid(&probe, "helper_pid");
    wait_for_state_ids(&harness, &ids).await;

    harness.stop_daemon();
    assert!(
        helper_stopped(&harness, helper_pid).await,
        "helper remained after its daemon exited"
    );
}

#[tokio::test]
async fn concurrent_hooks_share_one_helper_and_deliver_every_event() {
    let mut harness = Harness::new();
    harness.start_daemon().await;
    let events: Vec<Value> = (10..18).map(stop_event).collect();
    let expected_ids: Vec<String> = events.iter().flat_map(event_ids).collect();

    let mut hooks = Box::pin(join_all(
        events.into_iter().map(|event| harness.hook(event)),
    ));
    let mut observed_helpers = BTreeSet::new();
    let outputs = loop {
        tokio::select! {
            outputs = &mut hooks => break outputs,
            response = harness.probe() => {
                if let Some(response) = response {
                    observed_helpers.insert((probe_pid(&response, "owner_pid"), probe_pid(&response, "helper_pid")));
                }
            }
        }
    };
    drop(hooks);
    for output in &outputs {
        assert_successful_silent_hook(output);
    }

    let probe = wait_for_helper(&harness).await;
    observed_helpers.insert((
        probe_pid(&probe, "owner_pid"),
        probe_pid(&probe, "helper_pid"),
    ));
    assert_eq!(
        observed_helpers.len(),
        1,
        "concurrent hooks observed multiple helpers: {observed_helpers:?}"
    );
    wait_for_state_ids(&harness, &expected_ids).await;

    let helper_pid = probe_pid(&probe, "helper_pid");
    harness.stop_daemon();
    assert!(
        helper_stopped(&harness, helper_pid).await,
        "helper remained after its daemon exited"
    );
}

#[tokio::test]
async fn helper_survives_termination_of_its_originating_hook_process_group() {
    let mut harness = Harness::new();
    harness.start_daemon().await;
    let event = stop_event(20);
    let ids = event_ids(&event);
    let hook = harness.hook_in_process_group(event);
    let hook_process_group = hook.id();

    let before = wait_for_helper(&harness).await;
    let helper_pid = probe_pid(&before, "helper_pid");
    assert!(
        Path::new(&format!("/proc/{helper_pid}")).exists(),
        "helper exited before hook-group termination"
    );
    assert_ne!(
        process_group(helper_pid),
        Some(u64::from(hook_process_group)),
        "helper shares the originating hook process group"
    );

    let (group_signal, _output) = tokio::task::spawn_blocking(move || {
        let signal = kill_process_group(hook_process_group);
        let output = hook.wait_with_output().unwrap();
        (signal, output)
    })
    .await
    .unwrap();
    assert!(
        group_signal.is_ok()
            || group_signal
                .as_ref()
                .is_err_and(|error| error.kind() == std::io::ErrorKind::NotFound),
        "could not signal the originating hook process group: {group_signal:?}"
    );

    let after = wait_for_helper(&harness).await;
    assert_eq!(
        probe_pid(&after, "owner_pid"),
        probe_pid(&before, "owner_pid")
    );
    assert_eq!(probe_pid(&after, "helper_pid"), helper_pid);
    assert!(
        Path::new(&format!("/proc/{helper_pid}")).exists(),
        "hook-group termination killed the helper"
    );
    wait_for_state_ids(&harness, &ids).await;

    harness.stop_daemon();
    assert!(
        helper_stopped(&harness, helper_pid).await,
        "helper remained after its daemon exited"
    );
}

#[tokio::test]
async fn replacement_daemon_reloads_pending_jobs_and_handles_new_event() {
    let mut harness = Harness::new();
    harness.start_daemon().await;

    let old_event = stop_event(30);
    let old_ids = event_ids(&old_event);
    let output = harness.hook(old_event).await;
    assert_successful_silent_hook(&output);
    let old_probe = wait_for_helper(&harness).await;
    wait_for_state_ids(&harness, &old_ids).await;

    harness.stop_daemon();
    let old_helper_pid = probe_pid(&old_probe, "helper_pid");
    assert!(
        helper_stopped(&harness, old_helper_pid).await,
        "helper remained after its daemon exited"
    );

    harness.start_daemon().await;
    let new_event = stop_event(31);
    let new_ids = event_ids(&new_event);
    let output = harness.hook(new_event).await;
    assert_successful_silent_hook(&output);

    let new_probe = wait_for_helper(&harness).await;
    assert_ne!(
        probe_pid(&new_probe, "owner_pid"),
        probe_pid(&old_probe, "owner_pid")
    );
    assert_ne!(probe_pid(&new_probe, "helper_pid"), old_helper_pid);
    let expected_ids: Vec<String> = old_ids.into_iter().chain(new_ids).collect();
    wait_for_state_ids(&harness, &expected_ids).await;

    harness.stop_daemon();
    assert!(
        helper_stopped(&harness, probe_pid(&new_probe, "helper_pid")).await,
        "replacement helper remained after its daemon exited"
    );
}
