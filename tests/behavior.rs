use std::{
    fs,
    path::{Path, PathBuf},
    process::{Child, Command, Output, Stdio},
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use tokio::{
    net::UnixListener,
    sync::{
        mpsc::{self, UnboundedReceiver},
        oneshot,
    },
    task::JoinHandle,
    time::{Instant, timeout_at},
};
use tokio_tungstenite::{accept_async, tungstenite::Message};

static NEXT: AtomicU64 = AtomicU64::new(0);
const SESSION: &str = "01a19756-b2fe-76f1-bdec-aa9091918001";
const ROOT: &str = "01a19756-b2fe-76f1-bdec-aa9091918001";
const CHILD: &str = "01a197e3-6cac-7071-aece-f44786810b43";
const TURN: &str = "01a197e3-1f9c-7381-a9d1-dfe47e2c66dd";
const NEW_TURN: &str = "01a197e3-4a70-72e4-bddb-ef456f348be1";
const ROOT_PATH: &str =
    "/codex/sessions/rollout-2026-11-01T00-00-00-000Z-01a19756-b2fe-76f1-bdec-aa9091918001.jsonl";
const FORK: &str = "01a19800-b5e6-7b5d-a06b-3b7e395b8ba9";
const FORK_PATH: &str =
    "/codex/sessions/rollout-2026-11-01T00-00-00-000Z-01a19800-b5e6-7b5d-a06b-3b7e395b8ba9.jsonl";
const PLUGIN_ID: &str = "codex-saver@codex-saver";
const OBSERVED_METHODS: &[&str] = &[
    "plugin/installed",
    "thread/read",
    "thread/turns/list",
    "thread/items/list",
    "thread/list",
    "thread/resume",
    "thread/compact/start",
    "thread/delete",
];

fn spawn_service(
    root: &Path,
    delay: u64,
    retry: u64,
    window: Option<u64>,
    stderr: &Arc<Mutex<Vec<u8>>>,
) -> (Child, std::thread::JoinHandle<()>) {
    let control = root.join("c/app-server-control/app-server-control.sock");
    let setup = if !control.exists() {
        let listener = std::os::unix::net::UnixListener::bind(&control).unwrap();
        listener.set_nonblocking(true).unwrap();
        let path = control.clone();
        Some(std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            runtime.block_on(async move {
                let listener = UnixListener::from_std(listener).unwrap();
                let (stream, _) = tokio::time::timeout(Duration::from_secs(3), listener.accept())
                    .await
                    .unwrap()
                    .unwrap();
                let mut socket = accept_async(stream).await.unwrap();
                let _ = tokio::time::timeout(Duration::from_secs(3), socket.next()).await;
                drop(listener);
                fs::remove_file(path).unwrap();
            });
        }))
    } else {
        None
    };
    let mut command = Command::new(env!("CARGO_BIN_EXE_codex-saver"));
    command
        .arg("serve")
        .arg(std::process::id().to_string())
        .env("XDG_RUNTIME_DIR", root.join("runtime"))
        .env("XDG_STATE_HOME", root.join("state"))
        .env("CODEX_HOME", root.join("c"))
        .env("CODEX_SAVER_DELAY_SECONDS", delay.to_string())
        .env("CODEX_SAVER_RETRY_SECONDS", retry.to_string())
        .env_remove("CODEX_SAVER_WINDOW_SECONDS")
        .stderr(Stdio::piped());
    if let Some(window) = window {
        command.env("CODEX_SAVER_WINDOW_SECONDS", window.to_string());
    }
    if root.join("realtime-offset.so").exists() {
        command
            .env("LD_PRELOAD", root.join("realtime-offset.so"))
            .env(
                "CODEX_SAVER_TEST_REALTIME_OFFSET_FILE",
                root.join("realtime-offset"),
            );
    }
    let mut service = command.spawn().unwrap();
    if let Some(setup) = setup {
        setup
            .join()
            .expect("test app-server startup handshake failed");
    }
    let mut stderr_pipe = service.stderr.take().unwrap();
    let captured_stderr = Arc::clone(stderr);
    let stderr_reader = std::thread::spawn(move || {
        use std::io::Read;
        let mut buffer = [0; 4096];
        loop {
            match stderr_pipe.read(&mut buffer) {
                Ok(0) | Err(_) => break,
                Ok(read) => captured_stderr
                    .lock()
                    .unwrap()
                    .extend_from_slice(&buffer[..read]),
            }
        }
    });
    (service, stderr_reader)
}

async fn scheduler_socket_ready(root: &Path) -> bool {
    for _ in 0..150 {
        if root.join("runtime/codex-saver/socket").exists() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    false
}

fn find_jobs_json(directory: &Path) -> Option<PathBuf> {
    let mut pending = vec![directory.to_path_buf()];
    while let Some(current) = pending.pop() {
        let entries = fs::read_dir(current).ok()?;
        for entry in entries.flatten() {
            let path = entry.path();
            let Ok(file_type) = entry.file_type() else {
                continue;
            };
            if file_type.is_dir() {
                pending.push(path);
            } else if path.file_name().and_then(|name| name.to_str()) == Some("jobs.json") {
                return Some(path);
            }
        }
    }
    None
}

fn json_contains_string(value: &Value, expected: &str) -> bool {
    match value {
        Value::String(value) => value == expected,
        Value::Array(values) => values
            .iter()
            .any(|value| json_contains_string(value, expected)),
        Value::Object(values) => values
            .values()
            .any(|value| json_contains_string(value, expected)),
        _ => false,
    }
}

fn committed_jobs_include_turn(state_home: &Path, turn_id: &str) -> Option<PathBuf> {
    let jobs = find_jobs_json(state_home)?;
    let contents = fs::read(&jobs).ok()?;
    let value: Value = serde_json::from_slice(&contents).ok()?;
    json_contains_string(&value, turn_id).then_some(jobs)
}

async fn wait_for_persisted_turn(state_home: &Path, turn_id: &str) -> PathBuf {
    let deadline = Instant::now() + Duration::from_secs(3);
    loop {
        if let Some(file) = committed_jobs_include_turn(state_home, turn_id) {
            return file;
        }
        assert!(
            Instant::now() < deadline,
            "scheduled turn {turn_id} was not committed in jobs.json under {state_home:?}"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

fn persisted_jobs_contain_turn(jobs_file: &Path, turn_id: &str) -> bool {
    match fs::read(jobs_file) {
        Ok(bytes) => match serde_json::from_slice::<Value>(&bytes) {
            Ok(value) => json_contains_string(&value, turn_id),
            Err(_) => true,
        },
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => false,
        Err(error) => panic!(
            "failed to read persisted jobs {}: {error}",
            jobs_file.display()
        ),
    }
}

fn set_persisted_job_boot_id(state_home: &Path, turn_id: &str, boot_id: Option<&str>) -> PathBuf {
    let path = find_jobs_json(state_home).expect("jobs.json was not created");
    let bytes = fs::read(&path).unwrap();
    let mut jobs: Value = serde_json::from_slice(&bytes).unwrap();
    let job = jobs
        .as_object_mut()
        .unwrap()
        .values_mut()
        .find(|job| job["turn_id"] == turn_id)
        .unwrap_or_else(|| panic!("turn {turn_id} was not saved in {}", path.display()));
    let fields = job.as_object_mut().unwrap();
    match boot_id {
        Some(boot_id) => {
            assert!(fields.get("boot_id").and_then(Value::as_str).is_some());
            fields.insert("boot_id".to_owned(), json!(boot_id));
        }
        None => {
            assert!(fields.remove("boot_id").is_some());
        }
    }
    fs::write(&path, serde_json::to_vec(&jobs).unwrap()).unwrap();
    path
}

struct Harness {
    root: PathBuf,
    window: Option<u64>,
    service: Child,
    stderr: Arc<Mutex<Vec<u8>>>,
    stderr_reader: Option<std::thread::JoinHandle<()>>,
}

impl Harness {
    async fn start(delay: u64, retry: u64) -> Self {
        Self::start_with_window(delay, retry, None).await
    }

    async fn start_with_window(delay: u64, retry: u64, window: Option<u64>) -> Self {
        Self::start_configured(delay, retry, window, false).await
    }

    async fn start_clocked(delay: u64, retry: u64, window: Option<u64>) -> Self {
        Self::start_configured(delay, retry, window, true).await
    }

    async fn start_configured(
        delay: u64,
        retry: u64,
        window: Option<u64>,
        shifted_clock: bool,
    ) -> Self {
        let unique = format!(
            "{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        );
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("target/c")
            .join(unique);
        fs::create_dir_all(root.join("runtime")).unwrap();
        fs::create_dir_all(root.join("c/app-server-control")).unwrap();
        if shifted_clock {
            let fixture =
                PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/realtime_offset.c");
            let result = Command::new("cc")
                .args(["-shared", "-fPIC", "-O2"])
                .arg(fixture)
                .arg("-ldl")
                .arg("-o")
                .arg(root.join("realtime-offset.so"))
                .output()
                .unwrap();
            assert!(
                result.status.success(),
                "clock fixture compilation failed: {}",
                String::from_utf8_lossy(&result.stderr)
            );
            fs::write(root.join("realtime-offset"), "0").unwrap();
        }
        let stderr = Arc::new(Mutex::new(Vec::new()));
        let (mut service, stderr_reader) = spawn_service(&root, delay, retry, window, &stderr);
        if scheduler_socket_ready(&root).await {
            return Self {
                root,
                window,
                service,
                stderr,
                stderr_reader: Some(stderr_reader),
            };
        }
        let message = String::from_utf8_lossy(&stderr.lock().unwrap()).into_owned();
        let _ = service.kill();
        let _ = service.wait();
        let _ = stderr_reader.join();
        let _ = fs::remove_dir_all(&root);
        panic!("scheduler socket did not appear; service stderr: {message}");
    }

    fn set_realtime_offset(&self, seconds: i64) {
        fs::write(self.root.join("realtime-offset"), seconds.to_string()).unwrap();
    }

    async fn restart(&mut self, delay: u64, retry: u64) {
        self.stop_service();
        let (service, stderr_reader) =
            spawn_service(&self.root, delay, retry, self.window, &self.stderr);
        self.service = service;
        self.stderr_reader = Some(stderr_reader);
        if !scheduler_socket_ready(&self.root).await {
            let message = self.stderr_text();
            self.stop_service();
            panic!("scheduler socket did not reappear; service stderr: {message}");
        }
    }

    async fn hook_with_timeout(&self, event: Value, duration: Duration) {
        let runtime = self.root.join("runtime");
        let codex_home = self.root.join("c");
        let state_home = self.root.join("state");
        let input = event.to_string();
        let task = tokio::task::spawn_blocking(move || {
            use std::io::Write;
            let mut child = Command::new(env!("CARGO_BIN_EXE_codex-saver"))
                .arg("hook")
                .env("XDG_RUNTIME_DIR", runtime)
                .env("CODEX_HOME", codex_home)
                .env("XDG_STATE_HOME", state_home)
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .spawn()?;
            child.stdin.take().unwrap().write_all(input.as_bytes())?;
            child.wait_with_output()
        });
        let output = tokio::time::timeout(duration, task)
            .await
            .expect("hook did not return while service RPC was pending")
            .expect("hook task panicked")
            .unwrap();
        assert!(output.status.success());
        assert!(output.stdout.is_empty());
    }

    async fn hook_output(&self, event: Value) -> Output {
        let runtime = self.root.join("runtime");
        let codex_home = self.root.join("c");
        let state_home = self.root.join("state");
        tokio::task::spawn_blocking(move || {
            use std::io::Write;
            let mut child = Command::new(env!("CARGO_BIN_EXE_codex-saver"))
                .arg("hook")
                .env("XDG_RUNTIME_DIR", runtime)
                .env("CODEX_HOME", codex_home)
                .env("XDG_STATE_HOME", state_home)
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .spawn()
                .unwrap();
            child
                .stdin
                .take()
                .unwrap()
                .write_all(event.to_string().as_bytes())
                .unwrap();
            child.wait_with_output().unwrap()
        })
        .await
        .unwrap()
    }

    fn listener(&self) -> UnixListener {
        UnixListener::bind(
            self.root
                .join("c/app-server-control/app-server-control.sock"),
        )
        .unwrap()
    }

    fn hook(&self, event: Value) {
        let result = self.hook_raw(&event.to_string());
        assert!(result.status.success());
        assert!(result.stdout.is_empty());
    }

    fn hook_raw(&self, input: &str) -> Output {
        self.hook_raw_with_metadata(input, None, None)
    }

    fn hook_with_metadata(
        &self,
        event: Value,
        plugin_id: Option<&str>,
        observed_at: Option<u64>,
    ) -> Output {
        self.hook_raw_with_metadata(&event.to_string(), plugin_id, observed_at)
    }

    fn hook_with_order(
        &self,
        event: Value,
        plugin_id: Option<&str>,
        observed_at: Option<u64>,
        observed_order: Option<u64>,
    ) -> Output {
        self.hook_raw_with_order(&event.to_string(), plugin_id, observed_at, observed_order)
    }

    fn hook_raw_with_metadata(
        &self,
        input: &str,
        plugin_id: Option<&str>,
        observed_at: Option<u64>,
    ) -> Output {
        self.hook_raw_with_order(input, plugin_id, observed_at, None)
    }

    fn hook_raw_with_order(
        &self,
        input: &str,
        plugin_id: Option<&str>,
        observed_at: Option<u64>,
        observed_order: Option<u64>,
    ) -> Output {
        use std::io::Write;
        let mut command = Command::new(env!("CARGO_BIN_EXE_codex-saver"));
        command
            .arg("hook")
            .env("XDG_RUNTIME_DIR", self.root.join("runtime"))
            .env("CODEX_HOME", self.root.join("c"))
            .env("XDG_STATE_HOME", self.root.join("state"));
        if let Some(plugin_id) = plugin_id {
            command.env("CODEX_SAVER_PLUGIN_ID", plugin_id);
        }
        if let Some(observed_at) = observed_at {
            command.env("CODEX_SAVER_OBSERVED_AT", observed_at.to_string());
        }
        if let Some(observed_order) = observed_order {
            command.env("CODEX_SAVER_OBSERVED_ORDER", observed_order.to_string());
        }
        let mut child = command
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()
            .unwrap();
        child
            .stdin
            .take()
            .unwrap()
            .write_all(input.as_bytes())
            .unwrap();
        child.wait_with_output().unwrap()
    }

    fn stop_service(&mut self) {
        if self.service.try_wait().unwrap().is_none() {
            self.service.kill().unwrap();
            self.service.wait().unwrap();
        }
        if let Some(reader) = self.stderr_reader.take() {
            let _ = reader.join();
        }
        let _ = fs::remove_file(self.root.join("runtime/codex-saver/socket"));
    }

    fn assert_service_running(&mut self) {
        assert!(
            self.service.try_wait().unwrap().is_none(),
            "service exited unexpectedly"
        );
    }

    fn stderr_text(&self) -> String {
        String::from_utf8_lossy(&self.stderr.lock().unwrap()).into_owned()
    }
}

impl Drop for Harness {
    fn drop(&mut self) {
        let _ = self.service.kill();
        let _ = self.service.wait();
        if let Some(reader) = self.stderr_reader.take() {
            let _ = reader.join();
        }
        let _ = fs::remove_dir_all(&self.root);
    }
}

struct RpcPlan {
    thread_reads: Vec<Value>,
    turns: Value,
    turn_pages_after: Vec<(String, Value)>,
    items: Value,
    reject_item_list: bool,
    descendants: Value,
    completion_turn_id: String,
    completion_status: String,
    include_item_completed: bool,
    item_completed_override: Option<Value>,
    events_before_started: Vec<Value>,
    plugin_status: Value,
    plugin_status_unavailable: bool,
    thread_read_gate: Option<oneshot::Receiver<()>>,
    compact_response_gate: Option<oneshot::Receiver<()>>,
}

impl RpcPlan {
    fn new(thread: Value, turns: Value, completion_turn_id: &str) -> Self {
        Self {
            thread_reads: vec![thread],
            turns,
            turn_pages_after: Vec::new(),
            items: json!({"data": [], "nextCursor": null}),
            reject_item_list: false,
            descendants: json!({"data": [], "nextCursor": null}),
            completion_turn_id: completion_turn_id.to_owned(),
            completion_status: "completed".to_owned(),
            include_item_completed: true,
            item_completed_override: None,
            events_before_started: Vec::new(),
            plugin_status: json!({"marketplaces": [], "marketplaceLoadErrors": []}),
            plugin_status_unavailable: false,
            thread_read_gate: None,
            compact_response_gate: None,
        }
    }
}

struct RpcMock {
    calls: UnboundedReceiver<Value>,
    events: UnboundedReceiver<Value>,
    responses: UnboundedReceiver<String>,
    task: JoinHandle<()>,
}

impl Drop for RpcMock {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl RpcMock {
    async fn next_event(&mut self) -> Value {
        timeout_at(Instant::now() + Duration::from_secs(4), self.events.recv())
            .await
            .expect("timed out waiting for fake app-server notification")
            .expect("fake app-server event channel closed")
    }

    async fn wait_for_response(&mut self, expected: &str) {
        let deadline = Instant::now() + Duration::from_secs(4);
        loop {
            let method = timeout_at(deadline, self.responses.recv())
                .await
                .unwrap_or_else(|_| panic!("timed out waiting for {expected} response"))
                .expect("fake app-server response channel closed");
            if method == expected {
                return;
            }
        }
    }
}

fn thread_view(
    id: &str,
    session_id: &str,
    path: &str,
    parent_thread_id: Option<&str>,
    status: &str,
    can_accept_direct_input: bool,
) -> Value {
    let status = if status == "active" {
        json!({"type": status, "activeFlags": []})
    } else {
        json!({"type": status})
    };
    json!({
        "id": id,
        "sessionId": session_id,
        "path": path,
        "parentThreadId": parent_thread_id,
        "historyMode": "paginated",
        "status": status,
        "canAcceptDirectInput": can_accept_direct_input,
    })
}

fn turns(data: Vec<Value>) -> Value {
    json!({"data": data, "nextCursor": null})
}

fn plugin_status(plugins: Vec<Value>) -> Value {
    json!({
        "marketplaces": [{"plugins": plugins}],
        "marketplaceLoadErrors": [],
    })
}

fn plugin_record(id: &str, installed: bool, enabled: bool) -> Value {
    json!({"id": id, "installed": installed, "enabled": enabled})
}

fn completed_turn(id: &str) -> Value {
    json!({"id": id, "status": "completed"})
}

fn root_stop_for(session_id: &str, turn_id: &str, transcript_path: &str) -> Value {
    json!({
        "hook_event_name": "Stop",
        "session_id": session_id,
        "turn_id": turn_id,
        "transcript_path": transcript_path,
    })
}

fn root_stop(turn_id: &str) -> Value {
    root_stop_for(SESSION, turn_id, ROOT_PATH)
}

fn root_prompt() -> Value {
    json!({
        "hook_event_name": "UserPromptSubmit",
        "session_id": SESSION,
        "transcript_path": ROOT_PATH,
    })
}

fn root_prompt_with(prompt: &str) -> Value {
    json!({
        "hook_event_name": "UserPromptSubmit",
        "session_id": SESSION,
        "turn_id": NEW_TURN,
        "transcript_path": ROOT_PATH,
        "prompt": prompt,
    })
}

fn subagent_prompt_with(prompt: &str) -> Value {
    json!({
        "hook_event_name": "UserPromptSubmit",
        "session_id": SESSION,
        "turn_id": NEW_TURN,
        "transcript_path": FORK_PATH,
        "agent_id": CHILD,
        "prompt": prompt,
    })
}

fn prompt_turn(id: &str, status: &str, completed_at: Option<u64>) -> Value {
    json!({
        "id": id,
        "status": status,
        "completedAt": completed_at,
        "items": [],
    })
}

fn unix_now_seconds() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

fn boot_time_seconds() -> u64 {
    fs::read_to_string("/proc/uptime")
        .unwrap()
        .split_whitespace()
        .next()
        .unwrap()
        .split('.')
        .next()
        .unwrap()
        .parse()
        .unwrap()
}

fn boot_time_centiseconds() -> u64 {
    let uptime: f64 = fs::read_to_string("/proc/uptime")
        .unwrap()
        .split_whitespace()
        .next()
        .unwrap()
        .parse()
        .unwrap();
    (uptime * 100.0).floor() as u64
}

fn assert_prompt_silent(output: Output) {
    assert!(output.status.success());
    assert!(output.stdout.is_empty());
}

fn assert_prompt_block(output: Output) -> String {
    assert!(output.status.success());
    let response: Value = serde_json::from_slice(&output.stdout)
        .unwrap_or_else(|error| panic!("hook output was not JSON: {error}; output: {output:?}"));
    assert_eq!(
        response["decision"], "block",
        "unexpected hook response: {response}"
    );
    let reason = response["reason"]
        .as_str()
        .expect("a blocking response must include a textual reason");
    assert!(!reason.trim().is_empty(), "blocking reason was empty");
    let reason_lower = reason.to_lowercase();
    assert!(
        reason_lower.contains("/clear"),
        "reason omitted /clear: {reason}"
    );
    assert!(
        reason_lower.contains("resubmit"),
        "reason omitted resubmission advice: {reason}"
    );
    assert!(
        reason_lower.contains("same message") || reason_lower.contains("same prompt"),
        "reason omitted advice to resend the same message: {reason}"
    );
    reason.to_owned()
}

fn assert_unknown_age_reason(reason: &str) {
    let reason_lower = reason.to_lowercase();
    assert!(
        reason_lower.contains("unknown")
            || reason_lower.contains("unavailable")
            || reason_lower.contains("cannot determine")
            || reason_lower.contains("can't determine")
            || reason_lower.contains("unable to determine"),
        "reason did not say the turn age was unknown: {reason}"
    );
}

fn precompact() -> Value {
    json!({
        "hook_event_name": "PreCompact",
        "session_id": SESSION,
        "transcript_path": ROOT_PATH,
    })
}

fn subagent_stop(turn_id: &str) -> Value {
    json!({
        "hook_event_name": "SubagentStop",
        "session_id": SESSION,
        "agent_id": CHILD,
        "turn_id": turn_id,
    })
}

fn subagent_prompt() -> Value {
    json!({
        "hook_event_name": "UserPromptSubmit",
        "session_id": SESSION,
        "agent_id": CHILD,
    })
}

fn subagent_start() -> Value {
    json!({
        "hook_event_name": "SubagentStart",
        "session_id": SESSION,
        "agent_id": CHILD,
    })
}

async fn start_rpc_mock(listener: UnixListener, mut plan: RpcPlan) -> RpcMock {
    let (sender, calls) = mpsc::unbounded_channel();
    let (event_sender, events) = mpsc::unbounded_channel();
    let (response_sender, responses) = mpsc::unbounded_channel();
    let task = tokio::spawn(async move {
        let mut last_thread = plan.thread_reads.first().cloned().unwrap_or(Value::Null);
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                return;
            };
            let Ok(mut socket) = accept_async(stream).await else {
                continue;
            };
            while let Some(Ok(message)) = socket.next().await {
                let Message::Text(text) = message else {
                    continue;
                };
                let Ok(request) = serde_json::from_str::<Value>(&text) else {
                    continue;
                };
                let Some(method) = request.get("method").and_then(Value::as_str) else {
                    continue;
                };
                let Some(id) = request.get("id").cloned() else {
                    continue;
                };
                if sender.send(request.clone()).is_err() {
                    return;
                }

                if method == "thread/read"
                    && let Some(gate) = plan.thread_read_gate.take()
                {
                    let _ = gate.await;
                }
                if method == "thread/compact/start"
                    && let Some(gate) = plan.compact_response_gate.take()
                {
                    let _ = gate.await;
                }

                let result = match method {
                    "plugin/installed" => plan.plugin_status.clone(),
                    "thread/read" => {
                        if !plan.thread_reads.is_empty() {
                            last_thread = plan.thread_reads.remove(0);
                        }
                        json!({"thread": last_thread})
                    }
                    "thread/turns/list" => {
                        let cursor = request["params"]["cursor"].as_str();
                        let mut page = match cursor {
                            Some(cursor) => plan
                                .turn_pages_after
                                .iter()
                                .find(|(page_cursor, _)| page_cursor == cursor)
                                .map(|(_, page)| page.clone())
                                .unwrap_or_else(|| json!({"data": [], "nextCursor": null})),
                            None => plan.turns.clone(),
                        };
                        if request["params"]["itemsView"] != "full"
                            && let Some(turns) = page.get_mut("data").and_then(Value::as_array_mut)
                        {
                            for turn in turns {
                                if let Some(fields) = turn.as_object_mut() {
                                    fields.remove("items");
                                }
                            }
                        }
                        page
                    }
                    "thread/items/list" => plan.items.clone(),
                    "thread/list" => plan.descendants.clone(),
                    _ => json!({}),
                };
                let mut completion_events = Vec::new();
                if method == "thread/compact/start" {
                    let params = request.get("params").cloned().unwrap_or_default();
                    let thread_id = params.get("threadId").cloned().unwrap_or(json!(ROOT));
                    let turn_id = plan.completion_turn_id.clone();
                    let item =
                        json!({"id": "context-compaction-item", "type": "contextCompaction"});
                    completion_events.extend(plan.events_before_started.clone());
                    completion_events.push(json!({
                        "jsonrpc": "2.0",
                        "method": "item/started",
                        "params": {"threadId": thread_id, "turnId": turn_id, "item": item},
                    }));
                    if plan.include_item_completed {
                        completion_events.push(plan.item_completed_override.clone().unwrap_or(
                            json!({
                                "jsonrpc": "2.0",
                                "method": "item/completed",
                                "params": {"threadId": thread_id, "turnId": turn_id, "item": item},
                            }),
                        ));
                    }
                    completion_events.push(json!({
                        "jsonrpc": "2.0",
                        "method": "turn/completed",
                        "params": {
                            "threadId": thread_id,
                            "turn": {"id": turn_id, "status": plan.completion_status},
                        },
                    }));
                }
                let mut client_closed = false;
                for notification in completion_events {
                    if socket
                        .send(Message::Text(notification.to_string().into()))
                        .await
                        .is_err()
                    {
                        client_closed = true;
                        break;
                    }
                    if event_sender.send(notification).is_err() {
                        return;
                    }
                }
                if client_closed {
                    break;
                }
                let response = if method == "plugin/installed" && plan.plugin_status_unavailable {
                    json!({
                        "jsonrpc": "2.0",
                        "id": id,
                        "error": {"code": -32000, "message": "plugin status unavailable"},
                    })
                } else if method == "thread/items/list" && plan.reject_item_list {
                    json!({
                        "jsonrpc": "2.0",
                        "id": id,
                        "error": {"code": -32601, "message": "method not available for legacy history"},
                    })
                } else {
                    json!({"jsonrpc": "2.0", "id": id, "result": result})
                };
                if socket
                    .send(Message::Text(response.to_string().into()))
                    .await
                    .is_err()
                {
                    break;
                }
                if response_sender.send(method.to_owned()).is_err() {
                    return;
                }
            }
        }
    });
    RpcMock {
        calls,
        events,
        responses,
        task,
    }
}

fn relevant(call: &Value) -> bool {
    call.get("method")
        .and_then(Value::as_str)
        .is_some_and(|method| OBSERVED_METHODS.contains(&method))
}

async fn next_method(mock: &mut RpcMock, expected: &str) -> Value {
    let deadline = Instant::now() + Duration::from_secs(6);
    loop {
        let call = timeout_at(deadline, mock.calls.recv())
            .await
            .unwrap_or_else(|_| panic!("timed out waiting for {expected}"))
            .expect("app-server connection closed");
        if !relevant(&call) {
            continue;
        }
        let actual = call["method"].as_str().unwrap();
        assert_eq!(actual, expected, "unexpected app-server call: {call}");
        return call;
    }
}

async fn next_relevant(mock: &mut RpcMock) -> Value {
    let deadline = Instant::now() + Duration::from_secs(6);
    loop {
        let call = timeout_at(deadline, mock.calls.recv())
            .await
            .expect("timed out waiting for app-server call")
            .expect("app-server connection closed");
        if relevant(&call) {
            return call;
        }
    }
}

async fn calls_until_mutation(mock: &mut RpcMock, expected: &str) -> Vec<Value> {
    calls_until_mutation_after(mock, expected, Vec::new()).await
}

async fn calls_until_method(mock: &mut RpcMock, expected: &str) -> Vec<Value> {
    let deadline = Instant::now() + Duration::from_secs(6);
    let mut calls = Vec::new();
    loop {
        let call = timeout_at(deadline, next_relevant(mock))
            .await
            .unwrap_or_else(|_| panic!("timed out waiting for {expected}; observed {calls:?}"));
        let reached = call["method"] == expected;
        calls.push(call);
        if reached {
            return calls;
        }
    }
}

async fn calls_until_mutation_after(
    mock: &mut RpcMock,
    expected: &str,
    mut calls: Vec<Value>,
) -> Vec<Value> {
    let deadline = Instant::now() + Duration::from_secs(6);
    loop {
        let call = timeout_at(deadline, next_relevant(mock))
            .await
            .unwrap_or_else(|_| panic!("timed out waiting for {expected}; observed {calls:?}"));
        let method = call["method"].as_str().unwrap();
        if matches!(
            method,
            "thread/resume" | "thread/compact/start" | "thread/delete"
        ) {
            assert_eq!(method, expected, "unexpected mutation call: {call}");
            calls.push(call);
            return calls;
        }
        calls.push(call);
    }
}

async fn calls_until_child_delete_after(
    mock: &mut RpcMock,
    child_id: &str,
    prior: Vec<Value>,
) -> Vec<Value> {
    let calls = calls_until_mutation_after(mock, "thread/delete", prior).await;
    assert_final_turn_check(&calls, child_id);
    assert!(
        calls.iter().any(|call| {
            call["method"] == "thread/list" && call["params"]["ancestorThreadId"] == child_id
        }),
        "child deletion lacked a descendant query: {calls:?}"
    );
    assert_eq!(calls.last().unwrap()["params"]["threadId"], child_id);
    calls
}

fn assert_final_turn_check(calls: &[Value], thread_id: &str) {
    let mutation_index = calls.len().saturating_sub(1);
    let read_index = calls[..mutation_index]
        .iter()
        .rposition(|call| call["method"] == "thread/read")
        .expect("mutation lacked a preceding thread/read");
    let turns_index = calls[..mutation_index]
        .iter()
        .rposition(|call| call["method"] == "thread/turns/list")
        .expect("mutation lacked a preceding thread/turns/list");
    assert!(
        read_index < turns_index,
        "turn validation preceded its thread read: {calls:?}"
    );
    assert_eq!(calls[read_index]["params"]["threadId"], thread_id);
    assert_eq!(calls[turns_index]["params"]["threadId"], thread_id);
}

async fn collect_calls(mock: &mut RpcMock, duration: Duration) -> Vec<Value> {
    let deadline = Instant::now() + duration;
    let mut found = Vec::new();
    loop {
        match timeout_at(deadline, mock.calls.recv()).await {
            Ok(Some(call)) if relevant(&call) => found.push(call),
            Ok(Some(_)) => {}
            Ok(None) | Err(_) => return found,
        }
    }
}

async fn collect_all_calls(mock: &mut RpcMock, duration: Duration) -> Vec<Value> {
    let deadline = Instant::now() + duration;
    let mut found = Vec::new();
    loop {
        match timeout_at(deadline, mock.calls.recv()).await {
            Ok(Some(call)) => found.push(call),
            Ok(None) | Err(_) => return found,
        }
    }
}

async fn prompt_with_history(
    window: Option<u64>,
    history: Value,
    items: Value,
    prompt: &str,
) -> (Output, Vec<Value>) {
    let harness = Harness::start_with_window(60, 30, window).await;
    let listener = harness.listener();
    let root = thread_view(ROOT, SESSION, ROOT_PATH, None, "idle", true);
    let mut plan = RpcPlan::new(root, history, NEW_TURN);
    plan.items = items;
    let mut mock = start_rpc_mock(listener, plan).await;

    let output = harness.hook_output(root_prompt_with(prompt)).await;
    let calls = collect_all_calls(&mut mock, Duration::from_millis(100)).await;
    (output, calls)
}

fn root_thread_with_history_mode(history_mode: Option<&str>) -> Value {
    let mut thread = thread_view(ROOT, SESSION, ROOT_PATH, None, "idle", true);
    if let Some(history_mode) = history_mode {
        thread["historyMode"] = json!(history_mode);
    } else {
        thread.as_object_mut().unwrap().remove("historyMode");
    }
    thread
}

async fn prompt_with_legacy_history(
    thread: Value,
    history: Value,
    prompt: &str,
) -> (Output, Vec<Value>) {
    let harness = Harness::start(60, 30).await;
    let listener = harness.listener();
    let mut plan = RpcPlan::new(thread, history, NEW_TURN);
    plan.reject_item_list = true;
    let mut mock = start_rpc_mock(listener, plan).await;

    let output = harness.hook_output(root_prompt_with(prompt)).await;
    let calls = collect_all_calls(&mut mock, Duration::from_millis(100)).await;
    (output, calls)
}

fn assert_legacy_turn_items_view(calls: &[Value], history_mode: &str) {
    let turn_lists = calls
        .iter()
        .filter(|call| call["method"] == "thread/turns/list")
        .collect::<Vec<_>>();
    assert!(
        !turn_lists.is_empty(),
        "{history_mode} history did not list turns: {calls:?}"
    );
    assert!(
        turn_lists
            .iter()
            .any(|call| call["params"]["itemsView"] == "full"),
        "{history_mode} history did not request full turn items: {calls:?}"
    );
    assert!(
        !calls
            .iter()
            .any(|call| call["method"] == "thread/items/list"),
        "{history_mode} history called unsupported thread/items/list: {calls:?}"
    );
}

fn assert_no_prompt_dispatch(calls: &[Value]) {
    for call in calls {
        let method = call["method"].as_str();
        assert!(
            !matches!(
                method,
                Some(
                    "turn/start"
                        | "turn/submit"
                        | "turn/input"
                        | "thread/turn/start"
                        | "thread/turn/submit"
                )
            ),
            "prompt was dispatched despite the hook result: {call}"
        );
    }
}

fn state_contains_text(state_home: &Path, expected: &str) -> std::io::Result<bool> {
    let mut pending = vec![state_home.to_path_buf()];
    while let Some(current) = pending.pop() {
        for entry in fs::read_dir(current)? {
            let entry = entry?;
            let path = entry.path();
            if entry.file_type()?.is_dir() {
                pending.push(path);
            } else {
                let contents = fs::read(path)?;
                if contents
                    .windows(expected.len())
                    .any(|window| window == expected.as_bytes())
                {
                    return Ok(true);
                }
            }
        }
    }
    Ok(false)
}

fn assert_no_mutation(calls: &[Value]) {
    for call in calls {
        assert!(
            !matches!(
                call["method"].as_str(),
                Some("thread/resume" | "thread/compact/start" | "thread/delete")
            ),
            "unexpected mutation call: {call}"
        );
    }
}

async fn compact_after_resume(mock: &mut RpcMock, thread_id: &str) -> Value {
    let before_resume = calls_until_mutation(mock, "thread/resume").await;
    assert_final_turn_check(&before_resume, thread_id);
    assert_eq!(
        before_resume.last().unwrap()["params"]["threadId"],
        thread_id
    );
    let before_compact = calls_until_mutation(mock, "thread/compact/start").await;
    assert_final_turn_check(&before_compact, thread_id);
    assert_eq!(
        before_compact.last().unwrap()["params"]["threadId"],
        thread_id
    );
    before_compact.last().unwrap().clone()
}

async fn wait_for_compacted(harness: &Harness, stderr_before: &str) {
    let deadline = Instant::now() + Duration::from_secs(3);
    loop {
        let stderr = harness.stderr_text();
        let since = stderr.strip_prefix(stderr_before).unwrap_or(&stderr);
        if since.to_ascii_lowercase().contains("compacted") {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "service did not report compaction: {since:?}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

async fn wait_for_inflight_expiry(
    harness: &Harness,
    stderr_before: &str,
    jobs_file: &Path,
    turn_id: &str,
) -> String {
    let deadline = Instant::now() + Duration::from_secs(3);
    loop {
        let stderr = harness.stderr_text();
        let since = stderr.strip_prefix(stderr_before).unwrap_or(&stderr);
        let lower = since.to_ascii_lowercase();
        let diagnostic = lower.contains("unknown")
            || lower.contains("uncertain")
            || lower.contains("deadline")
            || lower.contains("expired");
        if diagnostic && !persisted_jobs_contain_turn(jobs_file, turn_id) {
            return since.to_owned();
        }
        assert!(
            Instant::now() < deadline,
            "in-flight expiry diagnostic or persisted job removal missing; diagnostic={since:?}; job_present={}",
            persisted_jobs_contain_turn(jobs_file, turn_id)
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

async fn assert_compaction_events(
    mock: &mut RpcMock,
    thread_id: &str,
    turn_id: &str,
    status: &str,
    include_item_completed: bool,
) {
    let started = mock.next_event().await;
    assert_eq!(started["method"], "item/started");
    assert_eq!(started["params"]["threadId"], thread_id);
    assert_eq!(started["params"]["turnId"], turn_id);
    assert_eq!(started["params"]["item"]["type"], "contextCompaction");
    let item_id = started["params"]["item"]["id"].clone();

    if include_item_completed {
        let completed_item = mock.next_event().await;
        assert_eq!(completed_item["method"], "item/completed");
        assert_eq!(completed_item["params"]["threadId"], thread_id);
        assert_eq!(completed_item["params"]["turnId"], turn_id);
        assert_eq!(completed_item["params"]["item"]["id"], item_id);
        assert_eq!(
            completed_item["params"]["item"]["type"],
            "contextCompaction"
        );
    }

    let completed_turn = mock.next_event().await;
    assert_eq!(completed_turn["method"], "turn/completed");
    assert_eq!(completed_turn["params"]["threadId"], thread_id);
    assert_eq!(completed_turn["params"]["turn"]["id"], turn_id);
    assert_eq!(completed_turn["params"]["turn"]["status"], status);
    mock.wait_for_response("thread/compact/start").await;
}

async fn assert_root_compaction(
    harness: &Harness,
    mock: &mut RpcMock,
    turn_id: &str,
    stderr_before: &str,
) {
    compact_after_resume(mock, ROOT).await;
    assert_compaction_events(mock, ROOT, turn_id, "completed", true).await;
    wait_for_compacted(harness, stderr_before).await;
}

#[tokio::test]
async fn hook_is_successful_and_silent_when_service_is_absent() {
    let mut harness = Harness::start(1, 1).await;
    harness.stop_service();

    let output = harness.hook_raw(&root_stop(TURN).to_string());
    assert!(output.status.success());
    assert!(output.stdout.is_empty());
}

#[tokio::test]
async fn root_stop_reads_identity_and_latest_turn_then_resumes_and_compacts() {
    let harness = Harness::start(1, 30).await;
    let listener = harness.listener();
    let root = thread_view(ROOT, SESSION, ROOT_PATH, None, "idle", true);
    let mut mock = start_rpc_mock(
        listener,
        RpcPlan::new(root, turns(vec![completed_turn(TURN)]), TURN),
    )
    .await;

    // Malformed IPC must not kill the service; a later valid hook still completes.
    let mut service = harness;
    let _ = service.hook_raw("{}");
    service.assert_service_running();
    let malformed = collect_calls(&mut mock, Duration::from_millis(1200)).await;
    assert!(
        malformed.is_empty(),
        "malformed event reached app-server: {malformed:?}"
    );
    let stderr_before = service.stderr_text();
    service.hook(root_stop(TURN));

    assert_root_compaction(&service, &mut mock, TURN, &stderr_before).await;
}

#[tokio::test]
async fn root_fork_uses_transcript_uuid_when_it_differs_from_session_id() {
    let harness = Harness::start(1, 30).await;
    let listener = harness.listener();
    let fork = thread_view(FORK, SESSION, FORK_PATH, None, "idle", true);
    let mut mock = start_rpc_mock(
        listener,
        RpcPlan::new(fork, turns(vec![completed_turn(TURN)]), TURN),
    )
    .await;
    let stderr_before = harness.stderr_text();

    harness.hook(root_stop_for(SESSION, TURN, FORK_PATH));
    compact_after_resume(&mut mock, FORK).await;
    assert_compaction_events(&mut mock, FORK, TURN, "completed", true).await;
    wait_for_compacted(&harness, &stderr_before).await;
}

#[tokio::test]
async fn a_prompt_cancels_pending_root_work_and_a_newer_stop_reschedules_it() {
    let harness = Harness::start(1, 30).await;
    let listener = harness.listener();
    let root = thread_view(ROOT, SESSION, ROOT_PATH, None, "idle", true);
    let mut mock = start_rpc_mock(
        listener,
        RpcPlan::new(root, turns(vec![completed_turn(NEW_TURN)]), NEW_TURN),
    )
    .await;

    harness.hook(root_stop(TURN));
    harness.hook(root_prompt());
    let cancelled = collect_calls(&mut mock, Duration::from_millis(1500)).await;
    assert!(
        cancelled.is_empty(),
        "cancelled work reached app-server: {cancelled:?}"
    );

    let stderr_before = harness.stderr_text();
    harness.hook(root_stop(NEW_TURN));
    assert_root_compaction(&harness, &mut mock, NEW_TURN, &stderr_before).await;
}

#[tokio::test]
async fn precompact_cancels_pending_root_work() {
    let harness = Harness::start(1, 30).await;
    let listener = harness.listener();
    let root = thread_view(ROOT, SESSION, ROOT_PATH, None, "idle", true);
    let mut mock = start_rpc_mock(
        listener,
        RpcPlan::new(root, turns(vec![completed_turn(TURN)]), TURN),
    )
    .await;

    harness.hook(root_stop(TURN));
    harness.hook(precompact());
    let calls = collect_calls(&mut mock, Duration::from_millis(1500)).await;
    assert!(
        calls.is_empty(),
        "PreCompact did not cancel pending work: {calls:?}"
    );
}

#[tokio::test]
async fn a_prompt_cancels_work_while_thread_read_is_waiting_for_its_rpc_response() {
    let harness = Harness::start(1, 30).await;
    let listener = harness.listener();
    let root = thread_view(ROOT, SESSION, ROOT_PATH, None, "idle", true);
    let mut plan = RpcPlan::new(root, turns(vec![completed_turn(TURN)]), TURN);
    let (release_read, read_gate) = oneshot::channel();
    plan.thread_read_gate = Some(read_gate);
    let mut mock = start_rpc_mock(listener, plan).await;

    harness.hook(root_stop(TURN));
    let read = next_method(&mut mock, "thread/read").await;
    assert_eq!(read["params"]["threadId"], ROOT);
    harness
        .hook_with_timeout(root_prompt(), Duration::from_secs(3))
        .await;
    release_read.send(()).unwrap();

    let calls = collect_calls(&mut mock, Duration::from_millis(1500)).await;
    assert_no_mutation(&calls);
}

#[tokio::test]
async fn failed_or_interrupted_compaction_is_logged_and_not_retried() {
    for (status, include_item_completed) in
        [("failed", true), ("interrupted", true), ("failed", false)]
    {
        let mut harness = Harness::start(1, 1).await;
        let listener = harness.listener();
        let root = thread_view(ROOT, SESSION, ROOT_PATH, None, "idle", true);
        let mut plan = RpcPlan::new(root, turns(vec![completed_turn(TURN)]), TURN);
        plan.completion_status = status.to_owned();
        plan.include_item_completed = include_item_completed;
        let mut mock = start_rpc_mock(listener, plan).await;
        let stderr_before = harness.stderr_text();
        harness.hook(root_stop(TURN));

        compact_after_resume(&mut mock, ROOT).await;
        assert_compaction_events(&mut mock, ROOT, TURN, status, include_item_completed).await;
        let later = collect_calls(&mut mock, Duration::from_millis(1600)).await;
        assert!(
            !later
                .iter()
                .any(|call| call["method"] == "thread/compact/start"),
            "failed compaction was retried: {later:?}"
        );
        harness.assert_service_running();
        let stderr_after = harness.stderr_text();
        let new_stderr = stderr_after
            .strip_prefix(&stderr_before)
            .unwrap_or(&stderr_after);
        let lower = new_stderr.to_ascii_lowercase();
        assert!(
            lower.contains("fail") || lower.contains("interrupt") || lower.contains("uncertain"),
            "failed compaction produced no failure/uncertainty stderr: {new_stderr:?}"
        );
    }
}

#[tokio::test]
async fn mismatched_item_completion_is_uncertain_even_if_a_stray_event_arrived_first() {
    let mut harness = Harness::start(1, 1).await;
    let listener = harness.listener();
    let root = thread_view(ROOT, SESSION, ROOT_PATH, None, "idle", true);
    let mut plan = RpcPlan::new(root, turns(vec![completed_turn(TURN)]), TURN);
    plan.events_before_started.push(json!({
        "jsonrpc": "2.0",
        "method": "item/completed",
        "params": {
            "threadId": ROOT,
            "turnId": TURN,
            "item": {"id": "context-compaction-item", "type": "contextCompaction"},
        },
    }));
    plan.item_completed_override = Some(json!({
        "jsonrpc": "2.0",
        "method": "item/completed",
        "params": {
            "threadId": ROOT,
            "turnId": TURN,
            "item": {"id": "different-item", "type": "contextCompaction"},
        },
    }));
    let mut mock = start_rpc_mock(listener, plan).await;
    let stderr_before = harness.stderr_text();
    harness.hook(root_stop(TURN));

    compact_after_resume(&mut mock, ROOT).await;
    let stray = mock.next_event().await;
    assert_eq!(stray["method"], "item/completed");
    let started = mock.next_event().await;
    assert_eq!(started["method"], "item/started");
    let mismatched = mock.next_event().await;
    assert_eq!(mismatched["method"], "item/completed");
    assert_eq!(mismatched["params"]["item"]["id"], "different-item");
    let completed = mock.next_event().await;
    assert_eq!(completed["method"], "turn/completed");
    assert_eq!(completed["params"]["turn"]["status"], "completed");
    mock.wait_for_response("thread/compact/start").await;

    let later = collect_calls(&mut mock, Duration::from_millis(1600)).await;
    assert!(
        !later
            .iter()
            .any(|call| call["method"] == "thread/compact/start"),
        "mismatched item completion retried the compact mutation: {later:?}"
    );
    harness.assert_service_running();
    let stderr_after = harness.stderr_text();
    let new_stderr = stderr_after
        .strip_prefix(&stderr_before)
        .unwrap_or(&stderr_after);
    let lower = new_stderr.to_ascii_lowercase();
    assert!(
        lower.contains("uncertain") || lower.contains("without successful completion"),
        "mismatched item completion was not logged as uncertain: {new_stderr:?}"
    );
}

#[tokio::test]
async fn subagent_prompt_cancels_and_new_subagent_stop_reschedules_eligible_child() {
    let harness = Harness::start(1, 30).await;
    let listener = harness.listener();
    let child = thread_view(CHILD, SESSION, ROOT_PATH, Some(ROOT), "idle", true);
    let mut mock = start_rpc_mock(
        listener,
        RpcPlan::new(child, turns(vec![completed_turn(NEW_TURN)]), NEW_TURN),
    )
    .await;

    harness.hook(subagent_stop(TURN));
    harness.hook(subagent_prompt());
    let cancelled = collect_calls(&mut mock, Duration::from_millis(1500)).await;
    assert!(
        cancelled.is_empty(),
        "cancelled child work reached app-server: {cancelled:?}"
    );

    let stderr_before = harness.stderr_text();
    harness.hook(subagent_stop(NEW_TURN));
    compact_after_resume(&mut mock, CHILD).await;
    assert_compaction_events(&mut mock, CHILD, NEW_TURN, "completed", true).await;
    wait_for_compacted(&harness, &stderr_before).await;
}

#[tokio::test]
async fn subagent_start_cancels_pending_child_work() {
    let harness = Harness::start(1, 30).await;
    let listener = harness.listener();
    let child = thread_view(CHILD, SESSION, ROOT_PATH, Some(ROOT), "idle", true);
    let mut mock = start_rpc_mock(
        listener,
        RpcPlan::new(child, turns(vec![completed_turn(TURN)]), TURN),
    )
    .await;

    harness.hook(subagent_stop(TURN));
    harness.hook(subagent_start());
    let calls = collect_calls(&mut mock, Duration::from_millis(1500)).await;
    assert!(
        calls.is_empty(),
        "SubagentStart did not cancel pending child work: {calls:?}"
    );
}

#[tokio::test]
async fn mismatched_loaded_thread_identity_prevents_mutation() {
    let mismatches = [
        thread_view(
            "01a197e3-4a70-72e4-bddb-ef456f348be1",
            SESSION,
            ROOT_PATH,
            None,
            "idle",
            true,
        ),
        thread_view(
            ROOT,
            "01a197e3-4a70-72e4-bddb-ef456f348be1",
            ROOT_PATH,
            None,
            "idle",
            true,
        ),
        thread_view(ROOT, SESSION, "/other/transcript.jsonl", None, "idle", true),
    ];

    for wrong_thread in mismatches {
        let harness = Harness::start(1, 30).await;
        let listener = harness.listener();
        let mut mock = start_rpc_mock(
            listener,
            RpcPlan::new(wrong_thread, turns(vec![completed_turn(TURN)]), TURN),
        )
        .await;
        harness.hook(root_stop(TURN));

        let calls = collect_calls(&mut mock, Duration::from_millis(1500)).await;
        assert!(calls.iter().any(|call| call["method"] == "thread/read"));
        assert_no_mutation(&calls);
    }
}

#[tokio::test]
async fn non_idle_root_thread_prevents_mutation() {
    let harness = Harness::start(1, 30).await;
    let listener = harness.listener();
    let active_root = thread_view(ROOT, SESSION, ROOT_PATH, None, "active", true);
    let mut mock = start_rpc_mock(
        listener,
        RpcPlan::new(active_root, turns(vec![completed_turn(TURN)]), TURN),
    )
    .await;
    harness.hook(root_stop(TURN));

    let calls = collect_calls(&mut mock, Duration::from_millis(1500)).await;
    assert!(calls.iter().any(|call| call["method"] == "thread/read"));
    assert_no_mutation(&calls);
}

#[tokio::test]
async fn newer_or_incomplete_latest_turn_prevents_mutation() {
    let cases = [
        turns(vec![completed_turn(NEW_TURN)]),
        turns(vec![json!({"id": TURN, "status": "inProgress"})]),
    ];

    for latest_turns in cases {
        let harness = Harness::start(1, 30).await;
        let listener = harness.listener();
        let root = thread_view(ROOT, SESSION, ROOT_PATH, None, "idle", true);
        let mut mock = start_rpc_mock(listener, RpcPlan::new(root, latest_turns, TURN)).await;
        harness.hook(root_stop(TURN));

        let calls = collect_calls(&mut mock, Duration::from_millis(1500)).await;
        assert!(
            calls
                .iter()
                .any(|call| call["method"] == "thread/turns/list")
        );
        assert_no_mutation(&calls);
    }
}

#[tokio::test]
async fn idle_child_without_direct_input_is_deleted_only_after_empty_descendant_check() {
    let harness = Harness::start(1, 30).await;
    let listener = harness.listener();
    let child = thread_view(CHILD, SESSION, ROOT_PATH, Some(ROOT), "idle", false);
    let plan = RpcPlan::new(child, turns(vec![completed_turn(TURN)]), TURN);
    let mut mock = start_rpc_mock(listener, plan).await;
    harness.hook(subagent_stop(TURN));

    let first_read = next_method(&mut mock, "thread/read").await;
    assert_eq!(first_read["params"]["threadId"], CHILD);
    let calls = calls_until_child_delete_after(&mut mock, CHILD, vec![first_read]).await;
    assert!(
        calls
            .iter()
            .any(|call| call["method"] == "thread/turns/list")
    );
}

#[tokio::test]
async fn idle_child_with_unknown_direct_input_eligibility_is_not_mutated() {
    for null_field in [true, false] {
        let harness = Harness::start(1, 30).await;
        let listener = harness.listener();
        let mut child = thread_view(CHILD, SESSION, ROOT_PATH, Some(ROOT), "idle", false);
        if null_field {
            child["canAcceptDirectInput"] = Value::Null;
        } else {
            child
                .as_object_mut()
                .unwrap()
                .remove("canAcceptDirectInput");
        }
        let mut mock = start_rpc_mock(
            listener,
            RpcPlan::new(child, turns(vec![completed_turn(TURN)]), TURN),
        )
        .await;
        harness.hook(subagent_stop(TURN));

        let read = next_method(&mut mock, "thread/read").await;
        assert_eq!(read["params"]["threadId"], CHILD);
        let calls = collect_calls(&mut mock, Duration::from_millis(1500)).await;
        assert_no_mutation(&calls);
    }
}

#[tokio::test]
async fn active_descendant_prevents_deleting_idle_nonaccepting_child() {
    let harness = Harness::start(1, 30).await;
    let listener = harness.listener();
    let child = thread_view(CHILD, SESSION, ROOT_PATH, Some(ROOT), "idle", false);
    let mut plan = RpcPlan::new(child, turns(vec![completed_turn(TURN)]), TURN);
    plan.descendants = json!({
        "data": [thread_view("01a197e3-4a70-72e4-bddb-ef456f348be1", SESSION, ROOT_PATH, Some(CHILD), "active", true)],
        "nextCursor": null,
    });
    let mut mock = start_rpc_mock(listener, plan).await;
    harness.hook(subagent_stop(TURN));

    let first_read = next_method(&mut mock, "thread/read").await;
    assert_eq!(first_read["params"]["threadId"], CHILD);
    let rest = collect_calls(&mut mock, Duration::from_millis(1500)).await;
    assert!(
        rest.iter()
            .any(|call| call["method"] == "thread/turns/list")
    );
    assert!(rest.iter().any(|call| {
        call["method"] == "thread/list" && call["params"]["ancestorThreadId"] == CHILD
    }));
    assert_no_mutation(&rest);
}

#[tokio::test]
async fn active_child_is_rechecked_after_retry_without_mutation() {
    let harness = Harness::start(1, 1).await;
    let listener = harness.listener();
    let active = thread_view(CHILD, SESSION, ROOT_PATH, Some(ROOT), "active", false);
    let mut plan = RpcPlan::new(active.clone(), turns(vec![completed_turn(TURN)]), TURN);
    plan.thread_reads = vec![active.clone(), active];
    let mut mock = start_rpc_mock(listener, plan).await;
    harness.hook(subagent_stop(TURN));

    let started_at = Instant::now();
    assert_eq!(
        next_method(&mut mock, "thread/read").await["params"]["threadId"],
        CHILD
    );
    let mut observed = vec![json!({"method": "thread/read", "params": {"threadId": CHILD}})];
    let second_read = loop {
        let call = next_relevant(&mut mock).await;
        let method = call["method"].as_str().unwrap();
        assert!(
            !matches!(
                method,
                "thread/resume" | "thread/compact/start" | "thread/delete"
            ),
            "active child mutated before its retry: {call}"
        );
        if method == "thread/read" {
            break call;
        }
        observed.push(call);
    };
    assert_eq!(second_read["params"]["threadId"], CHILD);
    assert!(
        started_at.elapsed() >= Duration::from_millis(700),
        "active child retry ran too early"
    );
    observed.push(second_read);
    let later = collect_calls(&mut mock, Duration::from_millis(1500)).await;
    observed.extend(later);
    assert_no_mutation(&observed);
}

#[tokio::test]
async fn pending_job_survives_service_restart() {
    let mut harness = Harness::start(5, 30).await;
    let listener = harness.listener();
    let root = thread_view(ROOT, SESSION, ROOT_PATH, None, "idle", true);
    let mut mock = start_rpc_mock(
        listener,
        RpcPlan::new(root, turns(vec![completed_turn(TURN)]), TURN),
    )
    .await;
    let stderr_before = harness.stderr_text();

    harness.hook(root_stop(TURN));
    let persisted = wait_for_persisted_turn(&harness.root.join("state"), TURN).await;
    assert!(persisted.starts_with(harness.root.join("state")));
    harness.stop_service();
    harness.restart(5, 30).await;

    assert_root_compaction(&harness, &mut mock, TURN, &stderr_before).await;
}

#[tokio::test]
async fn dispatched_uncertain_compaction_is_not_replayed_after_service_restart() {
    let mut harness = Harness::start(1, 1).await;
    let listener = harness.listener();
    let root = thread_view(ROOT, SESSION, ROOT_PATH, None, "idle", true);
    let mut plan = RpcPlan::new(root, turns(vec![completed_turn(TURN)]), TURN);
    let (release_compaction, compact_gate) = oneshot::channel();
    plan.compact_response_gate = Some(compact_gate);
    let mut mock = start_rpc_mock(listener, plan).await;

    harness.hook(root_stop(TURN));
    compact_after_resume(&mut mock, ROOT).await;
    harness.stop_service();
    release_compaction.send(()).unwrap();
    harness.restart(1, 1).await;

    let after_restart = collect_calls(&mut mock, Duration::from_millis(2500)).await;
    assert_no_mutation(&after_restart);
    harness.assert_service_running();
}

#[tokio::test]
async fn unavailable_app_server_is_retried_after_retry_interval() {
    let harness = Harness::start(1, 1).await;
    let stderr_before = harness.stderr_text();
    harness.hook(root_stop(TURN));
    tokio::time::sleep(Duration::from_millis(1800)).await;

    let listener = harness.listener();
    let root = thread_view(ROOT, SESSION, ROOT_PATH, None, "idle", true);
    let mut mock = start_rpc_mock(
        listener,
        RpcPlan::new(root, turns(vec![completed_turn(TURN)]), TURN),
    )
    .await;

    assert_root_compaction(&harness, &mut mock, TURN, &stderr_before).await;
}

#[tokio::test]
async fn prompt_cancellation_prevents_a_failed_network_attempt_from_retrying() {
    let harness = Harness::start(1, 1).await;
    harness.hook(root_stop(TURN));
    tokio::time::sleep(Duration::from_millis(1500)).await;
    harness.hook(root_prompt());

    let listener = harness.listener();
    let root = thread_view(ROOT, SESSION, ROOT_PATH, None, "idle", true);
    let mut mock = start_rpc_mock(
        listener,
        RpcPlan::new(root, turns(vec![completed_turn(TURN)]), TURN),
    )
    .await;
    let calls = collect_calls(&mut mock, Duration::from_millis(2200)).await;
    assert!(
        calls.is_empty(),
        "cancelled retry reached app-server: {calls:?}"
    );
}

#[tokio::test]
async fn root_prompt_blocks_a_completed_turn_at_the_default_thirty_minute_window() {
    let completed_at = unix_now_seconds().saturating_sub(30 * 60);
    let (output, calls) = prompt_with_history(
        None,
        turns(vec![prompt_turn(TURN, "completed", Some(completed_at))]),
        json!({"data": [], "nextCursor": null}),
        "Continue the migration from the last session.",
    )
    .await;

    assert_prompt_block(output);
    assert_no_prompt_dispatch(&calls);
}

#[tokio::test]
async fn root_prompt_is_silent_without_stale_uncompacted_history() {
    let now = unix_now_seconds();
    let compacted = json!({
        "data": [{
            "turnId": TURN,
            "item": {"id": "compact-1", "type": "contextCompaction"},
        }],
        "nextCursor": null,
    });
    let cases = [
        (turns(Vec::new()), json!({"data": [], "nextCursor": null})),
        (
            turns(vec![prompt_turn(
                TURN,
                "completed",
                Some(now.saturating_sub(60)),
            )]),
            json!({"data": [], "nextCursor": null}),
        ),
        (
            turns(vec![prompt_turn(
                TURN,
                "completed",
                Some(now.saturating_sub(7200)),
            )]),
            compacted,
        ),
    ];

    for (history, items) in cases {
        let (output, calls) = prompt_with_history(
            None,
            history,
            items,
            "Continue the migration from the last session.",
        )
        .await;
        assert_prompt_silent(output);
        assert_no_prompt_dispatch(&calls);
    }
}

#[tokio::test]
async fn legacy_and_omitted_history_modes_use_full_turn_items() {
    let stale_at = unix_now_seconds().saturating_sub(7200);
    for (history_mode_name, history_mode) in
        [("explicit legacy", Some("legacy")), ("omitted", None)]
    {
        let thread = root_thread_with_history_mode(history_mode);
        let (stale_output, stale_calls) = prompt_with_legacy_history(
            thread.clone(),
            turns(vec![prompt_turn(TURN, "completed", Some(stale_at))]),
            "Continue from the stale uncompacted turn.",
        )
        .await;
        let stale_reason = assert_prompt_block(stale_output);
        assert!(
            !stale_reason.to_lowercase().contains("unknown"),
            "{history_mode_name} stale turn was treated as having unknown age: {stale_reason}"
        );
        assert_legacy_turn_items_view(&stale_calls, history_mode_name);
        assert_no_prompt_dispatch(&stale_calls);

        let mut compacted_turn = prompt_turn(TURN, "completed", Some(stale_at));
        compacted_turn["items"] = json!([{
            "id": "legacy-context-compaction",
            "type": "contextCompaction",
        }]);
        let (compacted_output, compacted_calls) = prompt_with_legacy_history(
            thread.clone(),
            turns(vec![compacted_turn]),
            "Continue after the completed compaction.",
        )
        .await;
        assert_prompt_silent(compacted_output);
        assert_legacy_turn_items_view(&compacted_calls, history_mode_name);
        assert_no_prompt_dispatch(&compacted_calls);

        let (unknown_output, unknown_calls) = prompt_with_legacy_history(
            thread,
            turns(vec![prompt_turn(TURN, "completed", None)]),
            "Continue from a turn with unknown completion time.",
        )
        .await;
        let unknown_reason = assert_prompt_block(unknown_output);
        assert_unknown_age_reason(&unknown_reason);
        assert_legacy_turn_items_view(&unknown_calls, history_mode_name);
        assert_no_prompt_dispatch(&unknown_calls);
    }
}

#[tokio::test]
async fn compaction_on_another_turn_does_not_exempt_the_latest_stale_turn() {
    let stale = prompt_turn(
        TURN,
        "completed",
        Some(unix_now_seconds().saturating_sub(7200)),
    );
    let unrelated_compaction = json!({
        "data": [{
            "turnId": NEW_TURN,
            "item": {"id": "compact-other", "type": "contextCompaction"},
        }],
        "nextCursor": null,
    });
    let (output, calls) = prompt_with_history(
        None,
        turns(vec![stale]),
        unrelated_compaction,
        "Continue from the old completed turn.",
    )
    .await;

    assert!(
        !output.stdout.is_empty(),
        "expected unrelated compaction not to exempt stale turn; RPCs: {calls:?}"
    );
    assert_prompt_block(output);
    assert_no_prompt_dispatch(&calls);
}

#[tokio::test]
async fn root_prompt_freshness_uses_the_configured_window() {
    let now = unix_now_seconds();
    let (short_window_output, short_window_calls) = prompt_with_history(
        Some(20 * 60),
        turns(vec![prompt_turn(
            TURN,
            "completed",
            Some(now.saturating_sub(25 * 60)),
        )]),
        json!({"data": [], "nextCursor": null}),
        "Resume the earlier task.",
    )
    .await;
    assert_prompt_block(short_window_output);
    assert_no_prompt_dispatch(&short_window_calls);

    let (long_window_output, long_window_calls) = prompt_with_history(
        Some(60 * 60),
        turns(vec![prompt_turn(
            TURN,
            "completed",
            Some(now.saturating_sub(35 * 60)),
        )]),
        json!({"data": [], "nextCursor": null}),
        "Resume the earlier task.",
    )
    .await;
    assert_prompt_silent(long_window_output);
    assert_no_prompt_dispatch(&long_window_calls);
}

#[tokio::test]
async fn root_prompt_allows_one_exact_retry_then_blocks_later_or_changed_messages() {
    let harness = Harness::start(60, 30).await;
    let listener = harness.listener();
    let root = thread_view(ROOT, SESSION, ROOT_PATH, None, "idle", true);
    let stale = prompt_turn(
        TURN,
        "completed",
        Some(unix_now_seconds().saturating_sub(7200)),
    );
    let mut mock = start_rpc_mock(listener, RpcPlan::new(root, turns(vec![stale]), NEW_TURN)).await;
    let same_message = "Continue the migration from the last session.";

    let first = harness.hook_output(root_prompt_with(same_message)).await;
    assert_prompt_block(first);
    let immediate_retry = harness.hook_output(root_prompt_with(same_message)).await;
    assert_prompt_silent(immediate_retry);
    let later_submission = harness.hook_output(root_prompt_with(same_message)).await;
    assert_prompt_block(later_submission);
    let changed_message = "Continue the migration, but skip the tests.";
    let changed_retry = harness.hook_output(root_prompt_with(changed_message)).await;
    assert_prompt_block(changed_retry);

    let calls = collect_all_calls(&mut mock, Duration::from_millis(100)).await;
    assert_no_prompt_dispatch(&calls);
}

#[tokio::test]
async fn root_prompt_with_unknown_completion_time_blocks_and_allows_one_exact_retry() {
    let harness = Harness::start(60, 30).await;
    let listener = harness.listener();
    let root = thread_view(ROOT, SESSION, ROOT_PATH, None, "idle", true);
    let mut mock = start_rpc_mock(
        listener,
        RpcPlan::new(
            root,
            turns(vec![prompt_turn(TURN, "completed", None)]),
            NEW_TURN,
        ),
    )
    .await;
    let message = "Continue the task with unavailable history time.";

    let first = harness.hook_output(root_prompt_with(message)).await;
    let reason = assert_prompt_block(first);
    assert_unknown_age_reason(&reason);
    let retry = harness.hook_output(root_prompt_with(message)).await;
    assert_prompt_silent(retry);

    let calls = collect_all_calls(&mut mock, Duration::from_millis(100)).await;
    assert_no_prompt_dispatch(&calls);
}

#[tokio::test]
async fn root_prompt_ignores_in_progress_and_interrupted_turns_across_pages() {
    let now = unix_now_seconds();
    let harness = Harness::start(60, 30).await;
    let listener = harness.listener();
    let root = thread_view(ROOT, SESSION, ROOT_PATH, None, "idle", true);
    let mut plan = RpcPlan::new(
        root,
        json!({
            "data": [
                prompt_turn(NEW_TURN, "inProgress", None),
                prompt_turn(FORK, "interrupted", Some(now.saturating_sub(5))),
            ],
            "nextCursor": "older-page",
        }),
        NEW_TURN,
    );
    plan.turn_pages_after.push((
        "older-page".to_owned(),
        turns(vec![prompt_turn(
            TURN,
            "completed",
            Some(now.saturating_sub(7200)),
        )]),
    ));
    let mut mock = start_rpc_mock(listener, plan).await;

    let output = harness
        .hook_output(root_prompt_with(
            "Continue from the most recent completed work.",
        ))
        .await;
    let reason = assert_prompt_block(output);
    assert!(
        !reason.to_lowercase().contains("unknown"),
        "the stale completed turn was treated as having an unknown age: {reason}"
    );
    let calls = collect_all_calls(&mut mock, Duration::from_millis(100)).await;
    let turn_pages = calls
        .iter()
        .filter(|call| call["method"] == "thread/turns/list")
        .collect::<Vec<_>>();
    assert!(
        turn_pages.len() >= 2,
        "turn history was not paginated: {calls:?}"
    );
    assert_eq!(turn_pages[1]["params"]["cursor"], "older-page");
    assert_no_prompt_dispatch(&calls);
}

#[tokio::test]
async fn root_prompt_fails_open_without_service_or_app_server() {
    let mut no_service = Harness::start(60, 30).await;
    no_service.stop_service();
    no_service
        .hook_with_timeout(
            root_prompt_with("A prompt with no scheduler service."),
            Duration::from_secs(3),
        )
        .await;

    let no_app_server = Harness::start(60, 30).await;
    no_app_server
        .hook_with_timeout(
            root_prompt_with("A prompt with no app server."),
            Duration::from_secs(3),
        )
        .await;
}

#[tokio::test]
async fn subagent_prompt_cancels_pending_work_without_blocking() {
    let harness = Harness::start(1, 30).await;
    let listener = harness.listener();
    let child = thread_view(CHILD, SESSION, FORK_PATH, Some(ROOT), "idle", true);
    let stale = prompt_turn(
        TURN,
        "completed",
        Some(unix_now_seconds().saturating_sub(7200)),
    );
    let mut mock = start_rpc_mock(listener, RpcPlan::new(child, turns(vec![stale]), TURN)).await;

    harness.hook(subagent_stop(TURN));
    assert_prompt_silent(
        harness
            .hook_output(subagent_prompt_with("Continue the child task."))
            .await,
    );
    let calls = collect_all_calls(&mut mock, Duration::from_millis(1500)).await;
    assert!(
        calls.is_empty(),
        "subagent prompt reached app-server: {calls:?}"
    );
}

#[tokio::test]
async fn blocked_prompt_text_is_not_written_to_persistent_scheduler_state() {
    let harness = Harness::start(60, 30).await;
    let state_home = harness.root.join("state");

    let listener = harness.listener();
    let root = thread_view(ROOT, SESSION, ROOT_PATH, None, "idle", true);
    let stale = prompt_turn(
        TURN,
        "completed",
        Some(unix_now_seconds().saturating_sub(7200)),
    );
    let mut mock = start_rpc_mock(listener, RpcPlan::new(root, turns(vec![stale]), NEW_TURN)).await;
    let prompt = "DO-NOT-PERSIST-user-prompt-6ac9859e-41a0-4a36-92df-d44a671f8483";
    assert_prompt_block(harness.hook_output(root_prompt_with(prompt)).await);
    let _ = collect_all_calls(&mut mock, Duration::from_millis(100)).await;

    let contains_prompt = state_contains_text(&state_home, prompt).unwrap_or_else(|error| {
        panic!("failed to inspect persistent scheduler state under {state_home:?}: {error}")
    });
    assert!(
        !contains_prompt,
        "user prompt content appeared in persistent scheduler state"
    );
}

#[tokio::test]
async fn boot_clock_helper_uses_shifted_wall_time_for_untracked_prompt_fallback() {
    let harness = Harness::start_clocked(60, 30, Some(30 * 60)).await;
    let listener = harness.listener();
    let root = thread_view(ROOT, SESSION, ROOT_PATH, None, "idle", true);
    let completed_at = unix_now_seconds();
    let mut mock = start_rpc_mock(
        listener,
        RpcPlan::new(
            root,
            turns(vec![prompt_turn(TURN, "completed", Some(completed_at))]),
            NEW_TURN,
        ),
    )
    .await;
    harness.set_realtime_offset(60 * 60);

    let output = harness
        .hook_output(root_prompt_with("Continue using wall-time fallback."))
        .await;
    assert_prompt_block(output);
    let calls = collect_all_calls(&mut mock, Duration::from_millis(100)).await;
    assert_no_prompt_dispatch(&calls);
}

#[tokio::test]
async fn boot_clock_tracked_prompt_age_uses_elapsed_time_after_wall_forward_jump() {
    let harness = Harness::start_clocked(60, 30, Some(5)).await;
    let listener = harness.listener();
    let root = thread_view(ROOT, SESSION, ROOT_PATH, None, "idle", true);
    let completed_at = unix_now_seconds();
    let mut mock = start_rpc_mock(
        listener,
        RpcPlan::new(
            root,
            turns(vec![prompt_turn(TURN, "completed", Some(completed_at))]),
            NEW_TURN,
        ),
    )
    .await;

    harness.hook(root_stop(TURN));
    wait_for_persisted_turn(&harness.root.join("state"), TURN).await;
    harness.set_realtime_offset(60 * 60);
    let output = harness
        .hook_output(root_prompt_with("Continue the just completed work."))
        .await;

    assert_prompt_silent(output);
    let calls = collect_all_calls(&mut mock, Duration::from_millis(100)).await;
    assert_no_prompt_dispatch(&calls);
}

#[tokio::test]
async fn boot_clock_tracked_stale_prompt_after_wall_rollback_is_blocked() {
    let harness = Harness::start_clocked(60, 30, Some(3)).await;
    let listener = harness.listener();
    let root = thread_view(ROOT, SESSION, ROOT_PATH, None, "idle", true);
    let mut mock = start_rpc_mock(
        listener,
        RpcPlan::new(
            root,
            turns(vec![prompt_turn(
                TURN,
                "completed",
                Some(unix_now_seconds()),
            )]),
            NEW_TURN,
        ),
    )
    .await;

    harness.hook(root_stop(TURN));
    wait_for_persisted_turn(&harness.root.join("state"), TURN).await;
    tokio::time::sleep(Duration::from_millis(3500)).await;
    harness.set_realtime_offset(-60 * 60);
    let output = harness
        .hook_output(root_prompt_with(
            "Continue from the tracked completed turn.",
        ))
        .await;

    assert_prompt_block(output);
    let calls = collect_all_calls(&mut mock, Duration::from_millis(100)).await;
    assert_no_prompt_dispatch(&calls);
}

#[tokio::test]
async fn boot_clock_tracked_prompt_age_survives_same_boot_restart_and_wall_rollback() {
    let mut harness = Harness::start_clocked(60, 30, Some(3)).await;
    let listener = harness.listener();
    let root = thread_view(ROOT, SESSION, ROOT_PATH, None, "idle", true);
    let mut mock = start_rpc_mock(
        listener,
        RpcPlan::new(
            root,
            turns(vec![prompt_turn(
                TURN,
                "completed",
                Some(unix_now_seconds()),
            )]),
            NEW_TURN,
        ),
    )
    .await;
    let stop_at = Instant::now();

    harness.hook(root_stop(TURN));
    wait_for_persisted_turn(&harness.root.join("state"), TURN).await;
    harness.stop_service();
    harness.restart(60, 30).await;
    tokio::time::sleep_until(stop_at + Duration::from_secs(4)).await;
    harness.set_realtime_offset(-60 * 60);
    let output = harness
        .hook_output(root_prompt_with("Continue after helper restart."))
        .await;

    assert_prompt_block(output);
    let calls = collect_all_calls(&mut mock, Duration::from_millis(100)).await;
    assert_no_prompt_dispatch(&calls);
}

#[tokio::test]
async fn boot_clock_wall_forward_jump_does_not_run_or_expire_a_delayed_compaction() {
    let harness = Harness::start_clocked(2, 1, Some(8)).await;
    let listener = harness.listener();
    let root = thread_view(ROOT, SESSION, ROOT_PATH, None, "idle", true);
    let mut mock = start_rpc_mock(
        listener,
        RpcPlan::new(root, turns(vec![completed_turn(TURN)]), TURN),
    )
    .await;

    harness.hook(root_stop(TURN));
    wait_for_persisted_turn(&harness.root.join("state"), TURN).await;
    harness.set_realtime_offset(60 * 60);
    let early = collect_calls(&mut mock, Duration::from_millis(500)).await;
    assert!(
        early.is_empty(),
        "wall-clock forward jump ran the job before its elapsed delay: {early:?}"
    );

    let stderr_before = harness.stderr_text();
    assert_root_compaction(&harness, &mut mock, TURN, &stderr_before).await;
}

#[tokio::test]
async fn boot_clock_wall_rollback_during_thread_read_does_not_change_elapsed_deadline() {
    let harness = Harness::start_clocked(1, 1, Some(6)).await;
    let listener = harness.listener();
    let root = thread_view(ROOT, SESSION, ROOT_PATH, None, "idle", true);
    let mut plan = RpcPlan::new(root, turns(vec![completed_turn(TURN)]), TURN);
    let (release_read, read_gate) = oneshot::channel();
    plan.thread_read_gate = Some(read_gate);
    let mut mock = start_rpc_mock(listener, plan).await;
    let stderr_before = harness.stderr_text();

    harness.hook(root_stop(TURN));
    let read = next_method(&mut mock, "thread/read").await;
    assert_eq!(read["params"]["threadId"], ROOT);
    harness.set_realtime_offset(-60 * 60);
    release_read.send(()).unwrap();

    let before_resume = calls_until_mutation_after(&mut mock, "thread/resume", vec![read]).await;
    assert_final_turn_check(&before_resume, ROOT);
    assert_eq!(before_resume.last().unwrap()["params"]["threadId"], ROOT);
    let before_compact = calls_until_mutation(&mut mock, "thread/compact/start").await;
    assert_final_turn_check(&before_compact, ROOT);
    assert_eq!(before_compact.last().unwrap()["params"]["threadId"], ROOT);
    assert_compaction_events(&mut mock, ROOT, TURN, "completed", true).await;
    wait_for_compacted(&harness, &stderr_before).await;
}

#[tokio::test]
async fn boot_clock_wall_rollback_during_thread_read_cannot_compact_after_expiry() {
    let mut harness = Harness::start_clocked(1, 1, Some(3)).await;
    let listener = harness.listener();
    let root = thread_view(ROOT, SESSION, ROOT_PATH, None, "idle", true);
    let mut plan = RpcPlan::new(root, turns(vec![completed_turn(TURN)]), TURN);
    let (release_read, read_gate) = oneshot::channel();
    plan.thread_read_gate = Some(read_gate);
    let mut mock = start_rpc_mock(listener, plan).await;
    let stop_at = Instant::now();

    harness.hook(root_stop(TURN));
    let read = next_method(&mut mock, "thread/read").await;
    assert_eq!(read["params"]["threadId"], ROOT);
    harness.set_realtime_offset(-60 * 60);
    tokio::time::sleep_until(stop_at + Duration::from_secs(4)).await;
    release_read.send(()).unwrap();

    let calls = collect_calls(&mut mock, Duration::from_millis(1500)).await;
    assert_no_mutation(&calls);
    harness.assert_service_running();
}

#[tokio::test]
async fn boot_clock_compaction_response_after_expiry_is_not_retried() {
    let mut harness = Harness::start_clocked(1, 1, Some(3)).await;
    let listener = harness.listener();
    let root = thread_view(ROOT, SESSION, ROOT_PATH, None, "idle", true);
    let mut plan = RpcPlan::new(root, turns(vec![completed_turn(TURN)]), TURN);
    let (release_compaction, compact_gate) = oneshot::channel();
    plan.compact_response_gate = Some(compact_gate);
    let mut mock = start_rpc_mock(listener, plan).await;
    let stop_at = Instant::now();
    let stderr_before = harness.stderr_text();

    harness.hook(root_stop(TURN));
    let jobs_file = wait_for_persisted_turn(&harness.root.join("state"), TURN).await;
    compact_after_resume(&mut mock, ROOT).await;
    harness.set_realtime_offset(-60 * 60);
    tokio::time::sleep_until(stop_at + Duration::from_secs(4)).await;

    wait_for_inflight_expiry(&harness, &stderr_before, &jobs_file, TURN).await;
    assert!(
        !persisted_jobs_contain_turn(&jobs_file, TURN),
        "expired job remained persisted while compact response was held"
    );
    release_compaction.send(()).unwrap();

    let after_response = collect_calls(&mut mock, Duration::from_millis(2500)).await;
    assert!(
        !after_response
            .iter()
            .any(|call| call["method"] == "thread/compact/start"),
        "expired in-flight compaction was dispatched again: {after_response:?}"
    );
    harness.assert_service_running();
}

#[tokio::test]
async fn boot_clock_persisted_job_survives_same_boot_restart_and_wall_jump() {
    let mut harness = Harness::start_clocked(3, 1, Some(9)).await;
    let listener = harness.listener();
    let root = thread_view(ROOT, SESSION, ROOT_PATH, None, "idle", true);
    let mut mock = start_rpc_mock(
        listener,
        RpcPlan::new(root, turns(vec![completed_turn(TURN)]), TURN),
    )
    .await;
    let stderr_before = harness.stderr_text();

    harness.hook(root_stop(TURN));
    wait_for_persisted_turn(&harness.root.join("state"), TURN).await;
    harness.set_realtime_offset(60 * 60);
    harness.stop_service();
    harness.restart(3, 1).await;

    assert_root_compaction(&harness, &mut mock, TURN, &stderr_before).await;
}

#[tokio::test]
async fn boot_clock_saved_jobs_from_another_or_unknown_boot_domain_never_compact() {
    for boot_id in [Some("different-boot-time-domain"), None] {
        let mut harness = Harness::start_clocked(1, 1, Some(6)).await;
        let listener = harness.listener();
        let root = thread_view(ROOT, SESSION, ROOT_PATH, None, "idle", true);
        let mut mock = start_rpc_mock(
            listener,
            RpcPlan::new(root, turns(vec![completed_turn(TURN)]), TURN),
        )
        .await;

        harness.hook(root_stop(TURN));
        let state_home = harness.root.join("state");
        wait_for_persisted_turn(&state_home, TURN).await;
        set_persisted_job_boot_id(&state_home, TURN, boot_id);
        harness.stop_service();
        harness.restart(1, 1).await;

        let calls = collect_calls(&mut mock, Duration::from_millis(2500)).await;
        assert_no_mutation(&calls);
        harness.assert_service_running();
    }
}

#[tokio::test]
async fn delayed_compaction_does_not_start_after_the_window_expires() {
    let mut harness = Harness::start_with_window(2, 1, Some(1)).await;
    let listener = harness.listener();
    let root = thread_view(ROOT, SESSION, ROOT_PATH, None, "idle", true);
    let mut mock = start_rpc_mock(
        listener,
        RpcPlan::new(root, turns(vec![completed_turn(TURN)]), TURN),
    )
    .await;
    harness.hook(root_stop(TURN));
    wait_for_persisted_turn(&harness.root.join("state"), TURN).await;
    let calls = collect_all_calls(&mut mock, Duration::from_millis(2500)).await;
    harness.assert_service_running();
    assert!(
        !calls
            .iter()
            .any(|call| call["method"] == "thread/compact/start"),
        "expired delayed job started compaction: {calls:?}"
    );
}

#[tokio::test]
async fn compaction_does_not_start_when_thread_read_finishes_after_expiry() {
    let harness = Harness::start_with_window(1, 1, Some(3)).await;
    let listener = harness.listener();
    let root = thread_view(ROOT, SESSION, ROOT_PATH, None, "idle", true);
    let mut plan = RpcPlan::new(root, turns(vec![completed_turn(TURN)]), TURN);
    let (release_read, read_gate) = oneshot::channel();
    plan.thread_read_gate = Some(read_gate);
    let mut mock = start_rpc_mock(listener, plan).await;
    let stop_at = Instant::now();

    harness.hook(root_stop(TURN));
    let read = next_method(&mut mock, "thread/read").await;
    assert_eq!(read["params"]["threadId"], ROOT);
    tokio::time::sleep_until(stop_at + Duration::from_millis(3200)).await;
    release_read.send(()).unwrap();

    let calls = collect_all_calls(&mut mock, Duration::from_millis(1500)).await;
    assert!(
        !calls
            .iter()
            .any(|call| call["method"] == "thread/compact/start"),
        "job held at thread/read started compaction after expiry: {calls:?}"
    );
}

#[tokio::test]
async fn delayed_retry_does_not_start_compaction_after_the_window_expires() {
    let mut harness = Harness::start_with_window(1, 3, Some(3)).await;
    harness.hook(root_stop(TURN));
    wait_for_persisted_turn(&harness.root.join("state"), TURN).await;
    tokio::time::sleep(Duration::from_millis(2200)).await;

    let listener = harness.listener();
    let root = thread_view(ROOT, SESSION, ROOT_PATH, None, "idle", true);
    let mut mock = start_rpc_mock(
        listener,
        RpcPlan::new(root, turns(vec![completed_turn(TURN)]), TURN),
    )
    .await;
    let calls = collect_all_calls(&mut mock, Duration::from_millis(2500)).await;
    harness.assert_service_running();
    assert!(
        !calls
            .iter()
            .any(|call| call["method"] == "thread/compact/start"),
        "expired retry started compaction: {calls:?}"
    );
}

#[tokio::test]
async fn persisted_compaction_job_does_not_resume_after_the_window_expires() {
    let mut harness = Harness::start_with_window(60, 1, Some(1)).await;
    let listener = harness.listener();
    let root = thread_view(ROOT, SESSION, ROOT_PATH, None, "idle", true);
    let mut mock = start_rpc_mock(
        listener,
        RpcPlan::new(root, turns(vec![completed_turn(TURN)]), TURN),
    )
    .await;
    harness.hook(root_stop(TURN));
    wait_for_persisted_turn(&harness.root.join("state"), TURN).await;
    harness.stop_service();
    tokio::time::sleep(Duration::from_millis(1200)).await;
    harness.restart(1, 1).await;

    let calls = collect_all_calls(&mut mock, Duration::from_millis(1800)).await;
    assert!(
        !calls
            .iter()
            .any(|call| call["method"] == "thread/compact/start"),
        "persisted expired job started compaction: {calls:?}"
    );
}

#[tokio::test]
async fn expired_window_does_not_prevent_deleting_an_ineligible_subagent() {
    let harness = Harness::start_with_window(2, 30, Some(1)).await;
    let listener = harness.listener();
    let child = thread_view(CHILD, SESSION, ROOT_PATH, Some(ROOT), "idle", false);
    let mut mock = start_rpc_mock(
        listener,
        RpcPlan::new(child, turns(vec![completed_turn(TURN)]), TURN),
    )
    .await;
    harness.hook(subagent_stop(TURN));

    let calls = calls_until_child_delete_after(&mut mock, CHILD, Vec::new()).await;
    assert_eq!(calls.last().unwrap()["method"], "thread/delete");
    assert!(
        !calls
            .iter()
            .any(|call| call["method"] == "thread/compact/start"),
        "ineligible child was compacted instead of deleted: {calls:?}"
    );
}

#[tokio::test]
async fn plugin_stop_persists_identity_and_uses_observed_boot_time_before_compacting() {
    let harness = Harness::start_with_window(20, 30, Some(45)).await;
    let listener = harness.listener();
    let root = thread_view(ROOT, SESSION, ROOT_PATH, None, "idle", true);
    let mut plan = RpcPlan::new(root, turns(vec![completed_turn(TURN)]), TURN);
    plan.plugin_status = plugin_status(vec![plugin_record(PLUGIN_ID, true, true)]);
    let mut mock = start_rpc_mock(listener, plan).await;
    let stderr_before = harness.stderr_text();
    let observed_at = boot_time_seconds().saturating_sub(15);

    let output = harness.hook_with_metadata(root_stop(TURN), Some(PLUGIN_ID), Some(observed_at));
    assert!(output.status.success());
    assert!(output.stdout.is_empty());

    let jobs_file = wait_for_persisted_turn(&harness.root.join("state"), TURN).await;
    let jobs: Value = serde_json::from_slice(&fs::read(jobs_file).unwrap()).unwrap();
    let saved_job = jobs
        .as_object()
        .unwrap()
        .values()
        .find(|job| job["turn_id"] == TURN)
        .expect("scheduled root job was not persisted");
    assert!(
        json_contains_string(saved_job, PLUGIN_ID),
        "scheduled job lost its plugin identity: {saved_job}"
    );

    let calls = calls_until_method(&mut mock, "thread/compact/start").await;
    let status_index = calls
        .iter()
        .position(|call| call["method"] == "plugin/installed")
        .expect("plugin compaction skipped plugin/installed");
    let first_mutation_index = calls
        .iter()
        .position(|call| {
            matches!(
                call["method"].as_str(),
                Some("thread/resume" | "thread/compact/start" | "thread/delete")
            )
        })
        .expect("root compaction did not issue a mutation");
    assert!(
        status_index < first_mutation_index,
        "root job mutated before plugin authorization: {calls:?}"
    );
    assert_final_turn_check(&calls, ROOT);
    assert_compaction_events(&mut mock, ROOT, TURN, "completed", true).await;
    wait_for_compacted(&harness, &stderr_before).await;
}

#[tokio::test]
async fn plugin_subagent_stop_checks_installation_before_deleting_child() {
    let harness = Harness::start_with_window(1, 30, Some(20)).await;
    let listener = harness.listener();
    let child = thread_view(CHILD, SESSION, ROOT_PATH, Some(ROOT), "idle", false);
    let mut plan = RpcPlan::new(child, turns(vec![completed_turn(TURN)]), TURN);
    plan.plugin_status = plugin_status(vec![plugin_record(PLUGIN_ID, true, true)]);
    let mut mock = start_rpc_mock(listener, plan).await;

    let output = harness.hook_with_metadata(
        subagent_stop(TURN),
        Some(PLUGIN_ID),
        Some(boot_time_seconds()),
    );
    assert!(output.status.success());
    assert!(output.stdout.is_empty());

    let calls = calls_until_method(&mut mock, "thread/delete").await;
    let status_index = calls
        .iter()
        .position(|call| call["method"] == "plugin/installed")
        .expect("plugin subagent deletion skipped plugin/installed");
    assert!(
        status_index < calls.len() - 1,
        "child deletion preceded plugin authorization: {calls:?}"
    );
    assert_final_turn_check(&calls, CHILD);
    assert!(calls.iter().any(|call| {
        call["method"] == "thread/list" && call["params"]["ancestorThreadId"] == CHILD
    }));
    assert_eq!(calls.last().unwrap()["params"]["threadId"], CHILD);
}

#[tokio::test]
async fn plugin_root_stop_does_not_mutate_for_disabled_uninstalled_or_missing_plugin() {
    let cases = [
        (
            "disabled",
            plugin_status(vec![plugin_record(PLUGIN_ID, true, false)]),
        ),
        (
            "uninstalled",
            plugin_status(vec![plugin_record(PLUGIN_ID, false, true)]),
        ),
        (
            "missing",
            plugin_status(vec![plugin_record(
                "another-plugin@marketplace",
                true,
                true,
            )]),
        ),
    ];

    for (case, status) in cases {
        let mut harness = Harness::start_with_window(1, 1, Some(20)).await;
        let listener = harness.listener();
        let root = thread_view(ROOT, SESSION, ROOT_PATH, None, "idle", true);
        let mut plan = RpcPlan::new(root, turns(vec![completed_turn(TURN)]), TURN);
        plan.plugin_status = status;
        let mut mock = start_rpc_mock(listener, plan).await;

        let output =
            harness.hook_with_metadata(root_stop(TURN), Some(PLUGIN_ID), Some(boot_time_seconds()));
        assert!(output.status.success(), "{case} hook failed");
        assert!(output.stdout.is_empty(), "{case} hook was not silent");

        let calls = collect_calls(&mut mock, Duration::from_millis(2500)).await;
        assert!(
            calls
                .iter()
                .any(|call| call["method"] == "plugin/installed"),
            "{case} plugin stop never checked installation: {calls:?}"
        );
        assert_no_mutation(&calls);
        harness.assert_service_running();
    }
}

#[tokio::test]
async fn unavailable_plugin_status_fails_safely_for_root_and_subagent_jobs() {
    for subagent in [false, true] {
        let mut harness = Harness::start_with_window(1, 1, Some(20)).await;
        let listener = harness.listener();
        let thread = if subagent {
            thread_view(CHILD, SESSION, ROOT_PATH, Some(ROOT), "idle", false)
        } else {
            thread_view(ROOT, SESSION, ROOT_PATH, None, "idle", true)
        };
        let mut plan = RpcPlan::new(thread, turns(vec![completed_turn(TURN)]), TURN);
        plan.plugin_status_unavailable = true;
        let mut mock = start_rpc_mock(listener, plan).await;
        let event = if subagent {
            subagent_stop(TURN)
        } else {
            root_stop(TURN)
        };

        let output = harness.hook_with_metadata(event, Some(PLUGIN_ID), Some(boot_time_seconds()));
        assert!(output.status.success());
        assert!(output.stdout.is_empty());

        let calls = collect_calls(&mut mock, Duration::from_millis(2500)).await;
        assert!(
            calls
                .iter()
                .any(|call| call["method"] == "plugin/installed"),
            "plugin status was unavailable but never requested: {calls:?}"
        );
        assert_no_mutation(&calls);
        harness.assert_service_running();
    }
}

#[tokio::test]
async fn expired_observed_plugin_stop_never_starts_root_compaction() {
    let mut harness = Harness::start_with_window(1, 1, Some(4)).await;
    let listener = harness.listener();
    let root = thread_view(ROOT, SESSION, ROOT_PATH, None, "idle", true);
    let mut plan = RpcPlan::new(root, turns(vec![completed_turn(TURN)]), TURN);
    plan.plugin_status = plugin_status(vec![plugin_record(PLUGIN_ID, true, true)]);
    let mut mock = start_rpc_mock(listener, plan).await;
    let observed_at = boot_time_seconds().saturating_sub(30);
    assert!(
        observed_at.saturating_add(4) < boot_time_seconds(),
        "test requires an observed time outside the configured cache window"
    );

    let output = harness.hook_with_metadata(root_stop(TURN), Some(PLUGIN_ID), Some(observed_at));
    assert!(output.status.success());
    assert!(output.stdout.is_empty());

    let calls = collect_calls(&mut mock, Duration::from_millis(2500)).await;
    assert_no_mutation(&calls);
    harness.assert_service_running();
}

#[tokio::test]
async fn older_stop_in_same_boot_second_does_not_replace_newer_job() {
    assert_out_of_order_stop_retains_newer_job(false).await;
}

#[tokio::test]
async fn equal_centisecond_stop_candidates_keep_the_server_latest_turn() {
    assert_out_of_order_stop_retains_newer_job(true).await;
}

#[tokio::test]
async fn equal_centisecond_stops_keep_latest_tracked_age_after_wall_forward_jump() {
    let harness = Harness::start_clocked(60, 30, Some(10)).await;
    let listener = harness.listener();
    let root = thread_view(ROOT, SESSION, ROOT_PATH, None, "idle", true);
    let completed_at = unix_now_seconds();
    let latest = prompt_turn(NEW_TURN, "completed", Some(completed_at));
    let plan = RpcPlan::new(root, turns(vec![latest]), NEW_TURN);
    let mut mock = start_rpc_mock(listener, plan).await;

    let newer_order = loop {
        let order = boot_time_centiseconds();
        if order % 100 >= 2 {
            break order;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    let observed_at = newer_order / 100;
    let newer = harness.hook_with_order(
        root_stop(NEW_TURN),
        None,
        Some(observed_at),
        Some(newer_order),
    );
    assert!(newer.status.success());
    assert!(newer.stdout.is_empty());
    wait_for_persisted_turn(&harness.root.join("state"), NEW_TURN).await;

    let delayed_older =
        harness.hook_with_order(root_stop(TURN), None, Some(observed_at), Some(newer_order));
    assert!(delayed_older.status.success());
    assert!(delayed_older.stdout.is_empty());

    harness.set_realtime_offset(60 * 60);
    let output = harness
        .hook_output(root_prompt_with("Continue the just completed work."))
        .await;

    assert_prompt_silent(output);
    let calls = collect_all_calls(&mut mock, Duration::from_millis(100)).await;
    assert!(
        calls
            .iter()
            .any(|call| call["method"] == "thread/turns/list"),
        "prompt check did not inspect the latest server turn: {calls:?}"
    );
    assert_no_prompt_dispatch(&calls);
}

async fn assert_out_of_order_stop_retains_newer_job(equal_order: bool) {
    let harness = Harness::start_with_window(4, 30, Some(30)).await;
    let listener = harness.listener();
    let root = thread_view(ROOT, SESSION, ROOT_PATH, None, "idle", true);
    let plan = RpcPlan::new(root, turns(vec![completed_turn(NEW_TURN)]), NEW_TURN);
    let mut mock = start_rpc_mock(listener, plan).await;

    let newer_order = loop {
        let order = boot_time_centiseconds();
        if order % 100 >= 2 {
            break order;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    let observed_at = newer_order / 100;
    let newer = harness.hook_with_order(
        root_stop(NEW_TURN),
        None,
        Some(observed_at),
        Some(newer_order),
    );
    assert!(newer.status.success());
    assert!(newer.stdout.is_empty());
    wait_for_persisted_turn(&harness.root.join("state"), NEW_TURN).await;

    let older_order = if equal_order {
        newer_order
    } else {
        newer_order - 1
    };
    let delayed_older =
        harness.hook_with_order(root_stop(TURN), None, Some(observed_at), Some(older_order));
    assert!(delayed_older.status.success());
    assert!(delayed_older.stdout.is_empty());

    let calls = calls_until_method(&mut mock, "thread/compact/start").await;
    assert_final_turn_check(&calls, ROOT);
    assert_compaction_events(&mut mock, ROOT, NEW_TURN, "completed", true).await;
}
