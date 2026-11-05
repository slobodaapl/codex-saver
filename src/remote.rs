use std::{collections::VecDeque, path::Path, time::Duration};

use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use tokio::{
    net::UnixStream,
    sync::{mpsc, oneshot},
    time::timeout,
};
use tokio_tungstenite::{WebSocketStream, client_async, tungstenite::Message};

use crate::{
    hook::Event,
    scheduler::{Activity, Job, WorkerEvent},
};

const REQUEST_TIMEOUT: Duration = Duration::from_secs(15);
const COMPACTION_TIMEOUT: Duration = Duration::from_secs(300);

#[derive(Debug)]
pub enum Outcome {
    Done(&'static str),
    Retry(String),
    Discard(String),
    Unknown(String),
}

pub enum PromptAge {
    Fresh,
    Stale,
    Unknown,
}

fn matches_scheduled_turn(job: &Job, turn: &Value) -> bool {
    matches!(turn["status"].as_str(), Some("completed" | "failed"))
        && turn["id"].as_str().is_some_and(|id| {
            id == job.turn_id
                || job
                    .alternate_turn_ids
                    .iter()
                    .any(|candidate| candidate == id)
        })
}

async fn plugin_enabled(client: &mut Client, job: &Job) -> Result<(), Outcome> {
    let Some(id) = &job.plugin_id else {
        return Ok(());
    };
    let result = client
        .request(
            "plugin/installed",
            json!({"cwds": job.cwd.iter().collect::<Vec<_>>() }),
        )
        .await
        .map_err(before_mutation)?;
    if result["marketplaceLoadErrors"]
        .as_array()
        .is_none_or(|errors| !errors.is_empty())
    {
        return Err(Outcome::Retry("plugin status is unavailable".into()));
    }
    let Some(marketplaces) = result["marketplaces"].as_array() else {
        return Err(Outcome::Retry("plugin status is unavailable".into()));
    };
    let enabled = marketplaces
        .iter()
        .filter_map(|m| m["plugins"].as_array())
        .flatten()
        .any(|p| p["id"] == *id && p["installed"] == true && p["enabled"] == true);
    if enabled {
        Ok(())
    } else {
        Err(Outcome::Discard("plugin is disabled or uninstalled".into()))
    }
}

enum RpcError {
    Transport(String),
    Remote(Value),
}

struct Client {
    ws: WebSocketStream<UnixStream>,
    notifications: VecDeque<Value>,
    next_id: u64,
}

impl Client {
    async fn connect(path: &Path) -> Result<Self, String> {
        let connect =
            async {
                let stream = UnixStream::connect(path).await.map_err(|e| e.to_string())?;
                let (ws, _) = client_async("ws://localhost/", stream)
                    .await
                    .map_err(|e| e.to_string())?;
                let mut client = Self {
                    ws,
                    notifications: VecDeque::new(),
                    next_id: 0,
                };
                client.call("initialize", json!({
                "clientInfo": {"name": "codex_saver", "version": env!("CARGO_PKG_VERSION")},
                "capabilities": {"experimentalApi": true}
            })).await.map_err(format_rpc_error)?;
                client
                    .ws
                    .send(Message::Text(
                        json!({"method":"initialized","params":{}})
                            .to_string()
                            .into(),
                    ))
                    .await
                    .map_err(|e| e.to_string())?;
                Ok(client)
            };
        timeout(REQUEST_TIMEOUT, connect)
            .await
            .map_err(|_| "connection timed out".to_owned())?
    }

    async fn next_value(&mut self) -> Result<Value, RpcError> {
        loop {
            match self.ws.next().await {
                Some(Ok(Message::Text(text))) => {
                    return serde_json::from_str(&text)
                        .map_err(|e| RpcError::Transport(format!("invalid JSON: {e}")));
                }
                Some(Ok(Message::Ping(payload))) => self
                    .ws
                    .send(Message::Pong(payload))
                    .await
                    .map_err(|e| RpcError::Transport(e.to_string()))?,
                Some(Ok(Message::Close(_))) | None => {
                    return Err(RpcError::Transport("server disconnected".into()));
                }
                Some(Err(e)) => return Err(RpcError::Transport(e.to_string())),
                Some(Ok(_)) => {}
            }
        }
    }

    async fn call(&mut self, method: &str, params: Value) -> Result<Value, RpcError> {
        self.next_id += 1;
        let id = self.next_id;
        self.ws
            .send(Message::Text(
                json!({"id":id,"method":method,"params":params})
                    .to_string()
                    .into(),
            ))
            .await
            .map_err(|e| RpcError::Transport(e.to_string()))?;
        loop {
            let value = self.next_value().await?;
            if value["id"] == id {
                return match value.get("error") {
                    Some(error) => Err(RpcError::Remote(error.clone())),
                    None => Ok(value["result"].clone()),
                };
            }
            if value.get("method").is_some() {
                self.notifications.push_back(value);
            }
        }
    }

    async fn request(&mut self, method: &str, params: Value) -> Result<Value, RpcError> {
        timeout(REQUEST_TIMEOUT, self.call(method, params))
            .await
            .map_err(|_| RpcError::Transport(format!("{method} timed out")))?
    }

    async fn wait_for_compaction(&mut self, thread_id: &str) -> Result<(), String> {
        timeout(COMPACTION_TIMEOUT, async {
            let mut started: Option<(String, String)> = None;
            let mut item_completed = false;
            loop {
                let value = match self.notifications.pop_front() {
                    Some(value) => value,
                    None => self.next_value().await.map_err(format_rpc_error)?,
                };
                if value["params"]["threadId"] != thread_id {
                    continue;
                }
                let params = &value["params"];
                if value["method"] == "item/started"
                    && params["item"]["type"] == "contextCompaction"
                    && let (Some(turn), Some(item)) =
                        (params["turnId"].as_str(), params["item"]["id"].as_str())
                {
                    started = Some((turn.to_owned(), item.to_owned()));
                    item_completed = false;
                }
                if let Some((turn, item)) = &started {
                    if value["method"] == "item/completed"
                        && params["turnId"] == *turn
                        && params["item"]["id"] == *item
                        && params["item"]["type"] == "contextCompaction"
                    {
                        item_completed = true;
                    }
                    if value["method"] == "turn/completed" && params["turn"]["id"] == *turn {
                        return match params["turn"]["status"].as_str() {
                            Some("completed") if item_completed => Ok(()),
                            status => Err(format!(
                                "compaction turn ended without successful completion: {status:?}"
                            )),
                        };
                    }
                }
            }
        })
        .await
        .map_err(|_| "compaction completion timed out".to_owned())?
    }
}

fn format_rpc_error(error: RpcError) -> String {
    match error {
        RpcError::Transport(error) => error,
        RpcError::Remote(error) => error.to_string(),
    }
}

fn before_mutation(error: RpcError) -> Outcome {
    match error {
        RpcError::Transport(error) => Outcome::Retry(error),
        RpcError::Remote(error) if error["code"] == -32001 => Outcome::Retry(error.to_string()),
        RpcError::Remote(error) => Outcome::Discard(error.to_string()),
    }
}

pub async fn prompt_age(
    path: &Path,
    event: &Event,
    window: u64,
    activity: Option<&Activity>,
) -> Result<PromptAge, String> {
    let mut client = Client::connect(path).await?;
    let id = event.thread_id.to_string();
    let result = client
        .request("thread/read", json!({"threadId":id}))
        .await
        .map_err(format_rpc_error)?;
    let thread = &result["thread"];
    if thread["id"] != id
        || thread["sessionId"] != event.session_id.to_string()
        || thread["parentThreadId"].is_string()
        || event
            .transcript_path
            .as_ref()
            .is_some_and(|path| thread["path"] != *path)
    {
        return Err("prompt thread identity is unavailable".into());
    }
    let legacy = thread["historyMode"].as_str().unwrap_or("legacy") == "legacy";
    let mut cursor: Option<String> = None;
    loop {
        let result = client.request("thread/turns/list", json!({
            "threadId":id, "limit":25, "sortDirection":"desc", "itemsView":"notLoaded", "cursor":cursor
        })).await.map_err(format_rpc_error)?;
        let turns = result["data"].as_array().ok_or("invalid turn list")?;
        for turn in turns {
            let current = thread["status"]["type"] == "active"
                && turn["status"] == "inProgress"
                && turn["id"]
                    .as_str()
                    .is_some_and(|id| event.turn_id.as_deref() == Some(id));
            if !current
                && (!matches!(turn["status"].as_str(), Some("completed" | "failed"))
                    || event.turn_id.as_deref() == turn["id"].as_str())
            {
                continue;
            }
            let completed_at = turn["completedAt"].as_u64();
            let tracked = activity.filter(|activity| {
                turn["id"].as_str().is_some_and(|id| {
                    id == activity.turn_id
                        || activity
                            .alternate_turn_ids
                            .iter()
                            .any(|candidate| candidate == id)
                }) && activity.observed_at <= crate::clock::now()
            });
            let fresh = match tracked {
                Some(activity) => crate::clock::now() < activity.observed_at.saturating_add(window),
                None => completed_at
                    .is_some_and(|at| crate::clock::wall_now() < at.saturating_add(window)),
            };
            if !current && fresh {
                return Ok(PromptAge::Fresh);
            }
            let turn_id = turn["id"].as_str().ok_or("completed turn has no id")?;
            let item_type = if current {
                "userMessage"
            } else {
                "contextCompaction"
            };
            if legacy {
                let full = client.request("thread/turns/list", json!({
                    "threadId":id, "limit":25, "sortDirection":"desc", "itemsView":"full", "cursor":cursor
                })).await.map_err(format_rpc_error)?;
                let matching = full["data"]
                    .as_array()
                    .and_then(|turns| turns.iter().find(|candidate| candidate["id"] == turn_id))
                    .ok_or("completed turn is unavailable")?;
                let items = matching["items"]
                    .as_array()
                    .ok_or("turn items are unavailable")?;
                if items.iter().any(|item| item["type"] == item_type) {
                    return Ok(PromptAge::Fresh);
                }
            } else {
                let mut item_cursor: Option<String> = None;
                loop {
                    let items = client
                        .request(
                            "thread/items/list",
                            json!({
                                "threadId":id, "turnId":turn_id, "limit":50,
                                "sortDirection":if current { "asc" } else { "desc" }, "cursor":item_cursor
                            }),
                        )
                        .await
                        .map_err(format_rpc_error)?;
                    let entries = items["data"].as_array().ok_or("invalid item list")?;
                    if entries.iter().any(|entry| {
                        entry["turnId"] == turn_id && entry["item"]["type"] == item_type
                    }) {
                        return Ok(PromptAge::Fresh);
                    }
                    match items["nextCursor"].as_str() {
                        Some(next) => item_cursor = Some(next.to_owned()),
                        None => break,
                    }
                }
            }
            if current {
                continue;
            }
            return Ok(if tracked.is_some() || completed_at.is_some() {
                PromptAge::Stale
            } else {
                PromptAge::Unknown
            });
        }
        match result["nextCursor"].as_str() {
            Some(next) => cursor = Some(next.to_owned()),
            None => return Ok(PromptAge::Fresh),
        }
    }
}

async fn authorize(job: &Job, events: &mpsc::UnboundedSender<WorkerEvent>) -> bool {
    let (send, receive) = oneshot::channel();
    events
        .send(WorkerEvent::Dispatch(job.thread_id, job.generation, send))
        .is_ok()
        && receive.await.unwrap_or(false)
}

async fn still_current(client: &mut Client, job: &Job) -> Result<(), Outcome> {
    let id = job.thread_id.to_string();
    let read = client
        .request("thread/read", json!({"threadId":id}))
        .await
        .map_err(before_mutation)?;
    if !matches!(
        read["thread"]["status"]["type"].as_str(),
        Some("idle" | "systemError")
    ) {
        return Err(Outcome::Retry("thread became active".into()));
    }
    let turns = client
        .request(
            "thread/turns/list",
            json!({
                "threadId":id, "limit":1, "sortDirection":"desc", "itemsView":"notLoaded"
            }),
        )
        .await
        .map_err(before_mutation)?;
    let current = turns["data"].as_array().and_then(|turns| turns.first());
    if !current.is_some_and(|turn| matches_scheduled_turn(job, turn)) {
        return Err(Outcome::Discard(
            "scheduled turn is no longer the latest eligible turn".into(),
        ));
    }
    Ok(())
}

pub async fn process(
    path: &Path,
    job: &Job,
    events: &mpsc::UnboundedSender<WorkerEvent>,
) -> Outcome {
    let mut client = match Client::connect(path).await {
        Ok(client) => client,
        Err(error) => return Outcome::Retry(error),
    };
    let thread_id = job.thread_id.to_string();
    let read = match client
        .request("thread/read", json!({"threadId":thread_id}))
        .await
    {
        Ok(value) => value,
        Err(error) => return before_mutation(error),
    };
    let thread = &read["thread"];
    if thread["id"] != thread_id || thread["sessionId"] != job.session_id.to_string() {
        return Outcome::Discard("thread identity changed".into());
    }
    if let Some(path) = &job.transcript_path
        && thread["path"] != *path
    {
        return Outcome::Discard("thread transcript path changed".into());
    }
    if job.subagent != thread["parentThreadId"].is_string() {
        return Outcome::Discard("thread kind changed".into());
    }
    match thread["status"]["type"].as_str() {
        Some("idle" | "systemError") => {}
        Some("active") => return Outcome::Retry("thread is active".into()),
        other => {
            return Outcome::Discard(format!(
                "thread is not in a loaded, inactive state: {other:?}"
            ));
        }
    }
    let turns = match client
        .request(
            "thread/turns/list",
            json!({
                "threadId":thread_id,"limit":1,"sortDirection":"desc","itemsView":"notLoaded"
            }),
        )
        .await
    {
        Ok(value) => value,
        Err(error) => return before_mutation(error),
    };
    let Some(turn) = turns["data"].as_array().and_then(|turns| turns.first()) else {
        return Outcome::Retry("latest turn is not available yet".into());
    };
    if !matches_scheduled_turn(job, turn) {
        return Outcome::Discard("scheduled turn is no longer the latest eligible turn".into());
    }

    if job.subagent && thread["canAcceptDirectInput"] == false {
        let mut cursor: Option<String> = None;
        loop {
            let descendants = match client.request("thread/list", json!({
                "ancestorThreadId": thread_id,
                "sourceKinds": ["subAgent", "subAgentReview", "subAgentCompact", "subAgentThreadSpawn", "subAgentOther"],
                "limit": 100,
                "cursor": cursor
            })).await {
                Ok(value) => value,
                Err(error) => return before_mutation(error),
            };
            let Some(threads) = descendants["data"].as_array() else {
                return Outcome::Retry("descendant status is unavailable".into());
            };
            if threads.iter().any(|child| {
                !matches!(
                    child["status"]["type"].as_str(),
                    Some("idle" | "systemError" | "notLoaded")
                )
            }) {
                return Outcome::Retry(
                    "a descendant is active or its status is unavailable".into(),
                );
            }
            match descendants["nextCursor"].as_str() {
                Some(next) => cursor = Some(next.to_owned()),
                None => break,
            }
        }
        if let Err(outcome) = still_current(&mut client, job).await {
            return outcome;
        }
        if let Err(outcome) = plugin_enabled(&mut client, job).await {
            return outcome;
        }
        if !authorize(job, events).await {
            return Outcome::Discard("schedule cancelled".into());
        }
        return match client
            .request("thread/delete", json!({"threadId":thread_id}))
            .await
        {
            Ok(_) => Outcome::Done("deleted subagent"),
            Err(RpcError::Remote(error)) if error["code"] == -32001 => {
                Outcome::Retry(error.to_string())
            }
            Err(RpcError::Remote(error)) => Outcome::Discard(error.to_string()),
            Err(RpcError::Transport(error)) => {
                Outcome::Unknown(format!("delete outcome unknown: {error}"))
            }
        };
    }
    if job.subagent && thread["canAcceptDirectInput"].as_bool().is_none() {
        return Outcome::Discard("subagent direct-input capability is unavailable".into());
    }
    if crate::scheduler::now() >= job.expires_at {
        return Outcome::Discard("cache window expired; skipping compaction".into());
    }
    if let Err(outcome) = plugin_enabled(&mut client, job).await {
        return outcome;
    }

    if let Err(error) = client
        .request(
            "thread/resume",
            json!({"threadId":thread_id,"excludeTurns":true}),
        )
        .await
    {
        return before_mutation(error);
    }
    if let Err(outcome) = still_current(&mut client, job).await {
        return outcome;
    }
    if let Err(outcome) = plugin_enabled(&mut client, job).await {
        return outcome;
    }
    let deadline = match crate::clock::Timer::new() {
        Ok(timer) => timer,
        Err(error) => return Outcome::Discard(format!("cannot enforce cache deadline: {error}")),
    };
    if let Err(error) = deadline.set(Some(job.expires_at)) {
        return Outcome::Discard(format!("cannot enforce cache deadline: {error}"));
    }
    if !authorize(job, events).await {
        return Outcome::Discard("schedule cancelled".into());
    }
    if crate::scheduler::now() >= job.expires_at {
        return Outcome::Discard("cache window expired; skipping compaction".into());
    }
    client.notifications.clear();
    let result = tokio::select! {
        biased;
        result = deadline.wait() => {
            return Outcome::Unknown(match result {
                Ok(()) => "cache deadline reached while starting compaction".into(),
                Err(error) => format!("cache deadline check failed: {error}"),
            });
        }
        result = client.request("thread/compact/start", json!({"threadId":thread_id})) => result,
    };
    match result {
        Ok(_) => {}
        Err(RpcError::Remote(error)) if error["code"] == -32001 => {
            return Outcome::Retry(error.to_string());
        }
        Err(RpcError::Remote(error)) => return Outcome::Discard(error.to_string()),
        Err(RpcError::Transport(error)) => {
            return Outcome::Unknown(format!("compaction outcome unknown: {error}"));
        }
    }
    match client.wait_for_compaction(&thread_id).await {
        Ok(()) => Outcome::Done("compacted"),
        Err(error) => Outcome::Unknown(error),
    }
}
