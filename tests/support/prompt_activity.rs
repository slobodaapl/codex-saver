use super::*;

fn activity_plan(history_mode: &str, turn_data: Vec<Value>) -> RpcPlan {
    let mut thread = thread_view(ROOT, SESSION, ROOT_PATH, None, "active", true);
    thread["historyMode"] = json!(history_mode);
    RpcPlan::new(thread, turns(turn_data), NEW_TURN)
}

fn accepted_user_message(id: &str) -> Value {
    json!({
        "type": "userMessage",
        "id": id,
        "content": [{"type": "text", "text": "accepted earlier input"}],
    })
}

async fn run_prompt(plan: RpcPlan) -> (Output, Vec<Value>) {
    let harness = Harness::start(3600, 1).await;
    let listener = harness.listener();
    let mut mock = start_rpc_mock(listener, plan).await;
    let output = harness
        .hook_output(root_prompt_with("steering input"))
        .await;
    let calls = collect_all_calls(&mut mock, Duration::from_millis(50)).await;
    (output, calls)
}

fn assert_history_read(calls: &[Value], method: &str) {
    assert!(
        calls.iter().any(|call| call["method"] == method),
        "hook did not read {method} history: {calls:?}"
    );
}

fn assert_no_prompt_compaction(calls: &[Value]) {
    assert!(
        calls
            .iter()
            .all(|call| call["method"] != "thread/compact/start"),
        "prompt activity check started compaction: {calls:?}"
    );
}

fn assert_no_prompt_work(calls: &[Value]) {
    assert_no_prompt_dispatch(calls);
    assert_no_prompt_compaction(calls);
}

fn assert_prompt_activity_silent(output: Output) {
    assert!(
        output.stdout.is_empty(),
        "expected silent hook output; got: {}",
        String::from_utf8_lossy(&output.stdout)
    );
    assert_prompt_silent(output);
}

#[tokio::test]
async fn paginated_current_turn_message_allows_steering_after_old_context() {
    let old_completion = unix_now_seconds() - 3600;
    let mut plan = activity_plan(
        "paginated",
        vec![
            prompt_turn(NEW_TURN, "inProgress", None),
            prompt_turn(TURN, "completed", Some(old_completion)),
        ],
    );
    plan.items = json!({
        "data": [{
            "turnId": NEW_TURN,
            "item": accepted_user_message("accepted-current-turn"),
        }],
        "nextCursor": null,
    });

    let (output, calls) = run_prompt(plan).await;

    assert_prompt_activity_silent(output);
    assert_history_read(&calls, "thread/items/list");
    assert_no_prompt_work(&calls);
}

#[tokio::test]
async fn legacy_current_turn_message_allows_steering_with_unknown_old_age() {
    let mut current = prompt_turn(NEW_TURN, "inProgress", None);
    current["items"] = json!([accepted_user_message("accepted-current-turn")]);
    let plan = activity_plan(
        "legacy",
        vec![current, prompt_turn(TURN, "completed", None)],
    );

    let (output, calls) = run_prompt(plan).await;

    assert_prompt_activity_silent(output);
    assert!(
        calls.iter().any(|call| {
            call["method"] == "thread/turns/list" && call["params"]["itemsView"] == "full"
        }),
        "legacy history was not read with full turn items: {calls:?}"
    );
    assert_no_prompt_work(&calls);
}

#[tokio::test]
async fn paginated_old_turn_message_does_not_count_as_current_steering() {
    let old_completion = unix_now_seconds() - 1800;
    let mut plan = activity_plan(
        "paginated",
        vec![
            prompt_turn(NEW_TURN, "inProgress", None),
            prompt_turn(TURN, "completed", Some(old_completion)),
        ],
    );
    plan.items = json!({
        "data": [{
            "turnId": TURN,
            "item": accepted_user_message("accepted-old-turn"),
        }],
        "nextCursor": null,
    });

    let (output, calls) = run_prompt(plan).await;

    assert_prompt_block(output);
    assert_history_read(&calls, "thread/items/list");
    assert_no_prompt_work(&calls);
}

#[tokio::test]
async fn legacy_first_input_stays_blocked_when_old_age_is_unknown() {
    let mut old = prompt_turn(TURN, "completed", None);
    old["items"] = json!([accepted_user_message("accepted-old-turn")]);
    let plan = activity_plan(
        "legacy",
        vec![prompt_turn(NEW_TURN, "inProgress", None), old],
    );

    let (output, calls) = run_prompt(plan).await;

    assert_prompt_block(output);
    assert!(
        calls.iter().any(|call| {
            call["method"] == "thread/turns/list" && call["params"]["itemsView"] == "full"
        }),
        "legacy history was not read with full turn items: {calls:?}"
    );
    assert_no_prompt_work(&calls);
}

#[tokio::test]
async fn item_history_error_remains_fail_open() {
    let old_completion = unix_now_seconds() - 3600;
    let mut plan = activity_plan(
        "paginated",
        vec![
            prompt_turn(NEW_TURN, "inProgress", None),
            prompt_turn(TURN, "completed", Some(old_completion)),
        ],
    );
    plan.reject_item_list = true;

    let (output, calls) = run_prompt(plan).await;

    assert_prompt_activity_silent(output);
    assert_history_read(&calls, "thread/items/list");
    assert_no_prompt_work(&calls);
}
