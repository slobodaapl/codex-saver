#!/bin/sh
set -eu

PROJECT=$(CDPATH= cd "$(dirname "$0")/.." && pwd)
HOOK=${CODEX_SAVER_HOOK_OVERRIDE:-"$PROJECT/plugin/hook.sh"}
PLUGIN_ROOT="$PROJECT/plugin"
mkdir -p "$PROJECT/target"
TEST_ROOT=$(mktemp -d "$PROJECT/target/bootstrap-contract.XXXXXX")
FIXTURES="$TEST_ROOT/fixtures"
MOCK_BIN="$TEST_ROOT/mock-bin"
FIXTURE_HELPER="$FIXTURES/helper"
ACTIVE_PIDS=

cleanup() {
	if [ -n "${TEST_ROOT-}" ] && [ -d "$TEST_ROOT" ]; then
		find "$TEST_ROOT" -type f -name '*.release' -exec touch {} \; 2>/dev/null || :
		for pid in $ACTIVE_PIDS; do
			wait "$pid" 2>/dev/null || :
		done
		rm -rf "$TEST_ROOT"
	fi
}
trap cleanup EXIT HUP INT TERM

fail() {
	printf 'FAIL: %s\n' "$*" >&2
	exit 1
}

assert_eq() {
	label=$1
	expected=$2
	actual=$3
	[ "$expected" = "$actual" ] || fail "$label: expected <$expected>, got <$actual>"
}

assert_count() {
	label=$1
	line=$2
	expected=$3
	actual=$(awk -v wanted="$line" '$0 == wanted { n++ } END { print n + 0 }' "$MOCK_RESULTS")
	assert_eq "$label" "$expected" "$actual"
}

wait_for_file() {
	path=$1
	limit=${2:-15}
	i=0
	while [ "$i" -lt "$limit" ]; do
		[ -e "$path" ] && return 0
		sleep 1
		i=$((i + 1))
	done
	return 1
}

wait_for_result() {
	line=$1
	limit=${2:-15}
	i=0
	while [ "$i" -lt "$limit" ]; do
		if awk -v wanted="$line" '$0 == wanted { found = 1 } END { exit !found }' "$MOCK_RESULTS"; then
			return 0
		fi
		sleep 1
		i=$((i + 1))
	done
	return 1
}

wait_for_log() {
	fragment=$1
	limit=${2:-15}
	i=0
	while [ "$i" -lt "$limit" ]; do
		if grep -F "$fragment" "$MOCK_LOG" >/dev/null 2>&1; then
			return 0
		fi
		sleep 1
		i=$((i + 1))
	done
	return 1
}

has_live_mock_run() {
	run_id=$1
	for proc_env in /proc/[0-9]*/environ; do
		[ -r "$proc_env" ] || continue
		if tr '\000' '\n' 2>/dev/null < "$proc_env" | grep -F -x "MOCK_RUN_ID=$run_id" >/dev/null 2>&1; then
			return 0
		fi
	done
	return 1
}

wait_for_run_quiescence() {
	run_id=$1
	limit=${2:-20}
	i=0
	while has_live_mock_run "$run_id"; do
		[ "$i" -lt "$limit" ] || return 1
		sleep 1
		i=$((i + 1))
	done
	return 0
}

count_log() {
	awk -v wanted="$1" '$0 == wanted { n++ } END { print n + 0 }' "$MOCK_LOG"
}

assert_clean_network_audit() {
	if awk '$0 != "args=0 body=0" { bad = 1 } END { exit !bad }' "$MOCK_AUDIT"; then
		fail "prompt or Stop data marker appeared in network arguments/body audit: $(cat "$MOCK_AUDIT")"
	fi
}

assert_no_payload_on_disk() {
	marker=$1
	for path in "$DATA_DIR" "$CASE_ROOT/tmp" "$CAPTURE_DIR" "$MOCK_LOG" "$MOCK_AUDIT" "$MOCK_RESULTS" "$CASE_ROOT/hook.json"; do
		if grep -R -F "$marker" "$path" >/dev/null 2>&1; then
			fail "payload marker persisted in test runtime data: $marker"
		fi
	done
}

has_installed_fixture() {
	find "$DATA_DIR" -type f -perm -100 -exec cmp -s {} "$FIXTURE_HELPER" \; -print |
		sed -n '1p' | grep -q .
}

assert_installed_fixture() {
	if ! has_installed_fixture; then
		fail "no executable matching the downloaded helper in $DATA_DIR"
	fi
}

assert_no_executable() {
	files=$(find "$DATA_DIR" -type f -perm -100 -print)
	[ -z "$files" ] || fail "failed bootstrap left executable(s): $files"
}

assert_json_warning() {
	python3 - "$1" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as stream:
    value = json.load(stream)
message = value.get("systemMessage") if isinstance(value, dict) else None
assert isinstance(message, str) and message.strip(), "missing non-empty systemMessage"
words = ("warning", "download", "install", "bootstrap", "setup", "initializ", "unavailable")
assert any(word in message.lower() for word in words), "systemMessage does not describe a bootstrap warning"
PY
}

assert_json_update_block() {
	python3 - "$1" <<'PY'
import json
import re
import sys

with open(sys.argv[1], encoding="utf-8") as stream:
    value = json.load(stream)
assert isinstance(value, dict), "block response root is not an object"
assert set(value) == {"decision", "reason", "systemMessage"}, "block response has unexpected keys"
assert value["decision"] == "block", "replacement response did not block the prompt"
assert isinstance(value["reason"], str) and value["reason"].strip(), "missing non-empty block reason"
message = value["systemMessage"]
assert isinstance(message, str) and message.strip(), "missing non-empty systemMessage"
normalized = re.sub(r"[^a-z0-9]+", " ", message.lower())
assert "updated" in normalized, "systemMessage does not say the helper was updated"
assert "codex" in normalized and "thread" in normalized and "reopen" in normalized, "systemMessage omits reopening the Codex thread"
assert "trust" in normalized and "hook" in normalized, "systemMessage omits re-trusting the hook"
assert "resend" in normalized and "message" in normalized, "systemMessage omits resending the message"
PY
}

assert_hook_output() {
	output=$1
	id=$2
	prefix=${3:-helper-output}
	python3 - "$output" "$id" "$prefix" <<'PY'
import sys

with open(sys.argv[1], "rb") as stream:
    actual = stream.read()
expected = f"{sys.argv[3]}:{sys.argv[2]}\n".encode()
assert actual == expected, f"helper stdout changed: expected {expected!r}, got {actual!r}"
PY
}

assert_capture() {
	id=$1
	body=$2
	wait_for_file "$CAPTURE_DIR/$id.sha256" 10 || fail "helper did not capture input for $id"
	expected=$(printf '%s' "$body" | sha256sum | awk '{ print $1 }')
	actual=$(cat "$CAPTURE_DIR/$id.sha256")
	assert_eq "$id input bytes" "$expected" "$actual"
	arg=$(sed -n '1p' "$CAPTURE_DIR/$id.args")
	assert_eq "$id helper command" hook "$arg"
}

assert_no_capture() {
	[ ! -e "$CAPTURE_DIR/$1.sha256" ] || fail "helper received blocked prompt $1"
}

wait_hook_pending() {
	status=$1
	id=$2
	if wait_for_file "$status" 2; then
		fail "hook returned before its held update outcome for $id"
	fi
	assert_no_capture "$id"
}

setup_case() {
	CASE_ROOT="$TEST_ROOT/$1"
	DATA_DIR="$CASE_ROOT/data"
	CAPTURE_DIR="$CASE_ROOT/captures"
	MOCK_LOG="$CASE_ROOT/network.log"
	MOCK_RESULTS="$CASE_ROOT/network.results"
	MOCK_AUDIT="$CASE_ROOT/network.audit"
	MOCK_COUNTER="$CASE_ROOT/asset.counter"
	MOCK_COUNTER_LOCK="$CASE_ROOT/asset.counter.lock"
	MOCK_DOWNLOAD_RELEASE="$CASE_ROOT/download.release"
	MOCK_LATEST_RELEASE="$CASE_ROOT/latest.release"
	mkdir -p "$DATA_DIR" "$CAPTURE_DIR" "$CASE_ROOT/tmp" "$CASE_ROOT/home"
	: > "$MOCK_LOG"
	: > "$MOCK_RESULTS"
	: > "$MOCK_AUDIT"
	: > "$MOCK_COUNTER"
	MOCK_GATE_DOWNLOAD=0
	MOCK_GATE_LATEST=0
	MOCK_FAIL_ASSET=0
	MOCK_BAD_SUMS=0
	MOCK_CONCURRENT=0
	MOCK_FAIL_LATEST=0
	MOCK_NOW=
}

seed_installed_helper() {
	cp -R "$TEST_ROOT/prompt-bootstrap/data/." "$DATA_DIR/"
	cp -R "$TEST_ROOT/prompt-bootstrap/home/." "$CASE_ROOT/home/"
}

assert_version_tag() {
	assert_eq "installed helper version" "$1" "$(cat "$DATA_DIR/version")"
}

assert_helper_file() {
	if ! find "$DATA_DIR" -type f -perm -100 -exec cmp -s {} "$1" \; -print | grep -q .; then
		fail "no installed executable matching $1"
	fi
}

start_hook() {
	event=$1
	body=$2
	output=$3
	status=$4
	data=$5
	arch=$6
	capture_id=$7
	case_root=$8
	run_id="$case_root/$capture_id"
	(
		set +e
		cd "$case_root"
		printf '%s' "$body" |
			env \
				PATH="$MOCK_BIN:/usr/bin:/bin" \
				HOME="$case_root/home" \
				TMPDIR="$case_root/tmp" \
				PLUGIN_ROOT="$PLUGIN_ROOT" \
				PLUGIN_DATA="$data" \
				CAPTURE_DIR="$CAPTURE_DIR" \
				CAPTURE_ID="$capture_id" \
				MOCK_FIXTURES="$FIXTURES" \
				MOCK_ARCH="$arch" \
				MOCK_LOG="$MOCK_LOG" \
				MOCK_RESULTS="$MOCK_RESULTS" \
				MOCK_AUDIT="$MOCK_AUDIT" \
				MOCK_RUN_ID="$run_id" \
				MOCK_NOW="$MOCK_NOW" \
				MOCK_COUNTER="$MOCK_COUNTER" \
				MOCK_COUNTER_LOCK="$MOCK_COUNTER_LOCK" \
				MOCK_DOWNLOAD_RELEASE="$MOCK_DOWNLOAD_RELEASE" \
				MOCK_LATEST_RELEASE="$MOCK_LATEST_RELEASE" \
				MOCK_GATE_DOWNLOAD="$MOCK_GATE_DOWNLOAD" \
				MOCK_GATE_LATEST="$MOCK_GATE_LATEST" \
				MOCK_FAIL_ASSET="$MOCK_FAIL_ASSET" \
				MOCK_BAD_SUMS="$MOCK_BAD_SUMS" \
				MOCK_CONCURRENT="$MOCK_CONCURRENT" \
				MOCK_FAIL_LATEST="$MOCK_FAIL_LATEST" \
				sh "$HOOK" "$event" > "$output"
		rc=$?
		printf '%s\n' "$rc" > "$status"
	) &
	LAST_PID=$!
	LAST_RUN_ID=$run_id
	ACTIVE_PIDS="$ACTIVE_PIDS $LAST_PID"
}

wait_hook_return() {
	status=$1
	pid=$2
	if ! wait_for_file "$status" 4; then
		touch "$MOCK_DOWNLOAD_RELEASE" "$MOCK_LATEST_RELEASE"
		wait "$pid" 2>/dev/null || :
		fail "hook did not return while mocked network was held"
	fi
	wait "$pid" 2>/dev/null || :
	assert_eq "hook exit status" 0 "$(cat "$status")"
}

make_fixtures() {
	mkdir -p "$FIXTURES" "$MOCK_BIN"
	cat > "$FIXTURE_HELPER" <<'SH'
#!/bin/sh
set -eu
if [ "${1-}" = hook ]; then
	name=${CAPTURE_ID:?}
	dir=${CAPTURE_DIR:?}
	mkdir -p "$dir"
	printf '%s\n' "$@" > "$dir/$name.args"
	printf '%s\n' "${CODEX_SAVER_OBSERVED_AT-}" > "$dir/$name.observed"
	printf '%s\n' "${CODEX_SAVER_OBSERVED_ORDER-}" > "$dir/$name.order"
	digest=$(sha256sum | awk '{ print $1 }')
	printf '%s\n' "$digest" > "$dir/$name.sha256"
	printf 'helper-output:%s\n' "$name"
fi
SH
	chmod +x "$FIXTURE_HELPER"
	X86_NAME=codex-saver-x86_64-unknown-linux-musl
	ARM_NAME=codex-saver-aarch64-unknown-linux-musl
	X86_SUM=$(sha256sum "$FIXTURE_HELPER" | awk '{ print $1 }')
	ARM_SUM=$X86_SUM
	cat > "$FIXTURES/latest.json" <<'JSON'
{"tag_name":"v1.2.3","assets":[{"id":101,"name":"codex-saver-x86_64-unknown-linux-musl","url":"https://api.github.com/repos/fixture/codex-saver/releases/assets/101","browser_download_url":"https://downloads.invalid/v1.2.3/codex-saver-x86_64-unknown-linux-musl"},{"id":102,"name":"codex-saver-aarch64-unknown-linux-musl","url":"https://api.github.com/repos/fixture/codex-saver/releases/assets/102","browser_download_url":"https://downloads.invalid/v1.2.3/codex-saver-aarch64-unknown-linux-musl"},{"id":103,"name":"SHA256SUMS","url":"https://api.github.com/repos/fixture/codex-saver/releases/assets/103","browser_download_url":"https://downloads.invalid/v1.2.3/SHA256SUMS"}]}
JSON
	{
		printf '%s  %s\n' "$X86_SUM" "$X86_NAME"
		printf '%s  %s\n' "$ARM_SUM" "$ARM_NAME"
	} > "$FIXTURES/SHA256SUMS"
	{
		printf '%064d  %s\n' 0 "$X86_NAME"
		printf '%064d  %s\n' 0 "$ARM_NAME"
	} > "$FIXTURES/SHA256SUMS.bad"
}

make_mocks() {
	cat > "$MOCK_BIN/curl" <<'SH'
#!/bin/sh
set -eu
request_args=$*
request_stdin=$(cat)
url=
out=
writeout=
body_from_stdin=0
body_file=
inspect_body_arg() {
	body_arg=$1
	case "$body_arg" in
		*@-) body_from_stdin=1 ;;
		*@*) body_file=${body_arg##*@}; body_file=${body_file%%;*} ;;
	esac
}
while [ "$#" -gt 0 ]; do
	case "$1" in
		-o|--output|--output-document|--url)
			option=$1
			value=$2
			shift 2
			case "$option" in
				--url) url=$value ;;
				*) out=$value ;;
			esac
			;;
		-o?*) out=${1#-o}; shift ;;
		--output=*|--output-document=*) out=${1#*=}; shift ;;
		--url=*) url=${1#*=}; shift ;;
		-w|--write-out) writeout=$2; shift 2 ;;
		--write-out=*) writeout=${1#*=}; shift ;;
		-d|--data|--data-binary|--data-urlencode|--data-raw|--json|-F|--form|--form-string)
			inspect_body_arg "$2"
			shift 2
			;;
		--data=*|--data-binary=*|--data-urlencode=*|--data-raw=*|--json=*|--form=*|--form-string=*)
			inspect_body_arg "${1#*=}"
			shift
			;;
		-T|--upload-file)
			body_file=$2
			[ "$body_file" = - ] && body_from_stdin=1
			shift 2
			;;
		--upload-file=*) body_file=${1#*=}; [ "$body_file" = - ] && body_from_stdin=1; shift ;;
		-H|--header|-A|--user-agent|--max-time|--connect-timeout|--retry|--retry-delay|-D|--dump-header|-u|--user|-X|--request)
			shift 2
			;;
		--silent|-s|--show-error|-S|--fail|-f|--location|-L|--compressed|-k|--insecure|--globoff|-g)
			shift
			;;
		--)
			shift
			[ "$#" -gt 0 ] && url=$1
			break
			;;
		-*) shift ;;
		*) url=$1; shift ;;
	esac
done
[ -n "$url" ] || { printf 'mock curl received no URL\n' >&2; exit 22; }
arg_marker=0
body_marker=0
case "$request_args" in
	*PROMPT_PRIVATE_PAYLOAD_MARKER*|*STOP_PRIVATE_PAYLOAD_MARKER*) arg_marker=1 ;;
esac
if [ "$body_from_stdin" = 1 ]; then
	case "$request_stdin" in
		*PROMPT_PRIVATE_PAYLOAD_MARKER*|*STOP_PRIVATE_PAYLOAD_MARKER*) body_marker=1 ;;
	esac
fi
if [ -n "$body_file" ] && [ -f "$body_file" ] &&
	grep -F -e PROMPT_PRIVATE_PAYLOAD_MARKER -e STOP_PRIVATE_PAYLOAD_MARKER "$body_file" >/dev/null 2>&1; then
	body_marker=1
fi
printf 'args=%s body=%s\n' "$arg_marker" "$body_marker" >> "$MOCK_AUDIT"
kind=unknown
source=
case "$url" in
	*/releases/assets/101|*codex-saver-x86_64-unknown-linux-musl*) kind=asset-x86; source="$MOCK_FIXTURES/helper" ;;
	*/releases/assets/102|*codex-saver-aarch64-unknown-linux-musl*) kind=asset-arm; source="$MOCK_FIXTURES/helper" ;;
	*/releases/assets/103|*SHA256SUMS*|*sha256sums*) kind=sums; source="$MOCK_FIXTURES/SHA256SUMS" ;;
	*releases/latest*|*releases*) kind=latest; source="$MOCK_FIXTURES/latest.json" ;;
esac
printf '%s\n' "$kind" >> "$MOCK_LOG"
if [ "$kind" = unknown ]; then
	printf 'unmocked network request\n' >&2
	printf 'unknown:failed\n' >> "$MOCK_RESULTS"
	exit 22
fi
record_result() { printf '%s\n' "$1" >> "$MOCK_RESULTS"; }
wait_for_gate() {
	gate=$1
	n=0
	while [ ! -e "$gate" ] && [ "$n" -lt 20 ]; do
		sleep 1
		n=$((n + 1))
	done
	[ -e "$gate" ]
}
next_asset_number() {
	n=0
	while ! mkdir "$MOCK_COUNTER_LOCK" 2>/dev/null; do
		sleep 1
		n=$((n + 1))
		[ "$n" -lt 10 ] || return 1
	done
	value=$(cat "$MOCK_COUNTER")
	value=$((value + 1))
	printf '%s\n' "$value" > "$MOCK_COUNTER"
	rmdir "$MOCK_COUNTER_LOCK"
	printf '%s\n' "$value"
}
write_body() {
	if [ -n "$out" ]; then cp "$source" "$out"; else cat "$source"; fi
}
if [ "$kind" = latest ] && [ "$MOCK_GATE_LATEST" = 1 ]; then
	if ! wait_for_gate "$MOCK_LATEST_RELEASE"; then
		record_result latest:failed
		exit 22
	fi
fi
if [ "$kind" = latest ] && [ "$MOCK_FAIL_LATEST" = 1 ]; then
	record_result latest:failed
	exit 22
fi
if [ "$kind" = asset-x86 ] || [ "$kind" = asset-arm ]; then
	if [ "$MOCK_CONCURRENT" = 1 ]; then
		asset_number=$(next_asset_number)
		if [ "$asset_number" -ge 2 ]; then
			n=0
			while ! find "$PLUGIN_DATA" -type f -perm -100 -exec cmp -s {} "$MOCK_FIXTURES/helper" \; -print | grep -q .; do
				sleep 1
				n=$((n + 1))
				[ "$n" -lt 20 ] || { record_result "$kind:failed"; exit 22; }
			done
			if [ -n "$out" ]; then printf '#!/bin/sh\nexit 99\n' > "$out"; else printf '#!/bin/sh\nexit 99\n'; fi
			record_result "$kind:failed"
			exit 22
		fi
	fi
	if [ "$MOCK_GATE_DOWNLOAD" = 1 ] && ! wait_for_gate "$MOCK_DOWNLOAD_RELEASE"; then
		record_result "$kind:failed"
		exit 22
	fi
	if [ "$MOCK_FAIL_ASSET" = 1 ]; then
		if [ -n "$out" ]; then printf '#!/bin/sh\nexit 98\n' > "$out"; else printf '#!/bin/sh\nexit 98\n'; fi
		record_result "$kind:failed"
		exit 22
	fi
fi
if [ "$kind" = sums ] && [ "$MOCK_BAD_SUMS" = 1 ]; then source="$MOCK_FIXTURES/SHA256SUMS.bad"; fi
write_body
record_result "$kind:ok"
case "$writeout" in
	*url_effective*)
		effective_url=$url
		case "$url" in
			*github.com/*/releases/latest*)
				latest_tag=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["tag_name"])' "$MOCK_FIXTURES/latest.json")
				effective_url=${url%/latest}/tag/$latest_tag
				;;
		esac
		printf '%s\n' "$effective_url"
		;;
esac
SH
	chmod +x "$MOCK_BIN/curl"
	cat > "$MOCK_BIN/wget" <<'SH'
#!/bin/sh
set -eu
request_args=$*
request_stdin=$(cat)
url=
out=
body_from_stdin=0
body_file=
arg_marker=0
body_marker=0
case "$request_args" in
	*PROMPT_PRIVATE_PAYLOAD_MARKER*|*STOP_PRIVATE_PAYLOAD_MARKER*) arg_marker=1 ;;
esac
while [ "$#" -gt 0 ]; do
	case "$1" in
		-O|--output-document) out=$2; shift 2 ;;
		--post-data|--body-data)
			case "$2" in *@-) body_from_stdin=1 ;; esac
			shift 2
			;;
		--post-file|--body-file)
			body_file=$2
			[ "$body_file" = - ] && body_from_stdin=1
			shift 2
			;;
		--post-file=*|--body-file=*)
			body_file=${1#*=}
			[ "$body_file" = - ] && body_from_stdin=1
			shift
			;;
		--post-data=*|--body-data=*) shift ;;
		-q|--quiet|-nv|--no-verbose|--no-check-certificate) shift ;;
		--output-document=*) out=${1#*=}; shift ;;
		-H|--header) shift 2 ;;
		--*) shift ;;
		-*) shift ;;
		*) url=$1; shift ;;
	esac
done
if [ "$body_from_stdin" = 1 ]; then
	case "$request_stdin" in *PROMPT_PRIVATE_PAYLOAD_MARKER*|*STOP_PRIVATE_PAYLOAD_MARKER*) body_marker=1 ;; esac
fi
if [ -n "$body_file" ] && [ -f "$body_file" ] &&
	grep -F -e PROMPT_PRIVATE_PAYLOAD_MARKER -e STOP_PRIVATE_PAYLOAD_MARKER "$body_file" >/dev/null 2>&1; then
	body_marker=1
fi
printf 'args=%s body=%s\n' "$arg_marker" "$body_marker" >> "$MOCK_AUDIT"
[ -n "$url" ] || exit 22
if [ -n "$out" ]; then exec "$MOCK_BIN/curl" -o "$out" "$url"; fi
exec "$MOCK_BIN/curl" "$url"
SH
	cat > "$MOCK_BIN/uname" <<'SH'
#!/bin/sh
if [ "${1-}" = -m ]; then
	printf '%s\n' "$MOCK_ARCH"
elif [ "${1-}" = -s ]; then
	printf 'Linux\n'
else
	exec /usr/bin/uname "$@"
fi
SH
	cat > "$MOCK_BIN/date" <<'SH'
#!/bin/sh
case "$*" in
	*+%s*)
		if [ -n "${MOCK_NOW-}" ]; then
			printf '%s\n' "$MOCK_NOW"
			exit 0
		fi
		;;
esac
exec /usr/bin/date "$@"
SH
	chmod +x "$MOCK_BIN/wget" "$MOCK_BIN/uname" "$MOCK_BIN/date"
}


make_fixtures
make_mocks

# Missing helper: warning returns before the held artifact download; x86_64 asset installs.
setup_case prompt-bootstrap
MOCK_GATE_DOWNLOAD=1
MOCK_BOOTSTRAP_NOW=$(date +%s)
MOCK_NOW=$MOCK_BOOTSTRAP_NOW
prompt_body='{"session_id":"prompt-1","private":"PROMPT_PRIVATE_PAYLOAD_MARKER","cwd":"/workspace"}'
start_hook UserPromptSubmit "$prompt_body" "$CASE_ROOT/hook.json" "$CASE_ROOT/hook.status" "$DATA_DIR" x86_64 bootstrap "$CASE_ROOT"
prompt_pid=$LAST_PID
bootstrap_run_id=$LAST_RUN_ID
wait_hook_return "$CASE_ROOT/hook.status" "$prompt_pid"
assert_json_warning "$CASE_ROOT/hook.json" || fail "missing-helper response was not a JSON systemMessage warning"
wait_for_log asset-x86 || {
	cat "$MOCK_LOG" >&2
	cat "$MOCK_RESULTS" >&2
	fail "x86_64 asset was not requested"
}
assert_count "latest release resolved" latest:ok 1
assert_no_executable
touch "$MOCK_DOWNLOAD_RELEASE"
wait_for_result asset-x86:ok || fail "x86_64 artifact download did not finish"
wait_for_result sums:ok || fail "SHA256SUMS download did not finish"
wait_for_run_quiescence "$bootstrap_run_id" || fail "bootstrap worker did not finish after successful download"
attempt=0
while [ "$attempt" -lt 10 ] && ! has_installed_fixture; do
	sleep 1
	attempt=$((attempt + 1))
done
assert_installed_fixture
assert_count "x86_64 asset selected" asset-x86:ok 1
assert_count "aarch64 asset excluded" asset-arm:ok 0
assert_clean_network_audit
assert_no_payload_on_disk PROMPT_PRIVATE_PAYLOAD_MARKER

# Stop during first bootstrap: forward exact bytes after installation and retain boot-clock observation.
setup_case stop-bootstrap
MOCK_GATE_DOWNLOAD=1
stop_body='{"session_id":"stop-1","private":"STOP_PRIVATE_PAYLOAD_MARKER","cwd":"/workspace"}'
stop_before=$(awk '{ print $1 }' /proc/uptime)
start_hook Stop "$stop_body" "$CASE_ROOT/hook.json" "$CASE_ROOT/hook.status" "$DATA_DIR" aarch64 stop "$CASE_ROOT"
stop_pid=$LAST_PID
stop_run_id=$LAST_RUN_ID
wait_hook_return "$CASE_ROOT/hook.status" "$stop_pid"
wait_for_log asset-arm || fail "aarch64 asset was not requested"
sleep 2
stop_release=$(awk '{ print $1 }' /proc/uptime)
touch "$MOCK_DOWNLOAD_RELEASE"
wait_for_result asset-arm:ok || fail "aarch64 artifact download did not finish"
wait_for_result sums:ok || fail "Stop SHA256SUMS download did not finish"
wait_for_run_quiescence "$stop_run_id" || fail "Stop bootstrap worker did not finish"
assert_capture stop "$stop_body"
assert_installed_fixture
stop_observed=$(cat "$CAPTURE_DIR/stop.observed")
stop_order=$(cat "$CAPTURE_DIR/stop.order")
[ -n "$stop_observed" ] || fail "Stop helper invocation lacked CODEX_SAVER_OBSERVED_AT"
[ -n "$stop_order" ] || fail "Stop helper invocation lacked CODEX_SAVER_OBSERVED_ORDER"
awk -v at="$stop_observed" -v order="$stop_order" -v before="$stop_before" -v released="$stop_release" 'BEGIN {
	order_seconds = order / 100
	exit !(at + 0 >= before - 1 && at + 0 < released && order + 0 >= (before - 1) * 100 &&
		order + 0 < released * 100 && order_seconds >= at && order_seconds < at + 1)
}' || fail "Stop observation mismatch: at=$stop_observed order=$stop_order before=$stop_before release=$stop_release"
assert_clean_network_audit
assert_no_payload_on_disk STOP_PRIVATE_PAYLOAD_MARKER

setup_case post-model-request-bootstrap
MOCK_GATE_DOWNLOAD=1
post_model_request_before=$(awk '{ print $1 }' /proc/uptime)
post_model_request_body='{"hook_event_name":"PostModelRequest","session_id":"failed-request-1","turn_id":"request-turn-1","transcript_path":"/workspace/session.jsonl","status":"failed","private":"STOP_PRIVATE_PAYLOAD_MARKER"}'
start_hook PostModelRequest "$post_model_request_body" "$CASE_ROOT/hook.json" "$CASE_ROOT/hook.status" "$DATA_DIR" x86_64 post-model-request "$CASE_ROOT"
post_model_request_pid=$LAST_PID
post_model_request_run_id=$LAST_RUN_ID
wait_hook_return "$CASE_ROOT/hook.status" "$post_model_request_pid"
wait_for_log asset-x86 || fail "PostModelRequest bootstrap did not request x86_64 asset"
sleep 4
post_model_request_release=$(awk '{ print $1 }' /proc/uptime)
touch "$MOCK_DOWNLOAD_RELEASE"
wait_for_result asset-x86:ok || fail "PostModelRequest artifact download did not finish"
wait_for_result sums:ok || fail "PostModelRequest SHA256SUMS download did not finish"
wait_for_run_quiescence "$post_model_request_run_id" || fail "PostModelRequest bootstrap worker did not finish"
assert_capture post-model-request "$post_model_request_body"
assert_installed_fixture
post_model_request_observed=$(cat "$CAPTURE_DIR/post-model-request.observed")
post_model_request_order=$(cat "$CAPTURE_DIR/post-model-request.order")
[ -n "$post_model_request_observed" ] || fail "PostModelRequest helper invocation lacked CODEX_SAVER_OBSERVED_AT"
[ -n "$post_model_request_order" ] || fail "PostModelRequest helper invocation lacked CODEX_SAVER_OBSERVED_ORDER"
awk -v at="$post_model_request_observed" -v order="$post_model_request_order" -v before="$post_model_request_before" -v released="$post_model_request_release" 'BEGIN {
	order_seconds = order / 100
	exit !(at + 0 >= before - 1 && at + 0 < before + 2 && at + 0 < released &&
		order + 0 >= (before - 1) * 100 && order + 0 < (before + 2) * 100 &&
		order + 0 < released * 100 && order_seconds >= at && order_seconds < at + 1)
}' || fail "PostModelRequest observation mismatch: at=$post_model_request_observed order=$post_model_request_order before=$post_model_request_before release=$post_model_request_release"
assert_clean_network_audit
assert_no_payload_on_disk STOP_PRIVATE_PAYLOAD_MARKER

# Failed artifact retrieval and checksum mismatch must leave no executable helper.
setup_case failed-download
MOCK_FAIL_ASSET=1
start_hook UserPromptSubmit '{"session_id":"failed-download"}' "$CASE_ROOT/hook.json" "$CASE_ROOT/hook.status" "$DATA_DIR" x86_64 failed "$CASE_ROOT"
failed_pid=$LAST_PID
failed_run_id=$LAST_RUN_ID
wait_hook_return "$CASE_ROOT/hook.status" "$failed_pid"
wait_for_result asset-x86:failed || fail "mocked artifact failure was not reached"
wait_for_run_quiescence "$failed_run_id" || fail "failed-download worker did not finish"
assert_no_executable
assert_clean_network_audit

setup_case bad-checksum
MOCK_BAD_SUMS=1
start_hook UserPromptSubmit '{"session_id":"bad-checksum"}' "$CASE_ROOT/hook.json" "$CASE_ROOT/hook.status" "$DATA_DIR" aarch64 checksum "$CASE_ROOT"
checksum_pid=$LAST_PID
checksum_run_id=$LAST_RUN_ID
wait_hook_return "$CASE_ROOT/hook.status" "$checksum_pid"
wait_for_result asset-arm:ok || fail "artifact for checksum test was not downloaded"
wait_for_result sums:ok || fail "mismatched SHA256SUMS was not downloaded"
wait_for_run_quiescence "$checksum_run_id" || fail "checksum-failure worker did not finish"
assert_no_executable
assert_clean_network_audit

# Concurrent first-use bootstraps: second response fails after first install; final helper stays complete.
setup_case concurrent-bootstrap
MOCK_GATE_DOWNLOAD=1
MOCK_CONCURRENT=1
start_hook UserPromptSubmit '{"session_id":"concurrent-1"}' "$CASE_ROOT/first.json" "$CASE_ROOT/first.status" "$DATA_DIR" x86_64 first "$CASE_ROOT"
first_pid=$LAST_PID
first_run_id=$LAST_RUN_ID
wait_hook_return "$CASE_ROOT/first.status" "$first_pid"
wait_for_log asset-x86 || fail "first concurrent bootstrap did not request x86_64 asset"
start_hook UserPromptSubmit '{"session_id":"concurrent-2"}' "$CASE_ROOT/second.json" "$CASE_ROOT/second.status" "$DATA_DIR" x86_64 second "$CASE_ROOT"
second_pid=$LAST_PID
second_run_id=$LAST_RUN_ID
wait_hook_return "$CASE_ROOT/second.status" "$second_pid"
assert_no_executable
touch "$MOCK_DOWNLOAD_RELEASE"
attempt=0
while [ "$attempt" -lt 15 ] && ! has_installed_fixture; do
	sleep 1
	attempt=$((attempt + 1))
done
wait_for_result asset-x86:ok || fail "concurrent bootstrap did not complete a valid artifact download"
if [ "$(cat "$MOCK_COUNTER")" -ge 2 ]; then
	wait_for_result asset-x86:failed || fail "second concurrent artifact attempt did not finish"
fi
wait_for_run_quiescence "$first_run_id" || fail "first concurrent worker did not finish"
wait_for_run_quiescence "$second_run_id" || fail "second concurrent worker did not finish"
assert_installed_fixture
assert_installed_fixture
concurrent_body='{"session_id":"after-concurrent","private":"PROMPT_PRIVATE_PAYLOAD_MARKER"}'
start_hook UserPromptSubmit "$concurrent_body" "$CASE_ROOT/after-concurrent.json" "$CASE_ROOT/after-concurrent.status" "$DATA_DIR" x86_64 after-concurrent "$CASE_ROOT"
concurrent_hook_pid=$LAST_PID
concurrent_hook_run_id=$LAST_RUN_ID
wait_hook_return "$CASE_ROOT/after-concurrent.status" "$concurrent_hook_pid"
assert_capture after-concurrent "$concurrent_body"
wait_for_run_quiescence "$concurrent_hook_run_id" || fail "post-concurrency helper hook did not finish"
assert_clean_network_audit
assert_no_payload_on_disk PROMPT_PRIVATE_PAYLOAD_MARKER

# Due update with unchanged release waits for lookup, then forwards exact input/output.
setup_case due-unchanged
seed_installed_helper
MOCK_NOW=$((MOCK_BOOTSTRAP_NOW + 86401))
MOCK_GATE_LATEST=1
body_one='{"session_id":"repeat-1","private":"PROMPT_PRIVATE_PAYLOAD_MARKER","message":"first exact prompt"}'
start_hook UserPromptSubmit "$body_one" "$CASE_ROOT/repeat-1.json" "$CASE_ROOT/repeat-1.status" "$DATA_DIR" x86_64 repeat-1 "$CASE_ROOT"
repeat_pid=$LAST_PID
wait_for_log latest || fail "helper-present prompt did not start an update check after 24 hours"
wait_hook_pending "$CASE_ROOT/repeat-1.status" repeat-1
touch "$MOCK_LATEST_RELEASE"
wait_hook_return "$CASE_ROOT/repeat-1.status" "$repeat_pid"
assert_capture repeat-1 "$body_one"
assert_hook_output "$CASE_ROOT/repeat-1.json" repeat-1
assert_version_tag v1.2.3
repeat_index=2
while [ "$repeat_index" -le 4 ]; do
	body=$(printf '{"session_id":"repeat-%s","private":"PROMPT_PRIVATE_PAYLOAD_MARKER","message":"unchanged exact bytes"}' "$repeat_index")
	start_hook UserPromptSubmit "$body" "$CASE_ROOT/repeat-$repeat_index.json" "$CASE_ROOT/repeat-$repeat_index.status" "$DATA_DIR" x86_64 "repeat-$repeat_index" "$CASE_ROOT"
	repeat_pid=$LAST_PID
	wait_hook_return "$CASE_ROOT/repeat-$repeat_index.status" "$repeat_pid"
	assert_capture "repeat-$repeat_index" "$body"
	assert_hook_output "$CASE_ROOT/repeat-$repeat_index.json" "repeat-$repeat_index"
	repeat_index=$((repeat_index + 1))
done
assert_eq "release lookups within cooldown" 1 "$(count_log latest)"
assert_eq "unchanged latest tag binary downloads" 0 "$(count_log asset-x86)"
assert_eq "unchanged latest tag checksum downloads" 0 "$(count_log sums)"
assert_clean_network_audit
assert_no_payload_on_disk PROMPT_PRIVATE_PAYLOAD_MARKER

# Candidate v1.2.4 changes helper bytes and release metadata; failures preserve v1.2.3.
OLD_HELPER="$FIXTURES/helper.v1.2.3"
cp "$FIXTURE_HELPER" "$OLD_HELPER"
sed 's/helper-output:/updated-helper-output:/' "$FIXTURE_HELPER" > "$FIXTURES/helper.updated"
mv "$FIXTURES/helper.updated" "$FIXTURE_HELPER"
UPDATED_SUM=$(sha256sum "$FIXTURE_HELPER" | awk '{ print $1 }')
{
	printf '%s  %s\n' "$UPDATED_SUM" "$X86_NAME"
	printf '%s  %s\n' "$UPDATED_SUM" "$ARM_NAME"
} > "$FIXTURES/SHA256SUMS"
sed 's/v1.2.3/v1.2.4/g' "$FIXTURES/latest.json" > "$FIXTURES/latest.next"
mv "$FIXTURES/latest.next" "$FIXTURES/latest.json"

setup_case failed-update-lookup
seed_installed_helper
MOCK_NOW=$((MOCK_BOOTSTRAP_NOW + 2 * 86401))
MOCK_FAIL_LATEST=1
failed_lookup_body='{"session_id":"failed-lookup","private":"PROMPT_PRIVATE_PAYLOAD_MARKER"}'
start_hook UserPromptSubmit "$failed_lookup_body" "$CASE_ROOT/hook.json" "$CASE_ROOT/hook.status" "$DATA_DIR" x86_64 failed-lookup "$CASE_ROOT"
failed_pid=$LAST_PID
wait_hook_return "$CASE_ROOT/hook.status" "$failed_pid"
wait_for_result latest:failed || fail "mocked latest-release lookup failure was not reached"
assert_capture failed-lookup "$failed_lookup_body"
assert_hook_output "$CASE_ROOT/hook.json" failed-lookup
assert_helper_file "$OLD_HELPER"
assert_version_tag v1.2.3
assert_clean_network_audit
assert_no_payload_on_disk PROMPT_PRIVATE_PAYLOAD_MARKER

setup_case failed-update-download
seed_installed_helper
MOCK_NOW=$((MOCK_BOOTSTRAP_NOW + 3 * 86401))
MOCK_FAIL_ASSET=1
failed_download_body='{"session_id":"failed-download","private":"PROMPT_PRIVATE_PAYLOAD_MARKER"}'
start_hook UserPromptSubmit "$failed_download_body" "$CASE_ROOT/hook.json" "$CASE_ROOT/hook.status" "$DATA_DIR" x86_64 failed-download "$CASE_ROOT"
failed_pid=$LAST_PID
wait_hook_return "$CASE_ROOT/hook.status" "$failed_pid"
wait_for_result asset-x86:failed || {
	cat "$MOCK_LOG" >&2
	cat "$MOCK_RESULTS" >&2
	fail "mocked update artifact failure was not reached"
}
assert_capture failed-download "$failed_download_body"
assert_hook_output "$CASE_ROOT/hook.json" failed-download
assert_helper_file "$OLD_HELPER"
assert_version_tag v1.2.3
assert_clean_network_audit
assert_no_payload_on_disk PROMPT_PRIVATE_PAYLOAD_MARKER

setup_case failed-update-checksum
seed_installed_helper
MOCK_NOW=$((MOCK_BOOTSTRAP_NOW + 4 * 86401))
MOCK_BAD_SUMS=1
failed_checksum_body='{"session_id":"failed-checksum","private":"PROMPT_PRIVATE_PAYLOAD_MARKER"}'
start_hook UserPromptSubmit "$failed_checksum_body" "$CASE_ROOT/hook.json" "$CASE_ROOT/hook.status" "$DATA_DIR" x86_64 failed-checksum "$CASE_ROOT"
failed_pid=$LAST_PID
wait_hook_return "$CASE_ROOT/hook.status" "$failed_pid"
wait_for_result asset-x86:ok || fail "artifact for update checksum test was not downloaded"
wait_for_result sums:ok || fail "mismatched update SHA256SUMS was not downloaded"
assert_capture failed-checksum "$failed_checksum_body"
assert_hook_output "$CASE_ROOT/hook.json" failed-checksum
assert_helper_file "$OLD_HELPER"
assert_version_tag v1.2.3
assert_clean_network_audit
assert_no_payload_on_disk PROMPT_PRIVATE_PAYLOAD_MARKER

# Successful replacement blocks the triggering send; cooldown invocation forwards without a false update notice.
setup_case successful-update
seed_installed_helper
MOCK_NOW=$((MOCK_BOOTSTRAP_NOW + 5 * 86401))
MOCK_GATE_DOWNLOAD=1
update_body='{"session_id":"update-1","private":"PROMPT_PRIVATE_PAYLOAD_MARKER","message":"must not reach old helper"}'
start_hook UserPromptSubmit "$update_body" "$CASE_ROOT/update.json" "$CASE_ROOT/update.status" "$DATA_DIR" x86_64 update-1 "$CASE_ROOT"
update_pid=$LAST_PID
wait_for_log asset-x86 || fail "due update did not request the replacement artifact"
wait_hook_pending "$CASE_ROOT/update.status" update-1
touch "$MOCK_DOWNLOAD_RELEASE"
wait_for_result asset-x86:ok || fail "replacement artifact did not finish downloading"
wait_for_result sums:ok || fail "replacement SHA256SUMS did not finish downloading"
wait_hook_return "$CASE_ROOT/update.status" "$update_pid"
assert_json_update_block "$CASE_ROOT/update.json"
assert_no_capture update-1
assert_helper_file "$FIXTURE_HELPER"
assert_version_tag v1.2.4
assert_clean_network_audit
assert_no_payload_on_disk PROMPT_PRIVATE_PAYLOAD_MARKER

cooldown_body='{"session_id":"after-update","private":"PROMPT_PRIVATE_PAYLOAD_MARKER","message":"forward after completed update"}'
start_hook UserPromptSubmit "$cooldown_body" "$CASE_ROOT/after-update.json" "$CASE_ROOT/after-update.status" "$DATA_DIR" x86_64 after-update "$CASE_ROOT"
cooldown_pid=$LAST_PID
wait_hook_return "$CASE_ROOT/after-update.status" "$cooldown_pid"
assert_capture after-update "$cooldown_body"
assert_hook_output "$CASE_ROOT/after-update.json" after-update updated-helper-output
assert_eq "release lookups after completed update cooldown" 1 "$(count_log latest)"
assert_clean_network_audit
assert_no_payload_on_disk PROMPT_PRIVATE_PAYLOAD_MARKER

# A waiter must observe binary replacement even when version metadata cannot be persisted.
setup_case update-metadata-failure-waiter
seed_installed_helper
MOCK_NOW=$((MOCK_BOOTSTRAP_NOW + 6 * 86401))
MOCK_GATE_DOWNLOAD=1
sed 's/v1.2.4/v1.2.5/g' "$FIXTURES/latest.json" > "$FIXTURES/latest.next"
mv "$FIXTURES/latest.next" "$FIXTURES/latest.json"
update_first_body='{"session_id":"metadata-first","private":"PROMPT_PRIVATE_PAYLOAD_MARKER"}'
start_hook UserPromptSubmit "$update_first_body" "$CASE_ROOT/first.json" "$CASE_ROOT/first.status" "$DATA_DIR" x86_64 metadata-first "$CASE_ROOT"
first_pid=$LAST_PID
first_run_id=$LAST_RUN_ID
wait_for_log asset-x86 || fail "metadata-failure update did not reach artifact download"
wait_hook_pending "$CASE_ROOT/first.status" metadata-first
assert_version_tag v1.2.3
mv "$DATA_DIR/version" "$CASE_ROOT/version.v1.2.3"
mkdir "$DATA_DIR/version"
chmod 555 "$DATA_DIR/version"
if touch "$DATA_DIR/version/write-probe" 2>/dev/null; then
	rm -f "$DATA_DIR/version/write-probe"
	fail "test could not make the version metadata path unwritable"
fi

update_waiter_body='{"session_id":"metadata-waiter","private":"PROMPT_PRIVATE_PAYLOAD_MARKER"}'
start_hook UserPromptSubmit "$update_waiter_body" "$CASE_ROOT/waiter.json" "$CASE_ROOT/waiter.status" "$DATA_DIR" x86_64 metadata-waiter "$CASE_ROOT"
waiter_pid=$LAST_PID
waiter_run_id=$LAST_RUN_ID
attempt=0
while ! has_live_mock_run "$waiter_run_id"; do
	[ "$attempt" -lt 10 ] || fail "metadata waiter did not start while updater was held"
	sleep 1
	attempt=$((attempt + 1))
done
wait_hook_pending "$CASE_ROOT/waiter.status" metadata-waiter
touch "$MOCK_DOWNLOAD_RELEASE"
wait_for_result asset-x86:ok || fail "metadata-failure replacement artifact did not finish downloading"
wait_for_result sums:ok || fail "metadata-failure replacement SHA256SUMS did not finish downloading"
wait_hook_return "$CASE_ROOT/first.status" "$first_pid"
wait_hook_return "$CASE_ROOT/waiter.status" "$waiter_pid"
assert_json_update_block "$CASE_ROOT/first.json"
assert_json_update_block "$CASE_ROOT/waiter.json"
assert_no_capture metadata-first
assert_no_capture metadata-waiter
assert_helper_file "$FIXTURE_HELPER"
[ -d "$DATA_DIR/version" ] || fail "version metadata unexpectedly became readable after blocked write"
[ -z "$(find "$DATA_DIR/version" -type f -print)" ] || fail "version metadata was persisted despite the unwritable path"
assert_eq "release lookups during metadata-failure update" 1 "$(count_log latest)"
assert_clean_network_audit
assert_no_payload_on_disk PROMPT_PRIVATE_PAYLOAD_MARKER

printf 'PASS: bootstrap contract tests\n'

