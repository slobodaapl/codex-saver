#!/bin/sh
set -u

event=${1-}
data=${PLUGIN_DATA:?}
binary=$data/codex-saver
repo=https://github.com/slobodaapl/codex-saver
plugin_dir=${PLUGIN_ROOT%/}
plugin_dir=${plugin_dir%/*}
plugin_name=${plugin_dir##*/}
plugin_dir=${plugin_dir%/*}
export CODEX_SAVER_PLUGIN_ID="$plugin_name@${plugin_dir##*/}"
mkdir -p "$data" || exit 0

update() (
    exec 9>"$data/update.lock" || exit 1
    flock -x -w 170 9 || exit 1
    now=$(date +%s) || exit 1
    if [ -x "$binary" ] && [ -f "$data/checked" ]; then
        read -r checked < "$data/checked" || checked=0
        case $checked in *[!0-9]*|'') checked=0;; esac
        if [ "$now" -ge "$checked" ] && [ $((now - checked)) -lt 86400 ]; then exit 0; fi
    fi
    printf '%s\n' "$now" > "$data/checked"
    case $(uname -m) in
        x86_64) asset=codex-saver-x86_64-unknown-linux-musl ;;
        aarch64|arm64) asset=codex-saver-aarch64-unknown-linux-musl ;;
        *) exit 1 ;;
    esac
    url=$(curl -fsSL -o /dev/null -w '%{url_effective}' --connect-timeout 5 --max-time 20 "$repo/releases/latest") || exit 1
    tag=${url##*/}
    case $tag in ''|*[!a-zA-Z0-9._-]*|.|..) exit 1;; esac
    if [ -x "$binary" ] && [ -f "$data/version" ]; then
        read -r installed < "$data/version" || installed=
        [ "$installed" = "$tag" ] && exit 0
    fi
    tmp=$(mktemp -d "$data/.download.XXXXXXXX") || exit 1
    trap 'rm -rf "$tmp"' EXIT HUP INT TERM
    curl -fsSL --connect-timeout 5 --max-time 120 "$repo/releases/download/$tag/$asset" -o "$tmp/$asset" || exit 1
    curl -fsSL --connect-timeout 5 --max-time 20 "$repo/releases/download/$tag/SHA256SUMS" -o "$tmp/SHA256SUMS" || exit 1
    (cd "$tmp" && awk -v name="$asset" '$2 == name {print}' SHA256SUMS | sha256sum -c --status) || exit 1
    chmod 755 "$tmp/$asset" && mv -f "$tmp/$asset" "$binary" || exit 1
    printf '%s\n' updated
    printf '%s\n' "$tag" > "$tmp/version" && mv -f "$tmp/version" "$data/version"
)

if [ -x "$binary" ]; then
    if [ "$event" = UserPromptSubmit ]; then
        before=$(stat -c '%d:%i' "$binary" 2>/dev/null) || before=
        updated=$(update </dev/null 2>/dev/null) || :
        after=$(stat -c '%d:%i' "$binary" 2>/dev/null) || after=
        if [ "$updated" = updated ] || { [ -n "$before" ] && [ -n "$after" ] && [ "$before" != "$after" ]; }; then
            printf '%s\n' '{"decision":"block","reason":"codex-saver was updated. Reopen this Codex thread and re-trust the hook before resending your message.","systemMessage":"codex-saver was updated. Reopen this Codex thread and re-trust the hook before resending your message."}'
            exit 0
        fi
    fi
    exec "$binary" hook
fi

case $event in
    Stop|SubagentStop|PostModelRequest)
        payload=$(cat)
        read -r uptime _ < /proc/uptime || uptime=0
        observed=${uptime%%.*}
        order=${uptime%.*}${uptime#*.}
        (
            update && printf '%s' "$payload" | CODEX_SAVER_OBSERVED_AT="$observed" CODEX_SAVER_OBSERVED_ORDER="$order" "$binary" hook
        ) </dev/null >/dev/null 2>&1 &
        ;;
    UserPromptSubmit)
        (update) </dev/null >/dev/null 2>&1 &
        printf '%s\n' '{"systemMessage":"codex-saver is downloading its helper; this prompt is not protected yet."}'
        ;;
    *) (update) </dev/null >/dev/null 2>&1 & ;;
esac
