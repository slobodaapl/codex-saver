# codex-saver

Schedule Codex thread compaction while the prompt cache may still be warm. After a completed turn or failed model request, the helper waits 25 minutes, checks that the same turn is still the latest and the thread is inactive (`idle` or `systemError`), then requests compaction from the local remote-control app server. A new prompt cancels the pending request.

Codex 0.166.0 does not allow external compaction of multi-agent v2 subagents. When the server reports that direct input is unavailable, codex-saver deletes the inactive subagent and its stored history instead. Deletion also removes its spawned descendants; the helper defers deletion if any descendant is active or its status is unavailable. The subagent cannot be resumed after deletion.

The app server does not offer an atomic idle precondition. A thread or descendant can become active between the final check and the request; compaction may replace that turn, and deletion may shut it down.

Hooks start one helper on demand. The helper watches the app-server daemon process and exits when it exits. After a daemon restart, the next hook starts a new helper and reloads saved jobs; a deadline can be missed before that hook runs. The helper retries a busy thread or unavailable server after 10 minutes while the daemon remains alive. Server rejections and uncertain outcomes are logged to `$XDG_STATE_HOME/codex-saver/helper.log`, or `~/.local/state/codex-saver/helper.log` when that variable is unset. An uncertain operation is not repeated. Scheduled jobs use the same directory.

Automatic compaction is never requested at or after the estimated 30-minute cache deadline, including on retry. On a new user prompt, the hook checks the latest completed or failed turn. If it is older than the cache window and has not been compacted, the hook blocks the prompt and recommends `/clear`. Resubmit the same message once to continue the existing thread. If the previous turn has no timestamp, the hook explains that its age is unknown and offers the same choice. An unavailable helper or app server lets the prompt through.

Deadlines and tracked turn ages use Linux's suspend-aware boot clock. Pending jobs from another boot or time namespace are discarded. For threads the helper did not observe in the current boot, the prompt check falls back to Codex's wall-clock completion timestamp; a clock correction before that timestamp cannot be detected.

## Plugin

After the repository and its first release are published, install the plugin and approve its hooks in Codex:

```sh
codex plugin marketplace add slobodaapl/codex-saver
codex plugin add codex-saver@codex-saver
```

The plugin supports Linux x86-64 and AArch64 and needs `sh`, `curl`, `flock`, `sha256sum`, `awk`, and standard Unix utilities.

On first use the hook downloads the matching release binary in the background. The first prompt proceeds without protection and displays a warning while the download is pending. Completion and failure hooks that arrive during the download retain their original scheduling time. Subsequent prompts check for a release at most once a day and wait for the check and any download. Replacing the binary blocks that prompt and warns you to reopen the Codex thread, re-trust the hook, and resend your message. A failed lookup, download, or checksum check leaves the existing binary in use and lets the prompt proceed. The download and checksum come from the same GitHub release; the checksum detects a damaged or mismatched download, not a compromised release. A running helper keeps its current code until its app-server daemon exits; later hooks can start the updated binary. Before scheduled actions, the helper checks that the plugin is still installed and enabled.

For development, build with `cargo build --release`. The hook definitions in `integration/` are provided for manual integration and are not installed automatically. Merge them with existing Codex hooks and approve them in Codex. No systemd service is required.

The default delays and cache window can be changed with `CODEX_SAVER_DELAY_SECONDS`, `CODEX_SAVER_RETRY_SECONDS`, and `CODEX_SAVER_WINDOW_SECONDS` in the hook environment. The hook sends identifiers, the root transcript path, and a prompt fingerprint over a user-private Unix socket; it does not send prompt or response text.
