<h1 align="center">codex-saver</h1>

<p align="center">
  <strong>A Codex plugin for Linux that compacts idle threads before the cache goes cold.</strong>
</p>

<p align="center">
  Keep long coding sessions cheaper to return to.<br>
  Automatic compaction after 25 minutes of inactivity, while the input may still be cached.
</p>

<p align="center">
  <img src="assets/warm-cache-clean.gif" alt="Compacting context while the cache is warm">
</p>

<p align="center">
  <a href="#install"><strong>Install</strong></a> ·
  <a href="#what-it-does">How it works</a> ·
  <a href="#settings-and-logs">Settings</a>
</p>

<p align="center">
  <strong>Linux x86-64 + AArch64 · No systemd service · MIT license</strong>
</p>

## Install

Install the plugin and approve its hooks in Codex:

```sh
codex plugin marketplace add slobodaapl/codex-saver
codex plugin add codex-saver@codex-saver
```

Supports Linux x86-64 and AArch64. Requires `sh`, `curl`, `flock`, `sha256sum`, `awk`, `stat`, and standard Unix utilities. No systemd service is required.

Need a release for another architecture? [Open an issue](https://github.com/slobodaapl/codex-saver/issues).

On first use, the hook downloads the matching release binary in the background. The first prompt goes through without protection and displays a warning while the download is pending. Completion and failure events during the download keep their original scheduling time.

Later prompts check for updates at most once a day and wait for the check and any download. If the binary is replaced, that send is blocked with a warning to reopen the Codex thread, re-trust the hook, and resend your message. A failed lookup, download, or checksum check leaves the existing binary in use and lets the prompt through.

A running helper keeps its current code until its Codex app-server daemon exits. Later hooks can start the updated binary. The helper checks that the plugin is still installed and enabled before scheduled actions.

## Compact while cached. Return with less context.

A long Codex session can build up a large context. Step away past the cache TTL, and your next request can pay the uncached input cost for all of it.

**codex-saver schedules compaction before that happens.** It aims to use the cheaper cached input for compaction, leaving a smaller context for your next request. Resume work sooner and the pending compaction is cancelled automatically.

The defaults assume a 30-minute cache window and schedule compaction after 25 minutes. Cache retention is not guaranteed; the timer is an estimate, and compaction itself uses a model request.

## What it does

After a completed turn or failed model request, including a capacity error, codex-saver arms a timer. When it fires, it checks that the same turn is still the latest and the thread is inactive, then asks Codex to compact it. A new prompt cancels the pending action. It does not request automatic compaction at or after the estimated cache deadline.

If you return after that window and the latest completed or failed turn has not been compacted, the hook blocks your message and recommends `/clear`. You can start fresh, or resubmit the same message once to continue the existing thread. An unknown turn age gets the same choice. If the helper or app server is unavailable, the prompt goes through.

**Inactive subagents and their history may be permanently deleted during cleanup.**

If work resumes just as cleanup starts, that work can be interrupted. Compaction can also fail when Codex is at capacity, leaving the thread without cache protection.

## Settings and logs

The hook environment accepts these overrides, in seconds:

| Variable | Default | Purpose |
| --- | --- | --- |
| `CODEX_SAVER_DELAY_SECONDS` | `1500` | Wait before the first scheduled action |
| `CODEX_SAVER_RETRY_SECONDS` | `600` | Wait before retrying a busy thread or unavailable server |
| `CODEX_SAVER_WINDOW_SECONDS` | `1800` | Estimated cache window; automatic requests stop at this deadline |

Logs and saved jobs live in `$XDG_STATE_HOME/codex-saver/`, or `~/.local/state/codex-saver/` when that variable is unset. Check `helper.log` for server rejections or uncertain outcomes. An uncertain operation is not repeated.

The hook sends identifiers, the root transcript path, and a prompt fingerprint to the local helper over a user-private Unix socket. It does not send prompt or response text through that socket. Compaction runs through Codex's existing model connection. Binary downloads and update checks contact GitHub; the binary and checksum come from the same release, so the checksum detects download damage or a mismatch, not a compromised release.

---

## Development

Build and check the project with:

```sh
cargo build --release
cargo test --offline
cargo clippy --offline --all-targets --all-features -- -D warnings
cargo fmt --check
sh tests/bootstrap.sh
```

The offline commands require dependencies to be available locally. The bootstrap tests use mocked downloads; the metadata-write failure case requires a non-root process.

Hooks start one helper on demand. It talks to the local remote-control app server and exits when the app-server daemon exits. After a restart, the next hook reloads saved jobs; a deadline can be missed before that hook runs. Busy threads and unavailable servers are retried only while the daemon remains alive and the estimated cache deadline permits it.

Deadlines and observed turn ages use Linux's suspend-aware boot clock. Jobs from another boot or time namespace are discarded. For threads not observed during the current boot, prompt checks fall back to Codex's wall-clock completion timestamp; a clock correction before that timestamp cannot be detected. Both `idle` and `systemError` are treated as inactive thread states.

## Contributing

Contributions are welcome. Open an issue or a pull request; there are no particular contribution rules.

## License

[MIT](LICENSE), copyright (c) 2026 Tibor Sloboda.
