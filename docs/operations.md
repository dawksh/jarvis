# Operations

## Host setup

Create a dedicated `jarvis` Linux user with home `/var/lib/jarvis`; provision administrative
sudo access deliberately and audit its use. Install the binary at `/usr/local/bin/jarvis` and
the supported Codex CLI at the configured absolute path. Sign into Codex as that same user.
Keep state, config and SSH credentials outside workspaces; permissions 0700 directories,
0600 files. Full administrator access means these paths are not a hard security boundary.

Enable lingering for the user (`loginctl enable-linger jarvis`) so its user systemd manager
survives logout. Install the supplied service in its user unit directory. Put secrets and
CODEX_*_MODEL values in `/var/lib/jarvis/config/jarvis.env`, protected mode 0600. The unit's
EnvironmentFile does not use shell `export` statements. Run `systemctl --user daemon-reload`
and `systemctl --user enable --now jarvis` as jarvis with its user bus available.

Provision Tailscale separately. Pin U2's SSH host key after checking its fingerprint through
an independent trusted channel. Use BatchMode and StrictHostKeyChecking in the jarvis user's
SSH config. Disable password prompts and confirm sudo is noninteractive where authorized.
The daemon gives Codex inventory; Codex executes SSH. Doctor checks connectivity.

## Controls and recovery

Execution runs as sibling `jarvis-ATTEMPT.service` user units. Restarting the daemon leaves
these units running. Each attempt directory contains request, events, stderr and result
artifacts; these can contain sensitive task data and must remain private. Unknown execution
identity retains the execution slot. Inspect `systemctl --user status jarvis-ATTEMPT` and
artifacts before intervening. Do not delete active attempts to bypass the slot.

Emergency stop: stop the daemon, then stop all matching attempt units using the user
manager. This does not reverse remote operations or stop remote detached jobs; inspect U2
before resuming. `/cancel` waits for local execution termination, not remote rollback.

`backend: local` is development-only: restart-safe identity checks and automated cancellation
are intentionally unavailable. Use Linux systemd for acceptance tests and production.

Owner approval is a behavioral protocol with the unrestricted agent. The final schema can
return `waiting_user` with an exact proposed action; a subsequent owner message resumes with
that context. It is not a command interception/security boundary. Do not claim exactly-once
side effects. Missing/invalid final outcomes and uncertain failures wait for the owner.
Structured rate-limit errors use provider reset metadata when available, otherwise a 15-minute delay with a configured failure bound.

## Backup and restore

The daemon creates a consistent SQLite backup using VACUUM INTO every 24 hours.
`jarvis ... backup` makes one immediately. Backups remain outside agent workspaces.
Copy them to separately secured storage. Stop the daemon and all attempt units before restore.
Preserve the old database and WAL/SHM files together. Restore the backup as `state/jarvis.db`,
remove old WAL/SHM files from the restored directory, set ownership and mode 0600, then run
SQLite `PRAGMA integrity_check` before starting. Retain the Codex home/session files,
workspaces and attempt artifacts too: a database backup alone does not restore agent context.

## Required live acceptance

- Verify each configured model and reasoning effort with subscription authentication.
- Authenticate/reject Telegram users and groups; redeliver identical updates.
- Run U1/U2 inspection and a reversible service change against the intended target.
- Kill Codex and restart/reboot U1; check slot ownership and durable event recovery.
- Disconnect U2 during a side effect; confirm inspection precedes repetition.
- Exercise rate limiting, Jev failure, pending follow-ups, pause/cancel and owner approval.
- Check systemd resource usage, idle daemon RSS, disk growth and backup restore.
- Complete the dashboard scenario with a health check and an injected interruption.

## Acceptance limits

Live Linux supervision, provider authentication/model access, remote side effects and the
full dashboard scenario still require the checklist above. The implementation supports
bounded conversation history, per-tier reasoning escalation after two eligible failures,
classified network/rate-limit retries, hourly maintenance, daily backups and 14-day artifact
retention for terminal tasks. Disk space below 256 MiB suspends new attempts. Progress messages
are edited at most once per minute. Unknown provider errors wait for owner inspection.
Structured retry_at metadata is honored when present; otherwise rate limits wait 15 minutes.
Logs use journald retention on production hosts; SQLite task/event history remains durable.

Completion depends on Codex returning a structured outcome after its own verification; the
daemon does not independently validate arbitrary applications. Model entitlement is checked
through live acceptance, not guessed from model names. Approval requests bind to the task's
instruction version, but unrestricted shell still relies on Codex honoring the protocol.
