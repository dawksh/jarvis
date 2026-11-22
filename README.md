# Jarvis

A Go daemon for a single owner's Telegram-controlled Codex tasks. SQLite owns the queue,
messages and notifications. Explicit Codex sessions own task execution. Linux user systemd
units own process lifetimes independently of the daemon.

## Build and configure

```sh
go test ./...
go build -o bin/jarvis ./cmd/jarvis
```

Copy `configs/jarvis.example.yaml` to an external configuration directory. Set positive
numeric Telegram owner/private-chat IDs and model IDs verified against your Codex subscription.
The CLI contract currently targets **codex-cli 0.171.0**. Authenticate as the dedicated
`jarvis` OS user with `codex login --device-auth`. Provide `TELEGRAM_BOT_TOKEN` and
`OPENROUTER_API_KEY` in a mode-0600 environment file; the latter is optional during outages
and missing credentials cause routing to fall back to standard.

```sh
jarvis -config /absolute/path/jarvis.yaml doctor
jarvis -config /absolute/path/jarvis.yaml serve
jarvis -config /absolute/path/jarvis.yaml tasks
jarvis -config /absolute/path/jarvis.yaml task inspect TASK_ID
jarvis -config /absolute/path/jarvis.yaml backup
```

Configuration flags precede the command. `doctor` checks the CLI version, login, SSH and
systemd connectivity without executing an agent task. It cannot establish model entitlement;
verify each configured model using a harmless live task before enabling production execution.

## Telegram

Plain messages start tasks, or append to unfinished work in the current conversation.
`/task <instruction>` queues separate work; `/followup <instruction>` continues the latest
conversation task when none is unfinished. `/new` changes conversations without cancelling
work. `/model` changes defaults for future tasks. `/pause`, `/cancel`, `/resume`, `/status`,
`/tasks`, `/servers`, `/logs`, and `/help` provide deterministic control.

Follow-ups to a busy task apply on the next turn. Use `/cancel` explicitly to interrupt;
the exact phrases “stop”, “cancel”, and “stop what you’re doing” also cancel. Pausing terminates
execution and retains the session; it does not freeze a process or undo changes.

## Deployment and recovery

See [operations](docs/operations.md) for dedicated-user setup, systemd, SSH, restore,
and live acceptance checks. `plan.md` is deliberately local and ignored.

This is an initial implementation, not a production-validated V1 release. The Linux/live
acceptance checklist must pass on your hosts before unattended administrative use.
