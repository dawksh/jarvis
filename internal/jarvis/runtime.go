package jarvis

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"
)

const SupportedCodex = "codex-cli 0.171.0"

type RunRequest struct {
	Codex, Workspace, Model, Reasoning, Session, Prompt string
	Timeout                                             time.Duration
}
type RunResult struct {
	ExitCode int
	Error    string
	Finished time.Time
}

func writeJSON(path string, v any) error {
	b, e := json.Marshal(v)
	if e != nil {
		return e
	}
	f, e := os.OpenFile(path+".tmp", os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0600)
	if e != nil {
		return e
	}
	if _, e = f.Write(b); e != nil {
		f.Close()
		return e
	}
	if e = f.Sync(); e != nil {
		f.Close()
		return e
	}
	if e = f.Close(); e != nil {
		return e
	}
	if e = os.Rename(path+".tmp", path); e != nil {
		return e
	}
	d, e := os.Open(filepath.Dir(path))
	if e != nil {
		return e
	}
	defer d.Close()
	return d.Sync()
}

// stderr is diagnostic output; cap it without blocking the agent on verbose tools.
type cappedLogWriter struct {
	file      *os.File
	remaining int
}

func (w *cappedLogWriter) Write(b []byte) (int, error) {
	n := len(b)
	take := min(n, w.remaining)
	if take > 0 {
		if _, e := w.file.Write(b[:take]); e != nil {
			return 0, e
		}
		w.remaining -= take
		if e := w.file.Sync(); e != nil {
			return 0, e
		}
	}
	return n, nil
}

type durableWriter struct{ file *os.File }

func (w durableWriter) Write(b []byte) (int, error) {
	n, e := w.file.Write(b)
	if e == nil {
		e = w.file.Sync()
	}
	return n, e
}
func codexArgs(r RunRequest) []string {
	a := []string{"exec", "--json", "--ignore-user-config", "--skip-git-repo-check", "-c", `approval_policy="never"`, "-c", `sandbox_mode="danger-full-access"`, "-c", `model_reasoning_effort="` + r.Reasoning + `"`, "--model", r.Model, "--output-schema", filepath.Join(r.Workspace, ".jarvis-result.schema.json")}
	if r.Session != "" {
		a = append(a, "resume", r.Session)
	}
	return append(a, "-")
}

const resultSchema = `{"type":"object","properties":{"status":{"type":"string","enum":["completed","waiting_user","failed"]},"summary":{"type":"string"},"failure_class":{"type":"string","enum":["none","reasoning","network","auth","rate_limit","unknown"]}},"required":["status","summary","failure_class"],"additionalProperties":false}`

// RunAttempt is a private subprocess entrypoint. Its files outlive the daemon reader.
func RunAttempt(dir string) error {
	b, e := os.ReadFile(filepath.Join(dir, "request.json"))
	if e != nil {
		return e
	}
	var r RunRequest
	if e = json.Unmarshal(b, &r); e != nil {
		return e
	}
	if e = os.WriteFile(filepath.Join(r.Workspace, ".jarvis-result.schema.json"), []byte(resultSchema), 0600); e != nil {
		return e
	}
	out, e := os.OpenFile(filepath.Join(dir, "events.jsonl"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if e != nil {
		return e
	}
	defer out.Close()
	errout, e := os.OpenFile(filepath.Join(dir, "stderr.log"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if e != nil {
		return e
	}
	defer errout.Close()
	cmd := exec.Command(r.Codex, codexArgs(r)...)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Dir = r.Workspace
	cmd.Stdin = strings.NewReader(r.Prompt)
	cmd.Stdout = durableWriter{out}
	cmd.Stderr = &cappedLogWriter{file: errout, remaining: 25 << 20}
	// Bot and router credentials must not enter the unrestricted agent environment.
	for _, k := range []string{"PATH", "HOME", "USER", "LANG", "CODEX_HOME", "SSH_AUTH_SOCK", "TMPDIR"} {
		if v := os.Getenv(k); v != "" {
			cmd.Env = append(cmd.Env, k+"="+v)
		}
	}
	if e = cmd.Start(); e != nil {
		return writeJSON(filepath.Join(dir, "result.json"), RunResult{-1, "launch failed", time.Now()})
	}
	_ = writeJSON(filepath.Join(dir, "process.json"), map[string]int{"pid": cmd.Process.Pid})
	timer := time.AfterFunc(r.Timeout, func() { _ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL) })
	e = cmd.Wait()
	timer.Stop()
	result := RunResult{ExitCode: 0, Finished: time.Now()}
	if e != nil {
		result.ExitCode = -1
		if x, ok := e.(*exec.ExitError); ok {
			result.ExitCode = x.ExitCode()
		}
		result.Error = "Codex exited unsuccessfully"
	}
	if e = out.Sync(); e != nil {
		return e
	}
	if e = errout.Sync(); e != nil {
		return e
	}
	return writeJSON(filepath.Join(dir, "result.json"), result)
}

type Supervisor struct{ Config Config }

func (s Supervisor) Dir(id string) string { return filepath.Join(s.Config.DataDir, "attempts", id) }
func (s Supervisor) Launch(ctx context.Context, id string, r RunRequest) error {
	dir := s.Dir(id)
	if e := os.MkdirAll(dir, 0700); e != nil {
		return e
	}
	if e := writeJSON(filepath.Join(dir, "request.json"), r); e != nil {
		return e
	}
	exe, e := os.Executable()
	if e != nil {
		return e
	}
	if s.Config.Backend == "systemd" {
		launchCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
		defer cancel()
		cmd := exec.CommandContext(launchCtx, "systemd-run", "--user", "--unit=jarvis-"+id, "--property=KillMode=control-group", "--property=TimeoutStopSec=15", "--property=RuntimeMaxSec="+strconv.Itoa(int(r.Timeout.Seconds())+30), "--", exe, "run-attempt", dir)
		if e = cmd.Run(); e != nil {
			return fmt.Errorf("systemd launch failed: %w", e)
		}
		return nil
	}
	cmd := exec.Command(exe, "run-attempt", dir)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	if e = cmd.Start(); e != nil {
		return e
	}
	if e = writeJSON(filepath.Join(dir, "runner.json"), map[string]int{"pid": cmd.Process.Pid}); e != nil {
		return e
	}
	go cmd.Wait()
	return nil
}
func (s Supervisor) Alive(ctx context.Context, id string) (bool, error) {
	if s.Config.Backend != "systemd" {
		return false, fmt.Errorf("local process identity cannot be safely reattached")
	}
	b, e := exec.CommandContext(ctx, "systemctl", "--user", "show", "jarvis-"+id, "--property=LoadState", "--property=ActiveState").Output()
	if strings.Contains(string(b), "LoadState=not-found") {
		return false, nil
	}
	if e != nil {
		return false, e
	}
	for _, state := range []string{"active", "activating", "deactivating", "reloading"} {
		if strings.Contains(string(b), "ActiveState="+state+"\n") {
			return true, nil
		}
	}
	if strings.Contains(string(b), "ActiveState=inactive") || strings.Contains(string(b), "ActiveState=failed") {
		return false, nil
	}
	return false, fmt.Errorf("unrecognized systemd unit state")
}
func (s Supervisor) Stop(ctx context.Context, id string) error {
	if s.Config.Backend == "systemd" {
		return exec.CommandContext(ctx, "systemctl", "--user", "stop", "jarvis-"+id).Run()
	}
	// Local mode deliberately refuses PID-based signalling after daemon restart.
	return fmt.Errorf("local backend requires manual termination; use systemd in production")
}
