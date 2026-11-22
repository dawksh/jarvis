package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/daksh/jarvis/internal/jarvis"
	"log/slog"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"
)

func main() {
	if e := run(); e != nil {
		slog.Error("jarvis stopped", "error", e)
		os.Exit(1)
	}
}
func run() error {
	if len(os.Args) == 3 && os.Args[1] == "run-attempt" {
		return jarvis.RunAttempt(os.Args[2])
	}
	flags := flag.NewFlagSet("jarvis", flag.ContinueOnError)
	config := flags.String("config", "configs/jarvis.yaml", "configuration path")
	if e := flags.Parse(os.Args[1:]); e != nil {
		return e
	}
	args := flags.Args()
	if len(args) == 0 {
		return fmt.Errorf("usage: jarvis [-config path] serve|doctor|status|tasks|task inspect <id>|backup")
	}
	c, e := jarvis.LoadConfig(*config)
	if e != nil {
		return e
	}
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stderr, nil)))
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer cancel()
	if args[0] == "doctor" {
		return doctor(ctx, c)
	}
	old := syscall.Umask(0077)
	defer syscall.Umask(old)
	s, e := jarvis.Open(c.DataDir)
	if e != nil {
		return e
	}
	defer s.Close()
	switch args[0] {
	case "serve":
		version, e := exec.CommandContext(ctx, c.Codex, "--version").Output()
		if e != nil || strings.TrimSpace(string(version)) != jarvis.SupportedCodex {
			return fmt.Errorf("serve requires %s; run doctor", jarvis.SupportedCodex)
		}
		// Process-scoped advisory lock prevents two pollers/schedulers sharing this DB.
		lock, e := os.OpenFile(filepath.Join(c.DataDir, "daemon.lock"), os.O_CREATE|os.O_RDWR, 0600)
		if e != nil {
			return e
		}
		defer lock.Close()
		if e = syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); e != nil {
			return fmt.Errorf("another daemon holds the execution lock")
		}
		service := jarvis.Service{Config: c, Store: s, Telegram: jarvis.Telegram{Token: os.Getenv("TELEGRAM_BOT_TOKEN")}, Supervisor: jarvis.Supervisor{Config: c}}
		childCtx, stop := context.WithCancel(ctx)
		defer stop()
		return service.Serve(childCtx)
	case "status", "tasks":
		tasks, e := s.Tasks()
		if e != nil {
			return e
		}
		return json.NewEncoder(os.Stdout).Encode(tasks)
	case "task":
		if len(args) != 3 || args[1] != "inspect" {
			return fmt.Errorf("usage: jarvis task inspect <id>")
		}
		task, e := s.GetTask(args[2])
		if e != nil {
			return e
		}
		rows, e := s.Query("SELECT id,number,status FROM attempts WHERE task_id=? ORDER BY number", task.ID)
		if e != nil {
			return e
		}
		defer rows.Close()
		type attempt struct {
			ID                string
			Number            int
			Status, Artifacts string
		}
		var attempts []attempt
		for rows.Next() {
			var a attempt
			if e = rows.Scan(&a.ID, &a.Number, &a.Status); e != nil {
				return e
			}
			a.Artifacts = filepath.Join(c.DataDir, "attempts", a.ID)
			attempts = append(attempts, a)
		}
		if e = rows.Err(); e != nil {
			return e
		}
		return json.NewEncoder(os.Stdout).Encode(struct {
			Task     jarvis.Task
			Attempts []attempt
		}{task, attempts})
	case "backup":
		return s.Backup(filepath.Join(c.DataDir, "backups"))
	default:
		return fmt.Errorf("unknown command %q", args[0])
	}
}
func doctor(ctx context.Context, c jarvis.Config) error {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	var failures []string
	check := func(name string, cmd *exec.Cmd) {
		b, e := cmd.CombinedOutput()
		if e != nil {
			fmt.Println(name + ": failed")
			failures = append(failures, name)
		} else if name == "Codex version" && strings.TrimSpace(string(b)) != jarvis.SupportedCodex {
			fmt.Printf("%s: unsupported version; require %s\n", name, jarvis.SupportedCodex)
			failures = append(failures, name)
		} else {
			fmt.Println(name + ": ok")
		}
	}
	check("Codex version", exec.CommandContext(ctx, c.Codex, "--version"))
	check("Codex login", exec.CommandContext(ctx, c.Codex, "login", "status"))
	if os.Getenv("TELEGRAM_BOT_TOKEN") == "" {
		failures = append(failures, "TELEGRAM_BOT_TOKEN")
		fmt.Println("TELEGRAM_BOT_TOKEN: missing")
	}
	if os.Getenv("OPENROUTER_API_KEY") == "" {
		fmt.Println("OPENROUTER_API_KEY: missing; routing uses standard fallback")
	}
	if c.Backend == "systemd" {
		check("systemd user manager", exec.CommandContext(ctx, "systemctl", "--user", "show-environment"))
	}
	for name, s := range c.Servers {
		if s.Type == "ssh" {
			if _, e := os.Stat(s.Identity); e != nil {
				failures = append(failures, "SSH identity "+name)
				continue
			}
			check("SSH "+name, exec.CommandContext(ctx, "ssh", "-o", "BatchMode=yes", "-o", "StrictHostKeyChecking=yes", "-o", "ConnectTimeout=5", "-i", s.Identity, s.User+"@"+s.Host, "true"))
		}
	}
	fmt.Println("Configured models require subscription capability verification before production use.")
	if len(failures) > 0 {
		return fmt.Errorf("doctor failed: %s", strings.Join(failures, ", "))
	}
	return nil
}
