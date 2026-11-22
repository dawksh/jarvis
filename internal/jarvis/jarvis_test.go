package jarvis

import (
	"context"
	"database/sql"
	"encoding/json"
	"gopkg.in/yaml.v3"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func testStore(t *testing.T) *Store {
	t.Helper()
	s, e := Open(t.TempDir())
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { s.Close() })
	return s
}
func testConfig(t *testing.T) Config {
	return Config{Owner: 10, Chat: 20, DataDir: t.TempDir(), WorkDir: t.TempDir(), MaxQueue: 50, MaxAttempts: 5, Timeout: time.Hour, Confidence: .7, Tiers: map[string]Tier{"light": {"small", "low"}, "standard": {"normal", "medium"}, "heavy": {"large", "high"}}}
}
func update(t *testing.T, id, owner int64, text string) Update {
	t.Helper()
	var u Update
	b := `{"update_id":` + jsonNumber(id) + `,"message":{"from":{"id":` + jsonNumber(owner) + `},"chat":{"id":20,"type":"private"},"text":` + quoted(text) + `}}`
	if e := json.Unmarshal([]byte(b), &u); e != nil {
		t.Fatal(e)
	}
	return u
}
func quoted(s string) string    { b, _ := json.Marshal(s); return string(b) }
func jsonNumber(n int64) string { b, _ := json.Marshal(n); return string(b) }
func accept(t *testing.T, s *Store, c Config, id int64, text string) {
	t.Helper()
	if e := s.Intake(id, update(t, id, 10, text)); e != nil {
		t.Fatal(e)
	}
	if e := s.Dispatch(c); e != nil {
		t.Fatal(e)
	}
}
func TestIntakeAuthAndDedup(t *testing.T) {
	s := testStore(t)
	c := testConfig(t)
	u := update(t, 100, 99, "unauthorized")
	if e := s.Intake(100, u); e != nil {
		t.Fatal(e)
	}
	if e := s.Dispatch(c); e != nil {
		t.Fatal(e)
	}
	accept(t, s, c, 101, "inspect servers")
	accept(t, s, c, 101, "inspect servers")
	tasks, e := s.Tasks()
	if e != nil || len(tasks) != 1 {
		t.Fatalf("tasks=%v error=%v", tasks, e)
	}
	if s.Offset() != 102 {
		t.Fatal(s.Offset())
	}
	var n int
	s.QueryRow("SELECT count(*) FROM messages").Scan(&n)
	if n != 1 {
		t.Fatal(n)
	}
}
func TestPendingFollowupsAndNewConversation(t *testing.T) {
	s := testStore(t)
	c := testConfig(t)
	accept(t, s, c, 1, "build")
	accept(t, s, c, 2, "add tests")
	var n int
	if e := s.QueryRow("SELECT count(*) FROM instructions WHERE applied=0").Scan(&n); e != nil || n != 1 {
		t.Fatal(n, e)
	}
	accept(t, s, c, 3, "/new")
	accept(t, s, c, 4, "separate project")
	tasks, _ := s.Tasks()
	if len(tasks) != 2 || tasks[0].Conversation == tasks[1].Conversation {
		t.Fatal(tasks)
	}
}
func TestSlotConstraintAndRollback(t *testing.T) {
	s := testStore(t)
	c := testConfig(t)
	accept(t, s, c, 1, "one")
	accept(t, s, c, 2, "/task two")
	tasks, _ := s.Tasks()
	e := s.Atomic(func(tx *sql.Tx) error {
		_, e := tx.Exec("INSERT INTO attempts(id,task_id,number,status,created) VALUES('a',?,1,'running',0),('b',?,1,'claimed',0)", tasks[0].ID, tasks[1].ID)
		return e
	})
	if e == nil {
		t.Fatal("two active executions accepted")
	}
	var n int
	s.QueryRow("SELECT count(*) FROM attempts").Scan(&n)
	if n != 0 {
		t.Fatal("transaction did not rollback")
	}
}

type fakeSupervisor struct {
	dir       string
	alive     bool
	healthErr error
	launches  int
	request   RunRequest
	stops     int
}

func (f *fakeSupervisor) Dir(string) string { return f.dir }
func (f *fakeSupervisor) Launch(_ context.Context, _ string, r RunRequest) error {
	f.launches++
	f.request = r
	return nil
}
func (f *fakeSupervisor) Alive(context.Context, string) (bool, error) { return f.alive, f.healthErr }
func (f *fakeSupervisor) Stop(context.Context, string) error          { f.stops++; f.alive = false; return nil }
func TestExecutionRestartAndEventReplay(t *testing.T) {
	t.Setenv("TYPESAFE_API_KEY", "")
	s := testStore(t)
	c := testConfig(t)
	f := &fakeSupervisor{dir: t.TempDir(), alive: true}
	service := Service{Config: c, Store: s, Supervisor: f}
	accept(t, s, c, 1, "build")
	if e := service.Tick(context.Background()); e != nil {
		t.Fatal(e)
	}
	if f.launches != 1 {
		t.Fatal(f.launches)
	}
	for i := 0; i < 2; i++ {
		if e := service.Tick(context.Background()); e != nil {
			t.Fatal(e)
		}
	}
	if f.launches != 1 {
		t.Fatal("duplicate launch")
	}
	events := `{"type":"thread.started","thread_id":"session-123"}` + "\n" + `{"type":"item.completed","item":{"type":"agent_message","text":"{\"status\":\"completed\",\"summary\":\"verified\"}"}}` + "\n" + `{"type":"turn.completed"}` + "\n"
	if e := os.WriteFile(filepath.Join(f.dir, "events.jsonl"), []byte(events), 0600); e != nil {
		t.Fatal(e)
	}
	if e := service.Tick(context.Background()); e != nil {
		t.Fatal(e)
	}
	if e := service.Tick(context.Background()); e != nil {
		t.Fatal(e)
	}
	var n int
	s.QueryRow("SELECT count(*) FROM events").Scan(&n)
	if n != 3 {
		t.Fatal("event duplicate", n)
	}
	if e := writeJSON(filepath.Join(f.dir, "result.json"), RunResult{ExitCode: 0}); e != nil {
		t.Fatal(e)
	}
	f.alive = false
	if e := service.Tick(context.Background()); e != nil {
		t.Fatal(e)
	}
	tasks, _ := s.Tasks()
	if tasks[0].Status != "completed" || tasks[0].Session != "session-123" {
		t.Fatal(tasks)
	}
}
func TestUncertainSideEffectRequiresOwner(t *testing.T) {
	t.Setenv("TYPESAFE_API_KEY", "")
	s := testStore(t)
	c := testConfig(t)
	f := &fakeSupervisor{dir: t.TempDir()}
	service := Service{Config: c, Store: s, Supervisor: f}
	accept(t, s, c, 1, "deploy")
	if e := service.Tick(context.Background()); e != nil {
		t.Fatal(e)
	}
	if e := service.Tick(context.Background()); e != nil {
		t.Fatal(e)
	}
	tasks, _ := s.Tasks()
	if tasks[0].Status != "waiting_user" {
		t.Fatal(tasks)
	}
	if e := service.Tick(context.Background()); e != nil {
		t.Fatal(e)
	}
	if f.launches != 1 {
		t.Fatal("uncertain side effect automatically replayed")
	}
}
func TestCancelWaitsForTermination(t *testing.T) {
	t.Setenv("TYPESAFE_API_KEY", "")
	s := testStore(t)
	c := testConfig(t)
	f := &fakeSupervisor{dir: t.TempDir(), alive: true}
	service := Service{Config: c, Store: s, Supervisor: f}
	accept(t, s, c, 1, "work")
	if e := service.Tick(context.Background()); e != nil {
		t.Fatal(e)
	}
	accept(t, s, c, 2, "/cancel")
	if e := service.Tick(context.Background()); e != nil {
		t.Fatal(e)
	}
	tasks, _ := s.Tasks()
	if tasks[0].Status != "cancelling" || f.stops != 1 {
		t.Fatal(tasks, f.stops)
	}
	if e := service.Tick(context.Background()); e != nil {
		t.Fatal(e)
	}
	tasks, _ = s.Tasks()
	if tasks[0].Status != "cancelled" {
		t.Fatal(tasks)
	}
}
func TestRouterContractAndFallback(t *testing.T) {
	t.Setenv("TYPESAFE_API_KEY", "secret")
	c := testConfig(t)
	body := `{"answers":{"tier":{"type":"choice","choice":"heavy","confidence":0.95}}}`
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer secret" {
			t.Error("authorization missing")
		}
		var req map[string]any
		if e := json.NewDecoder(r.Body).Decode(&req); e != nil {
			t.Error(e)
		}
		if req["questions"] == nil || req["state"] == nil {
			t.Error(req)
		}
		w.Write([]byte(body))
	}))
	defer server.Close()
	c.JevEndpoint = server.URL
	if got := Route(context.Background(), c, "complex", "auto"); got.Tier != "heavy" {
		t.Fatal(got)
	}
	body = `{"answers":{"tier":{"type":"choice","choice":"invented","confidence":1}}}`
	if got := Route(context.Background(), c, "complex", "auto"); got.Tier != "standard" || got.Source != "fallback" {
		t.Fatal(got)
	}
	if got := Route(context.Background(), c, "complex", "light"); got.Source != "override" {
		t.Fatal(got)
	}
}
func TestRunnerContractAndSecretIsolation(t *testing.T) {
	dir := t.TempDir()
	workspace := t.TempDir()
	fake := filepath.Join(t.TempDir(), "codex")
	script := "#!/bin/sh\nif [ -n \"$TELEGRAM_BOT_TOKEN$TYPESAFE_API_KEY\" ]; then exit 99; fi\ncat >/dev/null\nprintf '%s\\n' '{\"type\":\"thread.started\",\"thread_id\":\"fake\"}'\n"
	if e := os.WriteFile(fake, []byte(script), 0700); e != nil {
		t.Fatal(e)
	}
	t.Setenv("TELEGRAM_BOT_TOKEN", "private")
	t.Setenv("TYPESAFE_API_KEY", "private")
	r := RunRequest{Codex: fake, Workspace: workspace, Model: "normal", Reasoning: "medium", Session: "explicit", Prompt: "test", Timeout: time.Second}
	if e := writeJSON(filepath.Join(dir, "request.json"), r); e != nil {
		t.Fatal(e)
	}
	if e := RunAttempt(dir); e != nil {
		t.Fatal(e)
	}
	b, e := os.ReadFile(filepath.Join(dir, "result.json"))
	if e != nil {
		t.Fatal(e)
	}
	var result RunResult
	json.Unmarshal(b, &result)
	if result.ExitCode != 0 {
		t.Fatal(result)
	}
	a := strings.Join(codexArgs(r), " ")
	if !strings.Contains(a, "resume explicit -") || strings.Contains(a, "--last") {
		t.Fatal(a)
	}
}
func TestBackupRestore(t *testing.T) {
	s := testStore(t)
	c := testConfig(t)
	accept(t, s, c, 1, "durable task")
	dir := t.TempDir()
	if e := s.Backup(dir); e != nil {
		t.Fatal(e)
	}
	files, _ := filepath.Glob(filepath.Join(dir, "*.db"))
	db, e := sql.Open("sqlite", files[0])
	if e != nil {
		t.Fatal(e)
	}
	defer db.Close()
	var n int
	if e = db.QueryRow("SELECT count(*) FROM tasks").Scan(&n); e != nil || n != 1 {
		t.Fatal(n, e)
	}
	var result string
	if e = db.QueryRow("PRAGMA integrity_check").Scan(&result); e != nil || result != "ok" {
		t.Fatal(result, e)
	}
}

func TestReasoningEscalationAndRetryBound(t *testing.T) {
	t.Setenv("TYPESAFE_API_KEY", "")
	s := testStore(t)
	c := testConfig(t)
	c.MaxAttempts = 4
	f := &fakeSupervisor{dir: t.TempDir()}
	service := Service{Config: c, Store: s, Supervisor: f}
	accept(t, s, c, 1, "fix tests")
	for i := 0; i < 4; i++ {
		os.Remove(filepath.Join(f.dir, "result.json"))
		os.Remove(filepath.Join(f.dir, "events.jsonl"))
		if e := service.Tick(context.Background()); e != nil {
			t.Fatal(e)
		}
		expected := "normal"
		if i >= 2 {
			expected = "large"
		}
		if f.request.Model != expected {
			t.Fatalf("attempt %d model %s expected %s", i, f.request.Model, expected)
		}
		events := `{"type":"thread.started","thread_id":"session"}` + "\n" + `{"type":"item.completed","item":{"type":"agent_message","text":"{\"status\":\"failed\",\"summary\":\"tests still fail\",\"failure_class\":\"reasoning\"}"}}` + "\n" + `{"type":"turn.completed"}` + "\n"
		if e := os.WriteFile(filepath.Join(f.dir, "events.jsonl"), []byte(events), 0600); e != nil {
			t.Fatal(e)
		}
		if e := writeJSON(filepath.Join(f.dir, "result.json"), RunResult{}); e != nil {
			t.Fatal(e)
		}
		if e := service.Tick(context.Background()); e != nil {
			t.Fatal(e)
		}
		tasks, _ := s.Tasks()
		if i < 3 {
			if tasks[0].Status != "retry_scheduled" || tasks[0].RetryAfter <= time.Now().Unix() {
				t.Fatal(tasks)
			}
			s.Exec("UPDATE tasks SET retry_after=0")
		} else if tasks[0].Status != "failed" {
			t.Fatal(tasks)
		}
	}
}
func TestProviderResetAndNoInfrastructureEscalation(t *testing.T) {
	t.Setenv("TYPESAFE_API_KEY", "")
	s := testStore(t)
	c := testConfig(t)
	f := &fakeSupervisor{dir: t.TempDir()}
	service := Service{Config: c, Store: s, Supervisor: f}
	accept(t, s, c, 1, "deploy")
	for i := 0; i < 3; i++ {
		os.Remove(filepath.Join(f.dir, "result.json"))
		os.Remove(filepath.Join(f.dir, "events.jsonl"))
		if e := service.Tick(context.Background()); e != nil {
			t.Fatal(e)
		}
		if f.request.Model != "normal" {
			t.Fatal("rate limit escalated model")
		}
		reset := time.Now().Add(time.Hour).Unix()
		events := `{"type":"thread.started","thread_id":"session"}` + "\n" + `{"type":"turn.failed","error":{"code":"rate_limit_exceeded","retry_at":` + jsonNumber(reset) + `}}` + "\n"
		os.WriteFile(filepath.Join(f.dir, "events.jsonl"), []byte(events), 0600)
		writeJSON(filepath.Join(f.dir, "result.json"), RunResult{ExitCode: 1})
		if e := service.Tick(context.Background()); e != nil {
			t.Fatal(e)
		}
		tasks, _ := s.Tasks()
		if tasks[0].Status != "waiting_rate_limit" || tasks[0].RetryAfter != reset {
			t.Fatal(tasks)
		}
		s.Exec("UPDATE tasks SET retry_after=0")
	}
}
func TestApprovalResponseBinding(t *testing.T) {
	s := testStore(t)
	c := testConfig(t)
	accept(t, s, c, 1, "manage database")
	tasks, _ := s.Tasks()
	id := tasks[0].ID
	if _, e := s.Exec("INSERT INTO attempts(id,task_id,number,status,created) VALUES('a',?,1,'running',0)", id); e != nil {
		t.Fatal(e)
	}
	service := Service{Config: c, Store: s}
	if e := service.finish("a", id, "waiting_user", "Drop test database on u2?", 0); e != nil {
		t.Fatal(e)
	}
	accept(t, s, c, 2, "Yes, drop that test database only")
	var response string
	if e := s.QueryRow("SELECT response FROM approval_requests WHERE task_id=?", id).Scan(&response); e != nil || response != "Yes, drop that test database only" {
		t.Fatal(response, e)
	}
	tasks, _ = s.Tasks()
	if tasks[0].Status != "queued" || tasks[0].Version != 1 {
		t.Fatal(tasks)
	}
}
func TestOutboxRedactionEditAndRetry(t *testing.T) {
	s := testStore(t)
	c := testConfig(t)
	t.Setenv("TELEGRAM_BOT_TOKEN", "BOT-SECRET")
	t.Setenv("TYPESAFE_API_KEY", "JEV-SECRET")
	method := ""
	var text string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		method = r.URL.Path
		var request map[string]any
		json.NewDecoder(r.Body).Decode(&request)
		text, _ = request["text"].(string)
		w.Write([]byte(`{"ok":true,"result":{"message_id":77}}`))
	}))
	defer server.Close()
	service := Service{Config: c, Store: s, Telegram: Telegram{Token: "BOT-SECRET", BaseURL: server.URL}}
	s.Exec("INSERT INTO notifications(content,task_id) VALUES('BOT-SECRET JEV-SECRET','task')")
	if e := service.Deliver(context.Background()); e != nil {
		t.Fatal(e)
	}
	if strings.Contains(text, "SECRET") || !strings.HasSuffix(method, "sendMessage") {
		t.Fatal(method, text)
	}
	s.Exec("INSERT INTO notifications(content,task_id) VALUES('Completed','task')")
	if e := service.Deliver(context.Background()); e != nil {
		t.Fatal(e)
	}
	if !strings.HasSuffix(method, "editMessageText") {
		t.Fatal(method)
	}
}
func TestUnknownProcessRetainsSlot(t *testing.T) {
	t.Setenv("TYPESAFE_API_KEY", "")
	s := testStore(t)
	c := testConfig(t)
	f := &fakeSupervisor{dir: t.TempDir(), healthErr: os.ErrPermission}
	service := Service{Config: c, Store: s, Supervisor: f}
	accept(t, s, c, 1, "deploy")
	if e := service.Tick(context.Background()); e != nil {
		t.Fatal(e)
	}
	accept(t, s, c, 2, "/task second")
	for i := 0; i < 3; i++ {
		if e := service.Tick(context.Background()); e != nil {
			t.Fatal(e)
		}
	}
	if f.launches != 1 {
		t.Fatal("unobservable attempt released its slot")
	}
	var n int
	s.QueryRow("SELECT count(*) FROM attempts WHERE status IN ('claimed','running')").Scan(&n)
	if n != 1 {
		t.Fatal(n)
	}
}
func TestConfigValidation(t *testing.T) {
	c := testConfig(t)
	c.Codex = "codex"
	c.Backend = "systemd"
	path := filepath.Join(t.TempDir(), "config.yaml")
	b, e := yaml.Marshal(c)
	if e != nil {
		t.Fatal(e)
	}
	os.WriteFile(path, b, 0600)
	if _, e := LoadConfig(path); e != nil {
		t.Fatal(e)
	}
	c.Tiers["standard"] = Tier{"", "medium"}
	b, _ = yaml.Marshal(c)
	os.WriteFile(path, b, 0600)
	if _, e := LoadConfig(path); e == nil {
		t.Fatal("empty model accepted")
	}
}

func TestStoreRestartAndFutureSchema(t *testing.T) {
	dir := t.TempDir()
	s, e := Open(dir)
	if e != nil {
		t.Fatal(e)
	}
	c := testConfig(t)
	accept(t, s, c, 1, "persist me")
	s.Close()
	s, e = Open(dir)
	if e != nil {
		t.Fatal(e)
	}
	tasks, e := s.Tasks()
	if e != nil || len(tasks) != 1 || s.Offset() != 2 {
		t.Fatal(tasks, e)
	}
	if _, e = s.Exec("INSERT INTO schema_migrations VALUES(99)"); e != nil {
		t.Fatal(e)
	}
	s.Close()
	if s, e = Open(dir); e == nil {
		s.Close()
		t.Fatal("future schema accepted")
	}
}
func TestPartialEventDoesNotAdvanceOffset(t *testing.T) {
	t.Setenv("TYPESAFE_API_KEY", "")
	s := testStore(t)
	c := testConfig(t)
	f := &fakeSupervisor{dir: t.TempDir(), alive: true}
	service := Service{Config: c, Store: s, Supervisor: f}
	accept(t, s, c, 1, "work")
	if e := service.Tick(context.Background()); e != nil {
		t.Fatal(e)
	}
	line := `{"type":"thread.started","thread_id":"session"}`
	path := filepath.Join(f.dir, "events.jsonl")
	os.WriteFile(path, []byte(line), 0600)
	if e := service.Tick(context.Background()); e != nil {
		t.Fatal(e)
	}
	var offset int
	s.QueryRow("SELECT offset FROM attempts").Scan(&offset)
	if offset != 0 {
		t.Fatal(offset)
	}
	os.WriteFile(path, []byte(line+"\n"), 0600)
	if e := service.Tick(context.Background()); e != nil {
		t.Fatal(e)
	}
	tasks, _ := s.Tasks()
	if tasks[0].Session != "session" {
		t.Fatal(tasks)
	}
}
