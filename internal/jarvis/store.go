package jarvis

import (
	"crypto/rand"
	"database/sql"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"fmt"
	_ "modernc.org/sqlite"
	"os"
	"path/filepath"
	"time"
)

//go:embed migrations/001.sql
var initialSchema string

type Store struct{ *sql.DB }

func ID() string {
	var b [16]byte
	if _, e := rand.Read(b[:]); e != nil {
		panic(e)
	}
	return hex.EncodeToString(b[:])
}
func Open(dir string) (*Store, error) {
	if e := os.MkdirAll(dir, 0700); e != nil {
		return nil, e
	}
	db, e := sql.Open("sqlite", filepath.Join(dir, "jarvis.db"))
	if e != nil {
		return nil, e
	}
	db.SetMaxOpenConns(1)
	s := &Store{db}
	_, e = db.Exec("PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; PRAGMA foreign_keys=ON; PRAGMA busy_timeout=5000; CREATE TABLE IF NOT EXISTS schema_migrations(version INTEGER PRIMARY KEY)")
	if e != nil {
		db.Close()
		return nil, e
	}
	var version int
	if e = db.QueryRow("SELECT COALESCE(MAX(version),0) FROM schema_migrations").Scan(&version); e != nil {
		db.Close()
		return nil, e
	}
	if version > 1 {
		db.Close()
		return nil, fmt.Errorf("database schema %d is newer than this binary", version)
	}
	if version == 0 {
		e = s.Atomic(func(tx *sql.Tx) error { _, e := tx.Exec(initialSchema); return e })
	}

	if e != nil {
		db.Close()
		return nil, e
	}
	if e = os.Chmod(filepath.Join(dir, "jarvis.db"), 0600); e != nil {
		db.Close()
		return nil, e
	}
	return s, nil
}
func notify(tx *sql.Tx, s string) error {
	_, e := tx.Exec("INSERT INTO notifications(content) VALUES(?)", s)
	return e
}
func (s *Store) Atomic(f func(*sql.Tx) error) error {
	tx, e := s.Begin()
	if e != nil {
		return e
	}
	defer tx.Rollback()
	if e = f(tx); e != nil {
		return e
	}
	return tx.Commit()
}
func (s *Store) Intake(id int64, payload any) error {
	b, e := json.Marshal(payload)
	if e != nil {
		return e
	}
	return s.Atomic(func(tx *sql.Tx) error {
		if _, e := tx.Exec("INSERT OR IGNORE INTO updates(id,payload) VALUES(?,?)", id, string(b)); e != nil {
			return e
		}
		_, e := tx.Exec("INSERT INTO meta(key,value) VALUES('offset',?) ON CONFLICT(key) DO UPDATE SET value=MAX(CAST(value AS INTEGER),CAST(excluded.value AS INTEGER))", id+1)
		return e
	})
}
func (s *Store) Offset() int64 {
	var n int64
	s.QueryRow("SELECT value FROM meta WHERE key='offset'").Scan(&n)
	return n
}

type Task struct {
	ID, Conversation, Instruction, Status, Session, Tier, Model, Reasoning, Override, Summary string
	Version, RetryAfter, Created                                                              int64
}

const taskColumns = "id,conversation_id,instruction,status,session,tier,model,reasoning,override,summary,version,retry_after,created"

func scanTask(row interface{ Scan(...any) error }) (t Task, e error) {
	e = row.Scan(&t.ID, &t.Conversation, &t.Instruction, &t.Status, &t.Session, &t.Tier, &t.Model, &t.Reasoning, &t.Override, &t.Summary, &t.Version, &t.RetryAfter, &t.Created)
	return
}
func (s *Store) Tasks() ([]Task, error) {
	rows, e := s.Query("SELECT " + taskColumns + " FROM tasks ORDER BY created DESC,rowid DESC LIMIT 50")
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	var out []Task
	for rows.Next() {
		t, e := scanTask(rows)
		if e != nil {
			return nil, e
		}
		out = append(out, t)
	}
	return out, rows.Err()
}
func (s *Store) Backup(dir string) error {
	if e := os.MkdirAll(dir, 0700); e != nil {
		return e
	}
	path := filepath.Join(dir, fmt.Sprintf("jarvis-%d.db", time.Now().UnixNano()))
	_, e := s.Exec("VACUUM INTO ?", path)
	return e
}

func (s *Store) GetTask(id string) (Task, error) {
	return scanTask(s.QueryRow("SELECT "+taskColumns+" FROM tasks WHERE id=?", id))
}
