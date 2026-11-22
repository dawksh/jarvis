package jarvis

import (
	"database/sql"
	"fmt"
	"golang.org/x/sys/unix"
	"os"
	"path/filepath"
	"time"
)

func diskAvailable(dir string) error {
	var stat unix.Statfs_t
	if e := unix.Statfs(dir, &stat); e != nil {
		return e
	}
	if uint64(stat.Bavail)*uint64(stat.Bsize) < 256<<20 {
		return fmt.Errorf("less than 256 MiB free in %s", dir)
	}
	return nil
}
func (s *Service) Maintain() error {
	if e := diskAvailable(s.Config.DataDir); e != nil {
		return s.Store.Atomic(func(tx *sql.Tx) error {
			return notify(tx, "Low state disk space; new executions are suspended. Free space on U1.")
		})
	}
	var previous int64
	_ = s.Store.QueryRow("SELECT value FROM meta WHERE key='backup'").Scan(&previous)
	if time.Now().Unix()-previous >= 86400 {
		if e := s.Store.Backup(filepath.Join(s.Config.DataDir, "backups")); e != nil {
			return e
		}
		if _, e := s.Store.Exec("INSERT INTO meta(key,value) VALUES('backup',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", time.Now().Unix()); e != nil {
			return e
		}
	}
	var integrity string
	if e := s.Store.QueryRow("PRAGMA quick_check").Scan(&integrity); e != nil {
		return e
	}
	if integrity != "ok" {
		return fmt.Errorf("SQLite integrity check failed: %s", integrity)
	}
	cutoff := time.Now().Add(-14 * 24 * time.Hour)
	rows, e := s.Store.Query("SELECT a.id FROM attempts a JOIN tasks t ON t.id=a.task_id WHERE a.status='finished' AND a.created<? AND t.status IN ('completed','failed','cancelled')", cutoff.Unix())
	if e != nil {
		return e
	}
	var ids []string
	for rows.Next() {
		var id string
		if e = rows.Scan(&id); e != nil {
			rows.Close()
			return e
		}
		ids = append(ids, id)
	}
	e = rows.Err()
	rows.Close()
	if e != nil {
		return e
	}
	for _, id := range ids {
		if e = os.RemoveAll(s.Supervisor.Dir(id)); e != nil {
			return e
		}
	}
	files, e := filepath.Glob(filepath.Join(s.Config.DataDir, "backups", "jarvis-*.db"))
	if e != nil {
		return e
	}
	for _, p := range files {
		info, e := os.Stat(p)
		if e != nil {
			return e
		}
		if info.ModTime().Before(cutoff) {
			if e = os.Remove(p); e != nil {
				return e
			}
		}
	}
	return nil
}
