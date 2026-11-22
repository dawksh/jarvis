package jarvis

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

func (s *Store) Dispatch(c Config) error {
	var id int64
	var payload string
	e := s.QueryRow("SELECT id,payload FROM updates WHERE dispatched=0 ORDER BY id LIMIT 1").Scan(&id, &payload)
	if e == sql.ErrNoRows {
		return nil
	}
	if e != nil {
		return e
	}
	var u Update
	if e = json.Unmarshal([]byte(payload), &u); e != nil {
		return e
	}
	return s.Atomic(func(tx *sql.Tx) error {
		if _, e := tx.Exec("UPDATE updates SET dispatched=1 WHERE id=?", id); e != nil {
			return e
		}
		m := u.Message
		if m == nil || m.From.ID != c.Owner || m.Chat.ID != c.Chat || m.Chat.Type != "private" || m.Text == "" {
			return nil
		}
		var conv string
		e := tx.QueryRow("SELECT value FROM meta WHERE key='conversation'").Scan(&conv)
		if e != nil && e != sql.ErrNoRows {
			return e
		}
		if conv == "" {
			conv = ID()
			if _, e = tx.Exec("INSERT INTO conversations(id) VALUES(?)", conv); e != nil {
				return e
			}
			if _, e = tx.Exec("INSERT INTO meta(key,value) VALUES('conversation',?)", conv); e != nil {
				return e
			}
		}
		if _, e = tx.Exec("INSERT INTO messages VALUES(?,?,?,?,?)", ID(), conv, "user", m.Text, time.Now().Unix()); e != nil {
			return e
		}
		var task, status string
		e = tx.QueryRow("SELECT id,status FROM tasks WHERE status NOT IN ('completed','failed','cancelled') ORDER BY CASE WHEN status IN ('running','reconciling','pausing','cancelling') THEN 0 ELSE 1 END,created,rowid LIMIT 1").Scan(&task, &status)
		if e != nil && e != sql.ErrNoRows {
			return e
		}
		fields := strings.Fields(m.Text)
		command := fields[0]
		switch strings.ToLower(strings.TrimSpace(m.Text)) {
		case "stop", "cancel", "stop what you're doing", "stop what you’re doing":
			command = "/cancel"
		case "pause":
			command = "/pause"
		}
		reply := ""
		switch command {
		case "/start", "/help":
			reply = "Send a task, or use /task <instruction> to queue separate work. /followup <instruction> continues the latest task. /status /tasks /logs /cancel /pause /resume /new /model auto|light|standard|heavy /servers"
		case "/status", "/tasks":
			rows, e := tx.Query("SELECT id,status,substr(instruction,1,120) FROM tasks ORDER BY created DESC,rowid DESC LIMIT 10")
			if e != nil {
				return e
			}
			for rows.Next() {
				var a, b, d string
				if e = rows.Scan(&a, &b, &d); e != nil {
					rows.Close()
					return e
				}
				reply += fmt.Sprintf("%s %s: %s\n", a, b, d)
			}
			e = rows.Err()
			rows.Close()
			if e != nil {
				return e
			}
			if reply == "" {
				reply = "No tasks."
			}
		case "/servers":
			b, _ := json.Marshal(c.Servers)
			reply = string(b)
		case "/logs":
			reply = "Execution artifacts are available through jarvis task inspect <id>. Raw logs stay on the host to avoid exposing secrets."
		case "/new":
			conv = ID()
			if _, e = tx.Exec("INSERT INTO conversations(id) VALUES(?)", conv); e != nil {
				return e
			}
			if _, e = tx.Exec("UPDATE meta SET value=? WHERE key='conversation'", conv); e != nil {
				return e
			}
			reply = "New conversation. Existing tasks remain scheduled."
		case "/model":
			if len(fields) == 2 && (fields[1] == "auto" || c.Tiers[fields[1]].Model != "") {
				_, e = tx.Exec("UPDATE conversations SET model_override=? WHERE id=?", fields[1], conv)
				if e != nil {
					return e
				}
				reply = "Model default: " + fields[1]
			} else {
				var v string
				if e = tx.QueryRow("SELECT model_override FROM conversations WHERE id=?", conv).Scan(&v); e != nil {
					return e
				}
				reply = "Model default: " + v + ". Set /model auto|light|standard|heavy for future tasks."
			}
		case "/cancel", "/pause":
			if task == "" {
				reply = "No active task."
				break
			}
			next := "cancelled"
			if command == "/pause" {
				next = "paused"
			}
			if status == "running" || status == "reconciling" || status == "cancelling" || status == "pausing" {
				if command == "/pause" {
					next = "pausing"
				} else {
					next = "cancelling"
				}
			}
			if _, e = tx.Exec("UPDATE tasks SET status=?,version=version+1 WHERE id=?", next, task); e != nil {
				return e
			}
			reply = "Task " + task + ": " + next
		case "/resume":
			if task == "" {
				reply = "No task to resume."
				break
			}
			if status != "paused" && status != "waiting_user" {
				reply = "Task is not paused or waiting for input."
				break
			}
			var active int
			if e = tx.QueryRow("SELECT count(*) FROM attempts WHERE status IN ('claimed','running')").Scan(&active); e != nil {
				return e
			}
			if active > 0 {
				reply = "Execution identity is unresolved. Stop and reconcile it before resuming."
				break
			}
			if _, e = tx.Exec("UPDATE tasks SET status='queued',version=version+1 WHERE id=?", task); e != nil {
				return e
			}
			reply = "Queued " + task
		default:
			text := m.Text
			if command == "/followup" {
				if task == "" {
					_ = tx.QueryRow("SELECT id,status FROM tasks WHERE conversation_id=? ORDER BY created DESC,rowid DESC LIMIT 1", conv).Scan(&task, &status)
				}
				if task == "" {
					reply = "No task to continue."
					break
				}
				text = strings.TrimSpace(strings.TrimPrefix(text, command))
			}
			if command == "/task" {
				task = ""
				text = strings.TrimSpace(strings.TrimPrefix(text, command))
			} else if strings.HasPrefix(command, "/") && command != "/followup" {
				reply = "Unknown command. Use /help."
				break
			}
			if text == "" {
				reply = "Include an instruction."
				break
			}
			// Plain messages attach only to a task in the current conversation.
			if task != "" {
				var ownerConv string
				if e = tx.QueryRow("SELECT conversation_id FROM tasks WHERE id=?", task).Scan(&ownerConv); e != nil {
					return e
				}
				if ownerConv != conv {
					task = ""
				}
			}
			if task != "" {
				if status == "waiting_user" {
					if _, e = tx.Exec("UPDATE approval_requests SET response=? WHERE task_id=? AND response IS NULL AND instruction_version=(SELECT version FROM tasks WHERE id=?)", text, task, task); e != nil {
						return e
					}
				}
				if _, e = tx.Exec("INSERT INTO instructions(task_id,content) VALUES(?,?)", task, text); e != nil {
					return e
				}
				if _, e = tx.Exec("UPDATE tasks SET version=version+1,status=CASE WHEN status IN ('waiting_user','completed','failed','cancelled') THEN 'queued' ELSE status END WHERE id=?", task); e != nil {
					return e
				}
				reply = "Follow-up saved for " + task
				break
			}
			var count int
			if e = tx.QueryRow("SELECT count(*) FROM tasks WHERE status NOT IN ('completed','failed','cancelled')").Scan(&count); e != nil {
				return e
			}
			if count >= c.MaxQueue {
				return notify(tx, "Queue full. Your message is saved; submit it again when capacity is available.")
			}
			task = ID()
			if _, e = tx.Exec("INSERT INTO tasks(id,conversation_id,instruction,status,override,created) SELECT ?,id,?,'queued',model_override,? FROM conversations WHERE id=?", task, text, time.Now().Unix(), conv); e != nil {
				return e
			}
			reply = "Accepted task " + task
		}
		return notify(tx, reply)
	})
}
