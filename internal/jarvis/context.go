package jarvis

import (
	"fmt"
	"strings"
)

func (s *Store) Context(conversation, task string) (string, error) {
	var summary string
	if e := s.QueryRow("SELECT summary FROM conversations WHERE id=?", conversation).Scan(&summary); e != nil {
		return "", e
	}
	rows, e := s.Query("SELECT role,substr(content,1,2000) FROM (SELECT rowid,role,content FROM messages WHERE conversation_id=? ORDER BY rowid DESC LIMIT 8) ORDER BY rowid", conversation)
	if e != nil {
		return "", e
	}
	defer rows.Close()
	var out strings.Builder
	if len(summary) > 4000 {
		summary = summary[:4000]
	}
	out.WriteString("Summary: " + summary)
	for rows.Next() {
		var role, text string
		if e = rows.Scan(&role, &text); e != nil {
			return "", e
		}
		fmt.Fprintf(&out, "\n%s: %s", role, text)
	}
	return out.String(), rows.Err()
}
