package jarvis

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

type Update struct {
	ID      int64 `json:"update_id"`
	Message *struct {
		ID   int64 `json:"message_id"`
		From struct {
			ID int64 `json:"id"`
		} `json:"from"`
		Chat struct {
			ID   int64  `json:"id"`
			Type string `json:"type"`
		} `json:"chat"`
		Text string `json:"text"`
	} `json:"message"`
}
type Telegram struct {
	Token   string
	Client  *http.Client
	BaseURL string
}

func (t Telegram) Call(ctx context.Context, method string, body any, out any) error {
	b, e := json.Marshal(body)
	if e != nil {
		return e
	}
	base := t.BaseURL
	if base == "" {
		base = "https://api.telegram.org"
	}
	req, e := http.NewRequestWithContext(ctx, "POST", base+"/bot"+t.Token+"/"+method, bytes.NewReader(b))
	if e != nil {
		return fmt.Errorf("telegram request construction failed")
	}
	req.Header.Set("Content-Type", "application/json")
	client := t.Client
	if client == nil {
		client = &http.Client{Timeout: 40 * time.Second}
	}
	resp, e := client.Do(req)
	if e != nil {
		return fmt.Errorf("telegram transport failed")
	}
	defer resp.Body.Close()
	var envelope struct {
		Description string          `json:"description"`
		OK          bool            `json:"ok"`
		Result      json.RawMessage `json:"result"`
	}
	if e = json.NewDecoder(io.LimitReader(resp.Body, 2<<20)).Decode(&envelope); e != nil {
		return fmt.Errorf("telegram response invalid")
	}
	if !envelope.OK {
		if method == "editMessageText" && strings.Contains(envelope.Description, "message is not modified") {
			return nil
		}
		return fmt.Errorf("telegram %s failed (HTTP %d)", method, resp.StatusCode)
	}
	if out != nil {
		return json.Unmarshal(envelope.Result, out)
	}
	return nil
}
func (t Telegram) Poll(ctx context.Context, s *Store, c Config) error {
	var updates []Update
	if e := t.Call(ctx, "getUpdates", map[string]any{"offset": s.Offset(), "timeout": 25, "allowed_updates": []string{"message"}}, &updates); e != nil {
		return e
	}
	for _, u := range updates {
		if e := s.Intake(u.ID, u); e != nil {
			return e
		}
	}
	return nil
}
