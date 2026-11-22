package jarvis

import (
	"fmt"
	"gopkg.in/yaml.v3"
	"os"
	"path/filepath"
	"strings"
	"time"
)

type Tier struct {
	Model     string `yaml:"model"`
	Reasoning string `yaml:"reasoning"`
}
type Server struct {
	Type     string `yaml:"type"`
	Host     string `yaml:"host"`
	User     string `yaml:"user"`
	Identity string `yaml:"identity_file"`
}
type Config struct {
	DataDir     string            `yaml:"data_dir"`
	WorkDir     string            `yaml:"work_dir"`
	Owner       int64             `yaml:"owner_id"`
	Chat        int64             `yaml:"chat_id"`
	Codex       string            `yaml:"codex"`
	Backend     string            `yaml:"backend"`
	Timeout     time.Duration     `yaml:"timeout"`
	MaxAttempts int               `yaml:"max_attempts"`
	MaxQueue    int               `yaml:"max_queue"`
	JevEndpoint string            `yaml:"jev_endpoint"`
	JevModel    string            `yaml:"jev_model"`
	Confidence  float64           `yaml:"confidence"`
	Tiers       map[string]Tier   `yaml:"tiers"`
	Servers     map[string]Server `yaml:"servers"`
}

func LoadConfig(path string) (Config, error) {
	c := Config{Codex: "codex", Backend: "systemd", Timeout: 2 * time.Hour, MaxAttempts: 5, MaxQueue: 50, JevEndpoint: "https://api.typesafe.ai/v1/systemone", JevModel: "jev-latest", Confidence: .7}
	b, e := os.ReadFile(path)
	if e != nil {
		return c, e
	}
	d := yaml.NewDecoder(strings.NewReader(os.ExpandEnv(string(b))))
	d.KnownFields(true)
	if e = d.Decode(&c); e != nil {
		return c, e
	}
	if !filepath.IsAbs(c.DataDir) || !filepath.IsAbs(c.WorkDir) || c.DataDir == c.WorkDir {
		return c, fmt.Errorf("data_dir and work_dir must be distinct absolute paths")
	}
	if strings.HasPrefix(c.DataDir+"/", c.WorkDir+"/") {
		return c, fmt.Errorf("data_dir must be outside work_dir")
	}
	if c.Owner <= 0 || c.Chat <= 0 {
		return c, fmt.Errorf("owner_id and private chat_id must be positive")
	}
	if c.Backend != "systemd" && c.Backend != "local" {
		return c, fmt.Errorf("backend must be systemd or local")
	}
	if c.Timeout <= 0 || c.MaxAttempts < 1 || c.MaxQueue < 1 || c.Confidence < 0 || c.Confidence > 1 {
		return c, fmt.Errorf("invalid limits")
	}
	for _, k := range []string{"light", "standard", "heavy"} {
		t := c.Tiers[k]
		if t.Model == "" || strings.ContainsAny(t.Model, " \n\t") {
			return c, fmt.Errorf("configure a verified model for tier %s", k)
		}
		if t.Reasoning != "low" && t.Reasoning != "medium" && t.Reasoning != "high" {
			return c, fmt.Errorf("invalid reasoning for %s", k)
		}
	}
	for k, s := range c.Servers {
		if s.Type != "local" && s.Type != "ssh" {
			return c, fmt.Errorf("invalid server %s", k)
		}
		if s.Type == "ssh" && (s.Host == "" || s.User == "" || !filepath.IsAbs(s.Identity)) {
			return c, fmt.Errorf("incomplete SSH server %s", k)
		}
	}
	return c, nil
}
