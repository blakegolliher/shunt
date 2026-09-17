package telemetry

import (
	"bytes"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"
)

func TestConsoleHandlerLine(t *testing.T) {
	var buf bytes.Buffer
	log := slog.New(NewConsoleHandler(&buf, ConsoleOptions{Tag: "PLAINTEXT"}))
	log.With("cluster", "vast01").Info("cluster", "endpoints", []string{"10.0.0.1:80", "10.0.0.2:80"}, "note", "has spaces", "n", 3)
	log.Warn("upstream scheme is http")
	log.Debug("not shown")
	lines := strings.Split(strings.TrimSuffix(buf.String(), "\n"), "\n")
	if len(lines) != 2 {
		t.Fatalf("want 2 lines, got %d:\n%s", len(lines), buf.String())
	}
	first := lines[0]
	if _, err := time.Parse("15:04:05", first[:8]); err != nil {
		t.Errorf("line does not start with HH:MM:SS: %q", first)
	}
	if want := ` INFO  [PLAINTEXT] cluster  cluster=vast01 endpoints=10.0.0.1:80,10.0.0.2:80 note="has spaces" n=3`; first[8:] != want {
		t.Errorf("line:\n got %q\nwant %q", first[8:], want)
	}
	if want := ` WARN  [PLAINTEXT] upstream scheme is http`; lines[1][8:] != want {
		t.Errorf("line:\n got %q\nwant %q", lines[1][8:], want)
	}
}

func TestConsoleHandlerGroupsAndEmpty(t *testing.T) {
	var buf bytes.Buffer
	log := slog.New(NewConsoleHandler(&buf, ConsoleOptions{Level: slog.LevelDebug}))
	log.WithGroup("req").Debug("done", "status", 200, slog.Group("up", "cluster", "a"), "empty", "")
	got := buf.String()[8:]
	if want := " DEBUG done  req.status=200 req.up.cluster=a req.empty=\"\"\n"; got != want {
		t.Errorf("line:\n got %q\nwant %q", got, want)
	}
}

func BenchmarkConsoleHandler(b *testing.B) {
	log := slog.New(NewConsoleHandler(io.Discard, ConsoleOptions{Tag: "PLAINTEXT"})).With("cluster", "vast01")
	b.ReportAllocs()
	for b.Loop() {
		log.Info("directory reloaded", "trigger", "poll", "version", 12)
	}
}
