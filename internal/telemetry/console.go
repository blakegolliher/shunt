package telemetry

import (
	"context"
	"io"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"time"
)

// ConsoleHandler writes one short line per record for a person watching a terminal:
//
//	18:48:31 INFO  [PLAINTEXT] cluster  cluster=vast01 endpoint=http://10.0.0.1:80
//
// Local time to the second, the level padded to five columns, an optional tag, the message, then
// key=value pairs in the order they were added, values quoted only when they contain spaces or
// quotes. It is for eyes; JSON (telemetry.log_format: json) stays the format for machines.
type ConsoleHandler struct {
	mu     *sync.Mutex
	w      io.Writer
	level  slog.Leveler
	tag    string
	prefix string // attrs from WithAttrs, pre-rendered
	group  string // dotted group prefix from WithGroup
}

// ConsoleOptions configure a ConsoleHandler.
type ConsoleOptions struct {
	Level slog.Leveler // default Info
	Tag   string       // shown in brackets after the level on every line, e.g. PLAINTEXT
}

// NewConsoleHandler returns a handler that writes console lines to w.
func NewConsoleHandler(w io.Writer, o ConsoleOptions) *ConsoleHandler {
	level := o.Level
	if level == nil {
		level = slog.LevelInfo
	}
	return &ConsoleHandler{mu: &sync.Mutex{}, w: w, level: level, tag: o.Tag}
}

// Enabled reports whether l is at or above the handler's level.
func (h *ConsoleHandler) Enabled(_ context.Context, l slog.Level) bool {
	return l >= h.level.Level()
}

// Handle writes r as one line.
func (h *ConsoleHandler) Handle(_ context.Context, r slog.Record) error {
	b := make([]byte, 0, 256)
	t := r.Time
	if t.IsZero() {
		t = time.Now()
	}
	b = t.Local().AppendFormat(b, "15:04:05")
	b = append(b, ' ')
	lv := r.Level.String()
	b = append(b, lv...)
	for i := len(lv); i < 5; i++ {
		b = append(b, ' ')
	}
	if h.tag != "" {
		b = append(b, " ["...)
		b = append(b, h.tag...)
		b = append(b, ']')
	}
	b = append(b, ' ')
	b = append(b, r.Message...)
	if h.prefix != "" || r.NumAttrs() > 0 {
		b = append(b, ' ')
	}
	b = append(b, h.prefix...)
	r.Attrs(func(a slog.Attr) bool {
		b = appendAttr(b, h.group, a)
		return true
	})
	b = append(b, '\n')
	h.mu.Lock()
	defer h.mu.Unlock()
	_, err := h.w.Write(b)
	return err
}

// WithAttrs returns a handler whose lines carry attrs.
func (h *ConsoleHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	n := *h
	b := []byte(h.prefix)
	for _, a := range attrs {
		b = appendAttr(b, h.group, a)
	}
	n.prefix = string(b)
	return &n
}

// WithGroup returns a handler that prefixes later keys with name.
func (h *ConsoleHandler) WithGroup(name string) slog.Handler {
	if name == "" {
		return h
	}
	n := *h
	n.group = h.group + name + "."
	return &n
}

func appendAttr(b []byte, group string, a slog.Attr) []byte {
	a.Value = a.Value.Resolve()
	if a.Equal(slog.Attr{}) {
		return b
	}
	if a.Value.Kind() == slog.KindGroup {
		g := group
		if a.Key != "" {
			g += a.Key + "."
		}
		for _, ga := range a.Value.Group() {
			b = appendAttr(b, g, ga)
		}
		return b
	}
	b = append(b, ' ')
	b = append(b, group...)
	b = append(b, a.Key...)
	b = append(b, '=')
	var s string
	switch a.Value.Kind() {
	case slog.KindString:
		s = a.Value.String()
	case slog.KindTime:
		s = a.Value.Time().Format(time.RFC3339)
	case slog.KindAny:
		if ss, ok := a.Value.Any().([]string); ok {
			s = strings.Join(ss, ",")
			break
		}
		s = a.Value.String()
	default:
		s = a.Value.String()
	}
	if s == "" || strings.ContainsAny(s, " \t\n\"=") {
		return strconv.AppendQuote(b, s)
	}
	return append(b, s...)
}
