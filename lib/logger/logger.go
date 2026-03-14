package logger

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
)

//Future note: logging should support smart contract debug logs which can be stored or processed by the developer

type Logger interface {
	Debug(log ...any)
	Error(log ...any)
}

type PrefixedLogger struct {
	Prefix string
}

func (pl PrefixedLogger) Debug(log ...any) {
	fmt.Println("[Prefix: "+pl.Prefix+"] Debug:", log)
}

func (pl PrefixedLogger) Error(log ...any) {
	fmt.Println("[Prefix: "+pl.Prefix+"] Error: ", log)
}

var _ Logger = &PrefixedLogger{}

// PrintfHandler is an slog.Handler that outputs log lines in plain
// printf style:
//
//	[ORACLE] [CHAIN-RELAY] chain relay starting symbol=BTC height=125919
type PrintfHandler struct {
	prefix string
	level  slog.Level
	attrs  []slog.Attr
}

func NewPrintfHandler(prefix string, level slog.Level) *PrintfHandler {
	return &PrintfHandler{prefix: prefix, level: level}
}

func (h *PrintfHandler) Enabled(_ context.Context, level slog.Level) bool {
	return level >= h.level
}

func (h *PrintfHandler) Handle(_ context.Context, r slog.Record) error {
	var b strings.Builder
	b.WriteString(h.prefix)

	levelStr := ""
	switch r.Level {
	case slog.LevelDebug:
		levelStr = "DEBUG"
	case slog.LevelWarn:
		levelStr = "WARN"
	case slog.LevelError:
		levelStr = "ERROR"
	}
	if levelStr != "" {
		b.WriteString(" ")
		b.WriteString(levelStr)
		b.WriteString(":")
	}

	// Promote "symbol" attr to a prefix tag e.g. [BTC]
	var extras []slog.Attr
	r.Attrs(func(a slog.Attr) bool {
		if a.Key == "symbol" {
			fmt.Fprintf(&b, " [%s]", a.Value)
		} else {
			extras = append(extras, a)
		}
		return true
	})

	b.WriteString(" ")
	b.WriteString(r.Message)

	for _, a := range h.attrs {
		fmt.Fprintf(&b, " %s=%v", a.Key, a.Value)
	}
	for _, a := range extras {
		fmt.Fprintf(&b, " %s=%v", a.Key, a.Value)
	}

	fmt.Println(b.String())
	return nil
}

func (h *PrintfHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &PrintfHandler{
		prefix: h.prefix,
		level:  h.level,
		attrs:  append(append([]slog.Attr{}, h.attrs...), attrs...),
	}
}

func (h *PrintfHandler) WithGroup(name string) slog.Handler {
	return &PrintfHandler{
		prefix: h.prefix + " [" + strings.ToUpper(name) + "]",
		level:  h.level,
		attrs:  h.attrs,
	}
}

// NewSlogPrefixed creates a new slog.Logger with printf-style output.
func NewSlogPrefixed(prefix string, level slog.Level) *slog.Logger {
	return slog.New(NewPrintfHandler(prefix, level))
}

// WithSlogPrefix adds an additional bracketed prefix to a logger.
func WithSlogPrefix(l *slog.Logger, sub string) *slog.Logger {
	handler := l.Handler()
	if ph, ok := handler.(*PrintfHandler); ok {
		return slog.New(&PrintfHandler{
			prefix: ph.prefix + " " + sub,
			level:  ph.level,
			attrs:  ph.attrs,
		})
	}
	return l
}
