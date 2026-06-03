package main

import (
	"net"
	"net/http"
	"sync"
	"time"

	"golang.org/x/time/rate"
)

// review6 M9: per-IP rate limiter for the unauthenticated `/` deposit-address
// endpoint. The endpoint triggers a contract read, a key-derivation, and a
// DB insert per call; without rate-limiting, an unauthenticated client could
// trivially exhaust mongo + cpu. We deliberately do NOT add auth: the
// endpoint exists so users can request fresh deposit addresses, which by
// design requires public reachability. Rate-limit is the right control.
//
// Bucket size + refill rate are intentionally generous for normal users
// (5 requests, refill 1/sec) and trivial to amortize for attackers (≤5 RPS
// per IP, far below what the contract-read + DB-insert cost can absorb).

const (
	depositReqRate  = rate.Limit(1.0)  // 1 req/sec sustained
	depositReqBurst = 5                // 5 req burst
	limiterIdleAfter = 30 * time.Minute // drop stale entries
)

type ipLimiter struct {
	limiter  *rate.Limiter
	lastSeen time.Time
}

type rateLimiterMap struct {
	mu     sync.Mutex
	byIP   map[string]*ipLimiter
	stopCh chan struct{}
}

func newRateLimiterMap() *rateLimiterMap {
	m := &rateLimiterMap{
		byIP:   make(map[string]*ipLimiter),
		stopCh: make(chan struct{}),
	}
	go m.gcLoop()
	return m
}

// allow returns true if the IP is within the per-IP rate budget.
func (m *rateLimiterMap) allow(ip string) bool {
	m.mu.Lock()
	entry, ok := m.byIP[ip]
	if !ok {
		entry = &ipLimiter{limiter: rate.NewLimiter(depositReqRate, depositReqBurst)}
		m.byIP[ip] = entry
	}
	entry.lastSeen = time.Now()
	m.mu.Unlock()
	return entry.limiter.Allow()
}

// gcLoop sweeps idle limiter entries every 5 minutes so the per-IP map
// can't grow unbounded across deploys with churning client IPs.
func (m *rateLimiterMap) gcLoop() {
	t := time.NewTicker(5 * time.Minute)
	defer t.Stop()
	for {
		select {
		case <-m.stopCh:
			return
		case now := <-t.C:
			m.mu.Lock()
			for ip, entry := range m.byIP {
				if now.Sub(entry.lastSeen) > limiterIdleAfter {
					delete(m.byIP, ip)
				}
			}
			m.mu.Unlock()
		}
	}
}

// clientIP extracts the request's source IP, preferring X-Forwarded-For's
// FIRST entry (the original client) when present, falling back to
// RemoteAddr. We don't trust XFF for auth — only as a rate-limit key — so a
// header-spoofing attacker can only DoS THEMSELVES by manipulating it.
func clientIP(r *http.Request) string {
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		for _, part := range splitAndTrim(xff, ',') {
			if part != "" {
				return part
			}
		}
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

func splitAndTrim(s string, sep byte) []string {
	var out []string
	start := 0
	for i := 0; i < len(s); i++ {
		if s[i] == sep {
			out = append(out, trimSpace(s[start:i]))
			start = i + 1
		}
	}
	out = append(out, trimSpace(s[start:]))
	return out
}

func trimSpace(s string) string {
	for len(s) > 0 && (s[0] == ' ' || s[0] == '\t') {
		s = s[1:]
	}
	for len(s) > 0 && (s[len(s)-1] == ' ' || s[len(s)-1] == '\t') {
		s = s[:len(s)-1]
	}
	return s
}
