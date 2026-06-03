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

// clientIP extracts the request's source IP from the TCP-layer RemoteAddr
// only. review6 M9 (adversarial-review correction): the prior version
// preferred X-Forwarded-For's first entry, which is fully attacker-
// controlled. By using the spoofed XFF as the rate-limit key, an attacker
// rotated the key per request and bypassed the per-IP throttle entirely.
//
// We deliberately do NOT consult XFF here. If this bot is deployed behind
// a trusted reverse proxy that rewrites the source IP via XFF, the proxy
// must rewrite RemoteAddr (e.g. via http.Server's PROXY-protocol handler
// or by replacing the listener) — not via header. Trusting XFF without
// a trusted-proxy allowlist is the documented anti-pattern.
//
// If a future deployment needs XFF support, gate it behind a config-
// supplied trusted-proxy list and only consult XFF when RemoteAddr
// matches one of those proxies. That's the standard pattern; until then,
// RemoteAddr is the only safe rate-limit key.
func clientIP(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}
