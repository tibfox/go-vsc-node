package main

// review6 M9: rate-limiter no longer trusts X-Forwarded-For.
//
// The prior clientIP() preferred XFF's first entry, which is attacker-
// controlled. Rotating XFF per request bypassed the per-IP throttle entirely
// (every request hit a fresh bucket). The fix returns RemoteAddr's host
// component only, with no XFF lookup.
//
// This file drives:
//
//   - clientIP semantics: XFF is IGNORED, RemoteAddr's host is returned;
//   - bucket identity: two requests with the same RemoteAddr and different
//     XFF resolve to the same key and the same rate.Limiter instance;
//   - throttling: burst of depositReqBurst (5) is allowed, the 6th request
//     is rejected, and after ~1s of refill the limiter allows again.

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// TestReview6_M9_ClientIP_IgnoresXForwardedFor asserts the host extracted by
// clientIP is RemoteAddr only — not the XFF header. The XFF value here is a
// spoofed RFC1918 address; if clientIP regressed to trusting XFF, the test
// would observe "10.0.0.1" or "10.0.0.42" instead of "203.0.113.7".
func TestReview6_M9_ClientIP_IgnoresXForwardedFor(t *testing.T) {
	cases := []struct {
		name       string
		remoteAddr string
		xff        string
		wantHost   string
	}{
		{
			name:       "xff_present_remoteaddr_with_port",
			remoteAddr: "203.0.113.7:54321",
			xff:        "10.0.0.1, 10.0.0.2",
			wantHost:   "203.0.113.7",
		},
		{
			name:       "xff_single_value_remoteaddr_v6",
			remoteAddr: "[2001:db8::1]:8443",
			xff:        "10.0.0.42",
			wantHost:   "2001:db8::1",
		},
		{
			name:       "xff_empty",
			remoteAddr: "198.51.100.5:1234",
			xff:        "",
			wantHost:   "198.51.100.5",
		},
		{
			name:       "remoteaddr_no_port_falls_back",
			remoteAddr: "192.0.2.1",
			xff:        "10.0.0.99",
			wantHost:   "192.0.2.1", // SplitHostPort errors → return RemoteAddr as-is
		},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "http://example/", nil)
			req.RemoteAddr = c.remoteAddr
			if c.xff != "" {
				req.Header.Set("X-Forwarded-For", c.xff)
			}
			if got := clientIP(req); got != c.wantHost {
				t.Fatalf("clientIP() = %q, want %q (XFF=%q must be ignored)", got, c.wantHost, c.xff)
			}
		})
	}
}

// TestReview6_M9_SameRemoteAddrSameBucket builds two requests with the same
// RemoteAddr but different XFF values, runs them through the rate limiter,
// and asserts the second one consumes from the same bucket as the first —
// i.e. they share state. With the pre-fix XFF-trusting clientIP, each XFF
// value minted a fresh bucket and the throttle did not apply.
func TestReview6_M9_SameRemoteAddrSameBucket(t *testing.T) {
	m := newRateLimiterMap()
	defer close(m.stopCh)

	mkReq := func(xff string) *http.Request {
		req := httptest.NewRequest(http.MethodGet, "http://example/", nil)
		req.RemoteAddr = "203.0.113.50:33333"
		if xff != "" {
			req.Header.Set("X-Forwarded-For", xff)
		}
		return req
	}

	// Drain the burst (5) with rotating XFF — each call must hit the same
	// bucket, so all 5 succeed and the 6th is denied. With the prior
	// XFF-trusting key, every call would mint a fresh bucket and the 6th
	// would still succeed.
	for i := 0; i < depositReqBurst; i++ {
		req := mkReq("10.0.0." + itoa(i))
		ip := clientIP(req)
		if !m.allow(ip) {
			t.Fatalf("burst request %d denied early — bucket mis-keyed", i+1)
		}
	}

	req := mkReq("10.0.0.99")
	ip := clientIP(req)
	if m.allow(ip) {
		t.Fatalf("6th burst request from same RemoteAddr was allowed — XFF rotation appears to mint new buckets")
	}
}

// TestReview6_M9_ThrottlingExhaustsAndRefills exercises the full bucket
// lifecycle. depositReqBurst (5) immediate requests must all pass; the 6th
// must be denied; after a 1-second sleep (rate = 1 req/sec) the limiter
// refills enough for one more request.
func TestReview6_M9_ThrottlingExhaustsAndRefills(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping rate-refill timing test in -short mode")
	}

	m := newRateLimiterMap()
	defer close(m.stopCh)

	ip := "198.51.100.77"

	// Burst — should all succeed.
	for i := 0; i < depositReqBurst; i++ {
		if !m.allow(ip) {
			t.Fatalf("burst request %d denied (burst=%d)", i+1, depositReqBurst)
		}
	}

	// Bucket exhausted — 6th must be denied.
	if m.allow(ip) {
		t.Fatalf("6th request was allowed — bucket appears unlimited")
	}

	// Wait for refill — depositReqRate is 1 req/sec, so 1.1s yields ~1.1
	// tokens. Even with scheduler jitter, one further allow() must succeed.
	time.Sleep(1100 * time.Millisecond)
	if !m.allow(ip) {
		t.Fatalf("after ~1.1s wait, refilled bucket still denied the request")
	}
}

// itoa is the smallest non-allocating int-to-string helper local tests need;
// strconv.Itoa would also work but the dependency footprint here is zero.
func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var buf [4]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[i:])
}
