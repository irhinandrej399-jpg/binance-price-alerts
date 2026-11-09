package clientip

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestTrustedPeer(t *testing.T) {
	for _, tc := range []struct {
		peer, header, want string
		status             int
	}{
		{"172.29.75.2:10", "192.0.2.1", "192.0.2.1:0", 204},
		{"172.29.75.2:10", "192.0.2.2", "192.0.2.2:0", 204},
		{"172.29.75.2:10", "::ffff:192.0.2.1", "192.0.2.1:0", 204},
		{"172.29.75.2:10", "2001:db8::1", "[2001:db8::1]:0", 204},
		{"172.29.75.3:10", "192.0.2.1", "172.29.75.3:10", 204},
		{"172.29.75.2:10", "", "", 400},
		{"172.29.75.2:10", "192.0.2.1, 192.0.2.2", "", 400},
		{"172.29.75.2:10", "fe80::1%en0", "", 400},
	} {
		t.Run(tc.peer+tc.header, func(t *testing.T) {
			h, err := Handler("172.29.75.2", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.RemoteAddr != tc.want {
					t.Errorf("peer %q want %q", r.RemoteAddr, tc.want)
				}
				w.WriteHeader(204)
			}))
			if err != nil {
				t.Fatal(err)
			}
			r := httptest.NewRequest("POST", "/", nil)
			r.RemoteAddr = tc.peer
			r.Header.Set("X-Real-IP", tc.header)
			r.Header.Set("X-Forwarded-For", "198.51.100.99")
			r.Header.Set("Forwarded", "for=198.51.100.99")
			w := httptest.NewRecorder()
			h.ServeHTTP(w, r)
			if w.Code != tc.status {
				t.Fatalf("status %d want %d", w.Code, tc.status)
			}
		})
	}
}
func TestConfigurationAndDuplicate(t *testing.T) {
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(204) })
	for _, ip := range []string{"0.0.0.0/0", "proxy", "fe80::1%en0", "0.0.0.0"} {
		if _, err := Handler(ip, next); err == nil {
			t.Errorf("accepted %q", ip)
		}
	}
	h, err := Handler("", next)
	if err != nil {
		t.Fatal(err)
	}
	r := httptest.NewRequest("GET", "/", nil)
	r.Header.Set("X-Real-IP", "garbage")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != 204 {
		t.Fatal(w.Code)
	}
	h, err = Handler("172.29.75.2", next)
	if err != nil {
		t.Fatal(err)
	}
	r.RemoteAddr = "172.29.75.2:1"
	r.Header["X-Real-Ip"] = []string{"192.0.2.1", "192.0.2.2"}
	w = httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != 400 {
		t.Fatal(w.Code)
	}
}
