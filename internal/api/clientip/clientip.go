// Package clientip implements the single, explicitly trusted ingress boundary.
package clientip

import (
	"errors"
	"net"
	"net/http"
	"net/netip"
)

// Handler accepts X-Real-IP only from the configured literal TCP peer. The
// ingress must overwrite it with its own TCP peer, never an incoming header.
// Empty configuration keeps direct-peer mode. Forwarding chains are unsupported.
func Handler(proxy string, next http.Handler) (http.Handler, error) {
	if proxy == "" {
		return next, nil
	}
	trusted, err := netip.ParseAddr(proxy)
	if err != nil || trusted.Zone() != "" || trusted.IsUnspecified() || trusted.IsMulticast() {
		return nil, errors.New("clientip: invalid trusted proxy")
	}
	trusted = trusted.Unmap()
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		host, _, err := net.SplitHostPort(r.RemoteAddr)
		peer, e := netip.ParseAddr(host)
		if err == nil && e == nil && peer.Unmap() == trusted {
			values := r.Header.Values("X-Real-IP")
			var ip netip.Addr
			if len(values) == 1 {
				ip, _ = netip.ParseAddr(values[0])
			}
			if !ip.IsValid() || ip.Zone() != "" || ip.IsUnspecified() || ip.IsMulticast() {
				w.Header().Set("Content-Type", "application/json")
				w.Header().Set("Cache-Control", "no-store")
				w.WriteHeader(400)
				_, _ = w.Write([]byte("{\"error\":{\"code\":\"invalid_request\"}}\n"))
				return
			}
			r = r.Clone(r.Context())
			r.RemoteAddr = net.JoinHostPort(ip.Unmap().String(), "0")
		}
		next.ServeHTTP(w, r)
	}), nil
}
