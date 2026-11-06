package main

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"testing"
	"time"
)

func TestHTTPDrain(t *testing.T) {
	for _, force := range []bool{false, true} {
		t.Run(map[bool]string{false: "complete", true: "deadline"}[force], func(t *testing.T) {
			l, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			entered, release, finished := make(chan struct{}), make(chan struct{}), make(chan struct{})
			server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				close(entered)
				defer close(finished)
				select {
				case <-release:
					w.Write([]byte("committed"))
				case <-r.Context().Done():
				}
			})}
			done := make(chan error, 1)
			go func() { done <- serveHTTP(ctx, server, l, func() {}, 200*time.Millisecond) }()
			response := make(chan string, 1)
			go func() {
				resp, e := http.Get("http://" + l.Addr().String())
				if e != nil {
					response <- ""
					return
				}
				defer resp.Body.Close()
				b, _ := io.ReadAll(resp.Body)
				response <- string(b)
			}()
			<-entered
			cancel()
			// Listener must close before the in-flight request finishes.
			deadline := time.Now().Add(time.Second)
			for {
				c, e := net.DialTimeout("tcp", l.Addr().String(), 20*time.Millisecond)
				if e != nil {
					break
				}
				c.Close()
				if time.Now().After(deadline) {
					t.Fatal("still accepting")
				}
				time.Sleep(time.Millisecond)
			}
			select {
			case <-done:
				t.Fatal("did not wait for request")
			default:
			}
			if !force {
				close(release)
			}
			select {
			case e := <-done:
				if force && !errors.Is(e, context.DeadlineExceeded) {
					t.Fatalf("deadline: %v", e)
				}
				if !force && !errors.Is(e, context.Canceled) {
					t.Fatalf("drain: %v", e)
				}
			case <-time.After(time.Second):
				t.Fatal("unbounded shutdown")
			}
			<-finished
			if b := <-response; !force && b != "committed" {
				t.Fatalf("lost response: %q", b)
			}
		})
	}
}

func TestShutdownFailureIsNotMasked(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := supervise(ctx, func(c context.Context) error { return c.Err() }, func(c context.Context) error { return context.DeadlineExceeded })
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("masked shutdown timeout: %v", err)
	}
}
