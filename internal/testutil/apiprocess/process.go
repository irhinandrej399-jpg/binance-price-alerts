// Package apiprocess owns a disposable executable's bounded lifecycle.
// It passes only explicit environment variables and discards child output,
// which may contain credentials. Intended for local Unix integration tests.
package apiprocess

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
	"unicode"
)

var (
	ErrConfig    = errors.New("process harness: invalid configuration")
	ErrStart     = errors.New("process harness: executable failed to start")
	ErrEarlyExit = errors.New("process harness: exited before readiness")
	ErrStop      = errors.New("process harness: wait after kill timed out")
)

type Config struct {
	Executable string
	Args       []string
	// StartupProbePath is an origin-local absolute path; empty means /readyz.
	// A successful probe only confirms HTTP 200 at this endpoint.
	StartupProbePath string
	// Env is the entire child environment, not an overlay on the parent.
	// HTTP_ADDR is reserved and supplied by the harness.
	Env            map[string]string
	StartupTimeout time.Duration
	StopTimeout    time.Duration
}
type Process struct {
	URL         string
	cmd         *exec.Cmd
	done        chan struct{}
	stopTimeout time.Duration
	mu          sync.Mutex
	stopped     bool
}

func (p *Process) PID() int              { return p.cmd.Process.Pid }
func (p *Process) Done() <-chan struct{} { return p.done }

// ExitCode observes reaped child metadata without exposing its command/output.
// A signal exit has code -1; ok is false until Done is closed.
func (p *Process) ExitCode() (code int, ok bool) {
	select {
	case <-p.done:
		return p.cmd.ProcessState.ExitCode(), true
	default:
		return 0, false
	}
}

// Stop sends TERM to the process group, then KILL, and always reaps the child.
// Both waits have a finite budget. Repeated/concurrent calls are safe.
func (p *Process) Stop() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.stopped {
		return nil
	}
	defer func() { _ = syscall.Kill(-p.PID(), syscall.SIGKILL) }()
	select {
	case <-p.done:
		p.stopped = true
		return nil
	default:
	}
	_ = syscall.Kill(-p.PID(), syscall.SIGTERM)
	timer := time.NewTimer(p.stopTimeout)
	defer timer.Stop()
	select {
	case <-p.done:
		p.stopped = true
		return nil
	case <-timer.C:
	}
	_ = syscall.Kill(-p.PID(), syscall.SIGKILL)
	timer.Reset(p.stopTimeout)
	select {
	case <-p.done:
		p.stopped = true
		return nil
	case <-timer.C:
		return ErrStop
	}
}

// With starts cfg.Executable, probes StartupProbePath (default /readyz), invokes check, and stops/reaps on
// all return paths (also panic/Goexit). check must respect ctx for its own I/O.
// Context cancellation independently stops the child after readiness too.
func With(ctx context.Context, cfg Config, check func(*Process) error) (err error) {
	if cfg.Executable == "" || cfg.StartupTimeout <= 0 || cfg.StopTimeout <= 0 || check == nil {
		return ErrConfig
	}
	probePath := cfg.StartupProbePath
	if probePath == "" {
		probePath = "/readyz"
	}
	if !validProbePath(probePath) {
		return ErrConfig
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	listener, e := net.Listen("tcp4", "127.0.0.1:0")
	if e != nil {
		return ErrStart
	}
	addr := listener.Addr().String()
	listener.Close()
	cmd := exec.Command(cfg.Executable, cfg.Args...)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Env = []string{"HTTP_ADDR=" + addr}
	for k, v := range cfg.Env {
		if k == "HTTP_ADDR" {
			return ErrConfig
		}
		cmd.Env = append(cmd.Env, k+"="+v)
	}
	if cmd.Start() != nil {
		return ErrStart
	} // Never include argv/env/raw exec errors.
	p := &Process{URL: "http://" + addr, cmd: cmd, done: make(chan struct{}), stopTimeout: cfg.StopTimeout}
	go func() { _ = cmd.Wait(); close(p.done) }()
	// Optional runner-owned registry lets infrastructure cleanup kill child
	// groups even if go test panics/times out without running its defers.
	marker := ""
	if directory := os.Getenv("API_PROCESS_PID_DIR"); directory != "" {
		marker = filepath.Join(directory, strconv.Itoa(p.PID()))
		if e := os.WriteFile(marker, nil, 0600); e != nil {
			_ = p.Stop()
			return ErrStart
		}
	}
	defer func() {
		stopErr := p.Stop()
		err = errors.Join(err, stopErr)
		if marker != "" && stopErr == nil {
			_ = os.Remove(marker)
		}
	}()
	watchDone := make(chan struct{})
	defer close(watchDone)
	go func() {
		select {
		case <-ctx.Done():
			_ = p.Stop()
		case <-watchDone:
		}
	}()
	readyCtx, cancel := context.WithTimeout(ctx, cfg.StartupTimeout)
	defer cancel()
	transport := &http.Transport{Proxy: nil}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, Timeout: 100 * time.Millisecond, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-p.done:
			// Exit code is safe metadata, child output and command are not.
			code := -1
			if p.cmd.ProcessState != nil {
				code = p.cmd.ProcessState.ExitCode()
			}
			return errors.Join(ErrEarlyExit, errors.New("exit status "+strconv.Itoa(code)))
		case <-readyCtx.Done():
			return readyCtx.Err()
		default:
		}
		req, _ := http.NewRequestWithContext(readyCtx, http.MethodGet, p.URL+probePath, nil)
		resp, e := client.Do(req)
		if e == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				select {
				case <-p.done:
					return ErrEarlyExit
				default:
				}
				return check(p)
			}
		}
		select {
		case <-p.done:
			continue
		case <-readyCtx.Done():
			return readyCtx.Err()
		case <-ticker.C:
		}
	}
}

// Validate before allocating resources or starting the child. Never return the
// supplied path or URL parser errors: callers may accidentally supply secrets.
func validProbePath(path string) bool {
	if !strings.HasPrefix(path, "/") || strings.HasPrefix(path, "//") || strings.ContainsAny(path, "?#\\") {
		return false
	}
	u, err := url.ParseRequestURI(path)
	if err != nil || u.Scheme != "" || u.Host != "" || u.Opaque != "" {
		return false
	}
	return !strings.ContainsFunc(path, unicode.IsControl) && !strings.ContainsFunc(u.Path, unicode.IsControl)
}
