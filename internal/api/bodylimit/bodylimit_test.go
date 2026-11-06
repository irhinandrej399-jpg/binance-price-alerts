package bodylimit_test

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/bodylimit"
)

type failingBody struct{ read, closed bool }

func (b *failingBody) Read([]byte) (int, error) {
	b.read = true
	return 0, errors.New("private read failure")
}
func (b *failingBody) Close() error { b.closed = true; return nil }

func TestReadFailureAndDeclaredOversize(t *testing.T) {
	for _, declared := range []int64{-1, 33} {
		body := &failingBody{}
		r := httptest.NewRequest("POST", "/", body)
		r.ContentLength = declared
		w := httptest.NewRecorder()
		bodylimit.New(32, http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
			t.Fatal("application handler called")
		})).ServeHTTP(w, r)
		if declared == -1 {
			if w.Code != 400 || w.Body.String() != "{\"error\":{\"code\":\"invalid_request\"}}\n" || !body.closed {
				t.Fatalf("read failure: %d %s closed=%v", w.Code, w.Body, body.closed)
			}
		} else if w.Code != 413 || body.read {
			t.Fatalf("declared oversize: status=%d read=%v", w.Code, body.read)
		}
	}
}

type countingBody struct{ bytes int }

func (b *countingBody) Read(p []byte) (int, error) {
	for i := range p {
		p[i] = 'x'
	}
	b.bytes += len(p)
	return len(p), nil
}
func (*countingBody) Close() error { return nil }

func TestUnknownLengthReadIsBounded(t *testing.T) {
	body := &countingBody{}
	r := httptest.NewRequest("POST", "/", body)
	r.ContentLength = -1
	w := httptest.NewRecorder()
	bodylimit.New(32, http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Fatal("application handler called")
	})).ServeHTTP(w, r)
	if w.Code != 413 || body.bytes != 33 {
		t.Fatalf("status=%d bytes=%d", w.Code, body.bytes)
	}
}

func TestBoundaries(t *testing.T) {
	for _, chunked := range []bool{false, true} {
		for _, size := range []int{0, 31, 32, 33, 1024} {
			t.Run(fmt.Sprintf("chunked=%v/size=%d", chunked, size), func(t *testing.T) {
				input := strings.Repeat("x", size)
				calls := 0
				h := bodylimit.New(32, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					calls++
					got, err := io.ReadAll(r.Body)
					if err != nil || string(got) != input {
						t.Errorf("body=%q err=%v", got, err)
					}
					w.WriteHeader(201)
				}))
				srv := httptest.NewServer(h)
				defer srv.Close()
				r, err := http.NewRequest("POST", srv.URL, strings.NewReader(input))
				if err != nil {
					t.Fatal(err)
				}
				if chunked {
					r.ContentLength = -1
					r.TransferEncoding = []string{"chunked"}
				}
				resp, err := srv.Client().Do(r)
				if err != nil {
					t.Fatal(err)
				}
				defer resp.Body.Close()
				got, err := io.ReadAll(resp.Body)
				if err != nil {
					t.Fatal(err)
				}
				if size <= 32 {
					if resp.StatusCode != 201 || calls != 1 {
						t.Fatalf("status=%d calls=%d", resp.StatusCode, calls)
					}
				} else if resp.StatusCode != 413 || calls != 0 || string(got) != "{\"error\":{\"code\":\"request_body_too_large\"}}\n" || resp.Header.Get("Content-Type") != "application/json" || resp.Header.Get("Cache-Control") != "no-store" {
					t.Fatalf("status=%d calls=%d body=%s headers=%v", resp.StatusCode, calls, got, resp.Header)
				}
			})
		}
	}
}
