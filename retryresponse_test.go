package retryresponse

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/caddyserver/caddy/v2/caddyconfig/caddyfile"
	"github.com/caddyserver/caddy/v2/modules/caddyhttp"
	"go.uber.org/zap"
)

func newHandler(t *testing.T, mutate func(*Handler)) *Handler {
	t.Helper()
	h := &Handler{}
	if mutate != nil {
		mutate(h)
	}
	if err := h.provision(zap.NewNop()); err != nil {
		t.Fatalf("provision: %v", err)
	}
	if err := h.Validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}
	return h
}

type cancelingReadCloser struct {
	cancel context.CancelFunc
}

func (c *cancelingReadCloser) Read([]byte) (int, error) {
	c.cancel()
	return 0, io.ErrUnexpectedEOF
}

func (c *cancelingReadCloser) Close() error { return nil }

type errorReadCloser struct {
	err error
}

func (e *errorReadCloser) Read([]byte) (int, error) { return 0, e.err }

func (e *errorReadCloser) Close() error { return nil }

type multiHeaderRecorder struct {
	header http.Header
	codes  []int
	body   bytes.Buffer
}

func newMultiHeaderRecorder() *multiHeaderRecorder {
	return &multiHeaderRecorder{header: make(http.Header)}
}

func (m *multiHeaderRecorder) Header() http.Header { return m.header }

func (m *multiHeaderRecorder) WriteHeader(code int) {
	m.codes = append(m.codes, code)
}

func (m *multiHeaderRecorder) Write(p []byte) (int, error) {
	for _, code := range m.codes {
		if code >= 200 {
			return m.body.Write(p)
		}
	}
	m.WriteHeader(http.StatusOK)
	return m.body.Write(p)
}

// First attempt returns 429, second returns 200 with the body intact
func TestRetryThenSuccess(t *testing.T) {
	h := newHandler(t, func(h *Handler) { h.Attempts = 10 })
	payload := strings.Repeat("json-payload:", 100)

	calls := 0
	next := caddyhttp.HandlerFunc(func(w http.ResponseWriter, r *http.Request) error {
		calls++
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatalf("attempt %d: read body: %v", calls, err)
		}
		if string(body) != payload {
			t.Fatalf("attempt %d: body mismatch: got %d bytes", calls, len(body))
		}
		if got := r.Header.Get("Content-Length"); got != fmt.Sprint(len(payload)) {
			t.Fatalf("attempt %d: Content-Length = %q", calls, got)
		}
		w.Header().Set("X-From-Attempt", fmt.Sprint(calls))
		if calls == 1 {
			w.Header().Set("Set-Cookie", "leak=1")
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = w.Write([]byte("aborted"))
			return nil
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(body)
		return nil
	})

	req := httptest.NewRequest(http.MethodPost, "/api", strings.NewReader(payload))
	rec := httptest.NewRecorder()
	if err := h.ServeHTTP(rec, req, next); err != nil {
		t.Fatalf("ServeHTTP: %v", err)
	}

	if calls != 2 {
		t.Errorf("next called %d times, want 2", calls)
	}
	if rec.Code != http.StatusOK {
		t.Errorf("status = %d, want 200", rec.Code)
	}
	if rec.Body.String() != payload {
		t.Errorf("body not replayed correctly")
	}
	if got := rec.Header().Get("X-From-Attempt"); got != "2" {
		t.Errorf("X-From-Attempt = %q, want 2", got)
	}
	// headers from the discarded attempt must not leak
	if got := rec.Header().Get("Set-Cookie"); got != "" {
		t.Errorf("Set-Cookie leaked from discarded attempt: %q", got)
	}
	if strings.Contains(rec.Body.String(), "aborted") {
		t.Errorf("body leaked from discarded attempt")
	}
}

// When every attempt returns 429, the last 429 goes to the client
func TestAllAttemptsExhausted(t *testing.T) {
	h := newHandler(t, func(h *Handler) { h.Attempts = 3 })

	calls := 0
	next := caddyhttp.HandlerFunc(func(w http.ResponseWriter, r *http.Request) error {
		calls++
		w.Header().Set("X-From-Attempt", fmt.Sprint(calls))
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = fmt.Fprintf(w, "attempt %d", calls)
		return nil
	})

	req := httptest.NewRequest(http.MethodPost, "/api", strings.NewReader("data"))
	rec := httptest.NewRecorder()
	if err := h.ServeHTTP(rec, req, next); err != nil {
		t.Fatalf("ServeHTTP: %v", err)
	}

	if calls != 3 {
		t.Errorf("next called %d times, want 3", calls)
	}
	if rec.Code != http.StatusTooManyRequests {
		t.Errorf("status = %d, want 429", rec.Code)
	}
	if rec.Body.String() != "attempt 3" {
		t.Errorf("body = %q, want %q", rec.Body.String(), "attempt 3")
	}
	if got := rec.Header().Get("X-From-Attempt"); got != "3" {
		t.Errorf("X-From-Attempt = %q, want 3", got)
	}
}

// Non-retryable statuses pass through on the first attempt
func TestNonRetryStatusPassthrough(t *testing.T) {
	h := newHandler(t, nil)

	calls := 0
	next := caddyhttp.HandlerFunc(func(w http.ResponseWriter, r *http.Request) error {
		calls++
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte("boom"))
		return nil
	})

	rec := httptest.NewRecorder()
	if err := h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil), next); err != nil {
		t.Fatalf("ServeHTTP: %v", err)
	}
	if calls != 1 {
		t.Errorf("next called %d times, want 1", calls)
	}
	if rec.Code != http.StatusInternalServerError || rec.Body.String() != "boom" {
		t.Errorf("response not passed through: %d %q", rec.Code, rec.Body.String())
	}
}

func TestInformationalResponseBeforeRetry(t *testing.T) {
	h := newHandler(t, func(h *Handler) { h.Attempts = 3 })

	calls := 0
	next := caddyhttp.HandlerFunc(func(w http.ResponseWriter, r *http.Request) error {
		calls++
		if calls == 1 {
			w.WriteHeader(http.StatusEarlyHints)
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = w.Write([]byte("discarded"))
			return nil
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
		return nil
	})

	rec := newMultiHeaderRecorder()
	if err := h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil), next); err != nil {
		t.Fatalf("ServeHTTP: %v", err)
	}
	if calls != 2 {
		t.Fatalf("next called %d times, want 2", calls)
	}
	if len(rec.codes) != 2 || rec.codes[0] != http.StatusEarlyHints || rec.codes[1] != http.StatusOK {
		t.Fatalf("codes = %v, want [103 200]", rec.codes)
	}
	if rec.body.String() != "ok" {
		t.Fatalf("body = %q, want ok", rec.body.String())
	}
}

// Bodies over memory_buffer spill to a temp file and survive the retry
func TestSpillToTempFile(t *testing.T) {
	dir := t.TempDir()
	h := newHandler(t, func(h *Handler) {
		h.MemoryBuffer = 16
		h.TempDir = dir
	})
	payload := strings.Repeat("0123456789abcdef", 256) // 4KiB > 16B

	calls := 0
	next := caddyhttp.HandlerFunc(func(w http.ResponseWriter, r *http.Request) error {
		calls++
		body, _ := io.ReadAll(r.Body)
		if string(body) != payload {
			t.Fatalf("attempt %d: body mismatch (%d bytes)", calls, len(body))
		}
		if calls == 1 {
			w.WriteHeader(http.StatusTooManyRequests)
			return nil
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(body)
		return nil
	})

	req := httptest.NewRequest(http.MethodPost, "/upload", strings.NewReader(payload))
	rec := httptest.NewRecorder()
	if err := h.ServeHTTP(rec, req, next); err != nil {
		t.Fatalf("ServeHTTP: %v", err)
	}
	if calls != 2 || rec.Code != http.StatusOK || rec.Body.String() != payload {
		t.Errorf("spilled body not replayed: calls=%d status=%d", calls, rec.Code)
	}
	// The temp file is unlinked right after creation, so none remain afterwards
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Errorf("temp files remain: %v", entries)
	}
}

// Verify directly that bufferRequestBody takes the spill path
func TestBufferRequestBodySpill(t *testing.T) {
	h := newHandler(t, func(h *Handler) {
		h.MemoryBuffer = 4
		h.TempDir = t.TempDir()
	})
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("0123456789"))
	b, err := h.bufferRequestBody(req)
	if err != nil {
		t.Fatal(err)
	}
	defer b.cleanup()
	if b.file == nil || b.mem != nil {
		t.Fatalf("expected file spill, got mem=%v file=%v", b.mem, b.file)
	}
	if b.size != 10 {
		t.Fatalf("size = %d, want 10", b.size)
	}
	// multiple readers can independently read from the start
	for i := range 2 {
		data, _ := io.ReadAll(b.newReader())
		if string(data) != "0123456789" {
			t.Fatalf("reader %d: %q", i, data)
		}
	}
}

func TestBufferRequestBodyMemoryBufferBoundary(t *testing.T) {
	h := newHandler(t, func(h *Handler) {
		h.MemoryBuffer = 4
		h.TempDir = t.TempDir()
	})
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("0123"))
	b, err := h.bufferRequestBody(req)
	if err != nil {
		t.Fatal(err)
	}
	defer b.cleanup()
	if b.file != nil {
		t.Fatalf("expected in-memory body, got file=%v", b.file)
	}
	if string(b.mem) != "0123" {
		t.Fatalf("mem = %q, want 0123", b.mem)
	}
	if b.size != 4 {
		t.Fatalf("size = %d, want 4", b.size)
	}
}

// A Content-Length over max_body means no buffering and no retry
func TestMaxBodyExceededByContentLength(t *testing.T) {
	h := newHandler(t, func(h *Handler) { h.MaxBody = 8 })
	payload := "this is way too large"

	calls := 0
	next := caddyhttp.HandlerFunc(func(w http.ResponseWriter, r *http.Request) error {
		calls++
		body, _ := io.ReadAll(r.Body)
		if string(body) != payload {
			t.Fatalf("body mismatch: %q", body)
		}
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = w.Write([]byte("no retry"))
		return nil
	})

	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(payload))
	rec := httptest.NewRecorder()
	if err := h.ServeHTTP(rec, req, next); err != nil {
		t.Fatalf("ServeHTTP: %v", err)
	}
	if calls != 1 {
		t.Errorf("next called %d times, want 1 (no retry)", calls)
	}
	if rec.Code != http.StatusTooManyRequests {
		t.Errorf("status = %d, want 429 passthrough", rec.Code)
	}
}

// A body of unknown length (chunked) that crosses max_body mid-spill still runs
// exactly once with no bytes lost
func TestMaxBodyExceededChunked(t *testing.T) {
	h := newHandler(t, func(h *Handler) {
		h.MemoryBuffer = 4
		h.MaxBody = 8
		h.TempDir = t.TempDir()
	})
	payload := "0123456789abcdef" // 16 bytes > max_body 8

	calls := 0
	next := caddyhttp.HandlerFunc(func(w http.ResponseWriter, r *http.Request) error {
		calls++
		body, _ := io.ReadAll(r.Body)
		if string(body) != payload {
			t.Fatalf("body mismatch: got %q", body)
		}
		w.WriteHeader(http.StatusTooManyRequests)
		return nil
	})

	// passing a bare io.Reader yields ContentLength = -1 (i.e. chunked)
	req := httptest.NewRequest(http.MethodPost, "/", io.MultiReader(strings.NewReader(payload)))
	if req.ContentLength != -1 {
		t.Fatalf("precondition: ContentLength = %d, want -1", req.ContentLength)
	}
	rec := httptest.NewRecorder()
	if err := h.ServeHTTP(rec, req, next); err != nil {
		t.Fatalf("ServeHTTP: %v", err)
	}
	if calls != 1 {
		t.Errorf("next called %d times, want 1 (no retry)", calls)
	}
}

// Retrying stops once the client disconnects
func TestClientDisconnectStopsRetry(t *testing.T) {
	h := newHandler(t, func(h *Handler) { h.Attempts = 10 })

	ctx, cancel := context.WithCancel(context.Background())
	calls := 0
	next := caddyhttp.HandlerFunc(func(w http.ResponseWriter, r *http.Request) error {
		calls++
		cancel() // simulate the client disconnecting mid-request
		w.WriteHeader(http.StatusTooManyRequests)
		return nil
	})

	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("x")).WithContext(ctx)
	rec := httptest.NewRecorder()
	err := h.ServeHTTP(rec, req, next)
	if calls != 1 {
		t.Errorf("next called %d times, want 1", calls)
	}
	var handlerErr caddyhttp.HandlerError
	if !errors.As(err, &handlerErr) || handlerErr.StatusCode != statusClientClosedRequest {
		t.Errorf("err = %v, want HandlerError 499", err)
	}
}

// A disconnect while reading the request body is logged like nginx's 499, not
// treated as a malformed request body
func TestClientDisconnectDuringBodyReadReturns499(t *testing.T) {
	h := newHandler(t, nil)

	ctx, cancel := context.WithCancel(context.Background())
	req := httptest.NewRequest(http.MethodPost, "/", nil).WithContext(ctx)
	req.Body = &cancelingReadCloser{cancel: cancel}
	req.ContentLength = -1

	calls := 0
	next := caddyhttp.HandlerFunc(func(w http.ResponseWriter, r *http.Request) error {
		calls++
		return nil
	})

	err := h.ServeHTTP(httptest.NewRecorder(), req, next)
	if calls != 0 {
		t.Errorf("next called %d times, want 0", calls)
	}
	var handlerErr caddyhttp.HandlerError
	if !errors.As(err, &handlerErr) || handlerErr.StatusCode != statusClientClosedRequest {
		t.Errorf("err = %v, want HandlerError 499", err)
	}
}

func TestRequestBodyHandlerErrorPreserved(t *testing.T) {
	h := newHandler(t, nil)

	req := httptest.NewRequest(http.MethodPost, "/", nil)
	req.Body = &errorReadCloser{
		err: caddyhttp.Error(http.StatusRequestEntityTooLarge, errors.New("request body too large")),
	}
	req.ContentLength = -1

	calls := 0
	next := caddyhttp.HandlerFunc(func(w http.ResponseWriter, r *http.Request) error {
		calls++
		return nil
	})

	err := h.ServeHTTP(httptest.NewRecorder(), req, next)
	if calls != 0 {
		t.Errorf("next called %d times, want 0", calls)
	}
	handlerErr, ok := err.(caddyhttp.HandlerError)
	if !ok || handlerErr.StatusCode != http.StatusRequestEntityTooLarge {
		t.Errorf("err = %v, want HandlerError 413", err)
	}
}

// Headers preset by the server (Server: Caddy etc.) are visible to, and
// removable by, each attempt
func TestPresetHeaderVisibleAndRemovable(t *testing.T) {
	h := newHandler(t, func(h *Handler) { h.Attempts = 5 })

	calls := 0
	next := caddyhttp.HandlerFunc(func(w http.ResponseWriter, r *http.Request) error {
		calls++
		if got := w.Header().Get("Server"); got != "Caddy" {
			t.Fatalf("attempt %d: preset Server header not visible: %q", calls, got)
		}
		w.Header().Del("Server")
		if calls == 1 {
			w.WriteHeader(http.StatusTooManyRequests)
			return nil
		}
		w.WriteHeader(http.StatusOK)
		return nil
	})

	rec := httptest.NewRecorder()
	rec.Header().Set("Server", "Caddy")
	if err := h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil), next); err != nil {
		t.Fatalf("ServeHTTP: %v", err)
	}
	if got, ok := rec.Header()["Server"]; ok {
		t.Errorf("Server header should have been removed, got %v", got)
	}
}

// A bodyless GET can be retried, and no Content-Length is added
func TestGetWithoutBody(t *testing.T) {
	h := newHandler(t, nil)

	calls := 0
	next := caddyhttp.HandlerFunc(func(w http.ResponseWriter, r *http.Request) error {
		calls++
		if got := r.Header.Get("Content-Length"); got != "" {
			t.Fatalf("unexpected Content-Length %q on GET", got)
		}
		if calls == 1 {
			w.WriteHeader(http.StatusTooManyRequests)
			return nil
		}
		w.WriteHeader(http.StatusOK)
		return nil
	})

	rec := httptest.NewRecorder()
	if err := h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil), next); err != nil {
		t.Fatalf("ServeHTTP: %v", err)
	}
	if calls != 2 || rec.Code != http.StatusOK {
		t.Errorf("calls=%d status=%d", calls, rec.Code)
	}
}

// Streaming (Flush) with a non-retryable status passes straight through
func TestStreamingFlushPassthrough(t *testing.T) {
	h := newHandler(t, nil)

	next := caddyhttp.HandlerFunc(func(w http.ResponseWriter, r *http.Request) error {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("chunk1"))
		http.NewResponseController(w).Flush()
		_, _ = w.Write([]byte("chunk2"))
		return nil
	})

	rec := httptest.NewRecorder()
	if err := h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/stream", nil), next); err != nil {
		t.Fatalf("ServeHTTP: %v", err)
	}
	if !rec.Flushed {
		t.Errorf("Flush was not propagated")
	}
	if rec.Body.String() != "chunk1chunk2" {
		t.Errorf("body = %q", rec.Body.String())
	}
}

type hijackableRecorder struct {
	*httptest.ResponseRecorder
	conn net.Conn
}

func (h *hijackableRecorder) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	return h.conn, bufio.NewReadWriter(bufio.NewReader(h.conn), bufio.NewWriter(h.conn)), nil
}

// Hijacked connections (WebSocket etc.) are never retried
func TestHijackNoRetry(t *testing.T) {
	h := newHandler(t, nil)
	c1, c2 := net.Pipe()
	defer c1.Close()
	defer c2.Close()

	calls := 0
	next := caddyhttp.HandlerFunc(func(w http.ResponseWriter, r *http.Request) error {
		calls++
		conn, _, err := http.NewResponseController(w).Hijack()
		if err != nil {
			t.Fatalf("hijack: %v", err)
		}
		_ = conn
		return nil
	})

	rec := &hijackableRecorder{ResponseRecorder: httptest.NewRecorder(), conn: c1}
	if err := h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/ws", nil), next); err != nil {
		t.Fatalf("ServeHTTP: %v", err)
	}
	if calls != 1 {
		t.Errorf("next called %d times, want 1", calls)
	}
}

// Handler errors propagate as-is without retrying
func TestHandlerErrorPropagates(t *testing.T) {
	h := newHandler(t, nil)
	wantErr := errors.New("php exploded")

	calls := 0
	next := caddyhttp.HandlerFunc(func(w http.ResponseWriter, r *http.Request) error {
		calls++
		return wantErr
	})

	rec := httptest.NewRecorder()
	err := h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil), next)
	if !errors.Is(err, wantErr) {
		t.Errorf("err = %v, want %v", err, wantErr)
	}
	if calls != 1 {
		t.Errorf("next called %d times, want 1", calls)
	}
}

// attempts 1 returns immediately with no retry
func TestSingleAttempt(t *testing.T) {
	h := newHandler(t, func(h *Handler) { h.Attempts = 1 })

	calls := 0
	next := caddyhttp.HandlerFunc(func(w http.ResponseWriter, r *http.Request) error {
		calls++
		w.WriteHeader(http.StatusTooManyRequests)
		return nil
	})

	rec := httptest.NewRecorder()
	if err := h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/", strings.NewReader("x")), next); err != nil {
		t.Fatalf("ServeHTTP: %v", err)
	}
	if calls != 1 || rec.Code != http.StatusTooManyRequests {
		t.Errorf("calls=%d status=%d", calls, rec.Code)
	}
}

// Caddyfile parsing
func TestUnmarshalCaddyfile(t *testing.T) {
	d := caddyfile.NewTestDispenser(`retry_response {
		statuses 429 503
		attempts 5
		memory_buffer 50MiB
		max_body 1024MiB
		temp_dir /tmp/caddy-retry
	}`)
	var h Handler
	if err := h.UnmarshalCaddyfile(d); err != nil {
		t.Fatalf("UnmarshalCaddyfile: %v", err)
	}
	if want := []int{429, 503}; !slicesEqual(h.Statuses, want) {
		t.Errorf("Statuses = %v, want %v", h.Statuses, want)
	}
	if h.Attempts != 5 {
		t.Errorf("Attempts = %d", h.Attempts)
	}
	if h.MemoryBuffer != 50<<20 {
		t.Errorf("MemoryBuffer = %d", h.MemoryBuffer)
	}
	if h.MaxBody != 1024<<20 {
		t.Errorf("MaxBody = %d", h.MaxBody)
	}
	if h.TempDir != "/tmp/caddy-retry" {
		t.Errorf("TempDir = %q", h.TempDir)
	}
}

func TestUnmarshalCaddyfileRejectsUnknown(t *testing.T) {
	d := caddyfile.NewTestDispenser(`retry_response {
		nope 1
	}`)
	var h Handler
	if err := h.UnmarshalCaddyfile(d); err == nil {
		t.Error("expected error for unknown subdirective")
	}
}

func TestValidate(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*Handler)
		valid  bool
	}{
		{"defaults", nil, true},
		{"zero attempts after provision clamps to default", func(h *Handler) { h.Attempts = 0 }, true},
		{"informational status", func(h *Handler) { h.Statuses = []int{100} }, false},
		{"status out of range", func(h *Handler) { h.Statuses = []int{600} }, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := &Handler{}
			if tc.mutate != nil {
				tc.mutate(h)
			}
			if err := h.provision(zap.NewNop()); err != nil {
				t.Fatal(err)
			}
			if err := h.Validate(); (err == nil) != tc.valid {
				t.Errorf("Validate() = %v, want valid=%v", err, tc.valid)
			}
		})
	}
}

// Verify at the byte level that a large body (multipart-sized) replays intact
func TestLargeBodyReplayIntegrity(t *testing.T) {
	h := newHandler(t, func(h *Handler) {
		h.MemoryBuffer = 1 << 10 // 1KiB
		h.TempDir = t.TempDir()
	})
	// 3MiB of patterned data
	var payload bytes.Buffer
	for i := 0; payload.Len() < 3<<20; i++ {
		fmt.Fprintf(&payload, "%08x", i)
	}
	want := payload.String()

	calls := 0
	next := caddyhttp.HandlerFunc(func(w http.ResponseWriter, r *http.Request) error {
		calls++
		body, _ := io.ReadAll(r.Body)
		if string(body) != want {
			t.Fatalf("attempt %d: corrupted body (%d bytes)", calls, len(body))
		}
		if calls < 3 {
			w.WriteHeader(http.StatusTooManyRequests)
			return nil
		}
		w.WriteHeader(http.StatusOK)
		return nil
	})

	req := httptest.NewRequest(http.MethodPost, "/upload", strings.NewReader(want))
	rec := httptest.NewRecorder()
	if err := h.ServeHTTP(rec, req, next); err != nil {
		t.Fatalf("ServeHTTP: %v", err)
	}
	if calls != 3 || rec.Code != http.StatusOK {
		t.Errorf("calls=%d status=%d", calls, rec.Code)
	}
}

func slicesEqual(a, b []int) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
