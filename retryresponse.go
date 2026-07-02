// Package retryresponse provides a Caddy HTTP middleware module that re-executes
// a request based on the response status of the downstream handler (php_server etc.).
//
// It reproduces the behavior of nginx's `fastcgi_next_upstream http_429 non_idempotent;`
// at the Caddy layer. A typical use case: the application converts a retryable
// condition (e.g. Google Cloud Spanner's AbortedException, after sleeping for the
// retry delay) into an HTTP 429 response; when this module sees the 429 it replays
// the same request instead of returning the response to the client. The module does
// not wait between attempts — any backoff is expected to happen in the application.
//
// To replay the request, the body is buffered in memory (up to memory_buffer) and
// spilled to a temp file beyond that. Bodies larger than max_body are not buffered
// and not retried (the request runs exactly once, leaving enforcement to the site's
// request_body limits and the like).
package retryresponse

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"maps"
	"net"
	"net/http"
	"os"
	"strconv"

	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/caddyconfig/caddyfile"
	"github.com/caddyserver/caddy/v2/caddyconfig/httpcaddyfile"
	"github.com/caddyserver/caddy/v2/modules/caddyhttp"
	"github.com/dustin/go-humanize"
	"go.uber.org/zap"
)

const (
	defaultAttempts     = 10       // matches a typical nginx upstream server count (= total tries)
	defaultMemoryBuffer = 50 << 20 // 50MiB
	defaultMaxBody      = 1 << 30  // 1GiB

	// non-standard status from nginx meaning "client closed the request" (for access logs)
	statusClientClosedRequest = 499
)

func init() {
	caddy.RegisterModule(&Handler{})
	httpcaddyfile.RegisterHandlerDirective("retry_response", parseCaddyfile)
	// When written directly in a site block, register the directive order so it
	// runs outside the route that wraps php_server / file_server
	httpcaddyfile.RegisterDirectiveOrder("retry_response", httpcaddyfile.Before, "route")
}

// Handler is a middleware that replays the buffered request body and re-executes
// the downstream handler when the response status is retryable.
type Handler struct {
	// HTTP statuses to retry on. Defaults to [429]
	Statuses []int `json:"statuses,omitempty"`

	// Total number of tries including the first one. Defaults to 10.
	// If the final attempt still yields a retryable status, that response is
	// returned to the client as-is
	Attempts int `json:"attempts,omitempty"`

	// Maximum request body size kept in memory (bytes). Anything beyond spills
	// to a temp file. Defaults to 50MiB
	MemoryBuffer int64 `json:"memory_buffer,omitempty"`

	// Maximum request body size to buffer (bytes). Larger bodies are neither
	// buffered nor retried; the request runs exactly once. Defaults to 1GiB
	MaxBody int64 `json:"max_body,omitempty"`

	// Directory for temp files. Defaults to the OS temp dir
	TempDir string `json:"temp_dir,omitempty"`

	statusSet map[int]struct{}
	logger    *zap.Logger
}

// CaddyModule implements caddy.Module.
func (*Handler) CaddyModule() caddy.ModuleInfo {
	return caddy.ModuleInfo{
		ID:  "http.handlers.retry_response",
		New: func() caddy.Module { return new(Handler) },
	}
}

// Provision implements caddy.Provisioner.
func (h *Handler) Provision(ctx caddy.Context) error {
	return h.provision(ctx.Logger())
}

// provision applies defaults and builds internal state (split out so tests can call it directly)
func (h *Handler) provision(logger *zap.Logger) error {
	h.logger = logger
	if len(h.Statuses) == 0 {
		h.Statuses = []int{http.StatusTooManyRequests}
	}
	if h.Attempts == 0 {
		h.Attempts = defaultAttempts
	}
	if h.MemoryBuffer == 0 {
		h.MemoryBuffer = defaultMemoryBuffer
	}
	if h.MaxBody == 0 {
		h.MaxBody = defaultMaxBody
	}
	if h.MemoryBuffer > h.MaxBody {
		h.MemoryBuffer = h.MaxBody
	}
	if h.TempDir == "" {
		h.TempDir = os.TempDir()
	} else if err := os.MkdirAll(h.TempDir, 0o700); err != nil {
		return fmt.Errorf("creating temp_dir: %v", err)
	}
	h.statusSet = make(map[int]struct{}, len(h.Statuses))
	for _, s := range h.Statuses {
		h.statusSet[s] = struct{}{}
	}
	return nil
}

// Validate implements caddy.Validator.
func (h *Handler) Validate() error {
	if h.Attempts < 1 {
		return fmt.Errorf("attempts must be >= 1, got %d", h.Attempts)
	}
	for _, s := range h.Statuses {
		// 1xx can never be a final status, so it cannot be retried on
		if s < 200 || s > 599 {
			return fmt.Errorf("invalid retry status: %d", s)
		}
	}
	if h.MemoryBuffer < 0 {
		return fmt.Errorf("memory_buffer must be >= 0, got %d", h.MemoryBuffer)
	}
	if h.MaxBody < 1 {
		return fmt.Errorf("max_body must be >= 1, got %d", h.MaxBody)
	}
	return nil
}

// ServeHTTP implements caddyhttp.MiddlewareHandler.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request, next caddyhttp.Handler) error {
	body, err := h.bufferRequestBody(r)
	if err != nil {
		return err
	}
	defer body.cleanup()

	if !body.replayable {
		// The body exceeds max_body and cannot be replayed, so run once without
		// retrying. The site's request_body limits and PHP-side post_max_size
		// handling apply as usual
		return next.ServeHTTP(w, r)
	}

	for attempt := 1; ; attempt++ {
		req := replayRequest(r, body)

		if attempt == h.Attempts {
			// Stream the final attempt's response straight to the client
			// (even if its status is retryable)
			return next.ServeHTTP(w, req)
		}

		aw := newAttemptWriter(w, h.statusSet)
		if err := next.ServeHTTP(aw, req); err != nil {
			// Handler errors are not subject to status-based retry.
			// Leave them to Caddy's error handling
			return err
		}

		if aw.hijacked || aw.committed || !aw.wroteHeader {
			// The response already went to the client (non-retryable status /
			// streaming / hijack). If nothing was written at all, defer to
			// Caddy's default handling
			return nil
		}

		// Retryable status: discard the response and re-execute
		if err := r.Context().Err(); err != nil {
			// The client already disconnected, so retrying is pointless
			h.logger.Debug("client disconnected during retry",
				zap.Int("attempt", attempt),
				zap.Int("status", aw.status))
			return caddyhttp.Error(statusClientClosedRequest, err)
		}
		h.logger.Debug("retrying request",
			zap.String("uri", r.RequestURI),
			zap.Int("attempt", attempt),
			zap.Int("status", aw.status))
	}
}

// bufferedBody is a request body buffered in a replayable form
type bufferedBody struct {
	replayable bool
	mem        []byte
	file       *os.File
	fileName   string
	size       int64
}

// newReader returns a fresh reader from the start of the body, independent per attempt
func (b *bufferedBody) newReader() io.ReadCloser {
	if b.file != nil {
		return io.NopCloser(io.NewSectionReader(b.file, 0, b.size))
	}
	return io.NopCloser(bytes.NewReader(b.mem))
}

func (b *bufferedBody) cleanup() {
	if b.file != nil {
		_ = b.file.Close()
		b.file = nil
	}
	if b.fileName != "" {
		_ = os.Remove(b.fileName)
		b.fileName = ""
	}
}

// bufferRequestBody buffers the request body in memory / a temp file.
// If the body exceeds max_body it sets replayable = false and swaps r.Body for a
// reader that stitches the already-read part to the unread rest (for a single,
// retry-free execution)
func (h *Handler) bufferRequestBody(r *http.Request) (*bufferedBody, error) {
	b := &bufferedBody{replayable: true}

	if r.Body == nil || r.Body == http.NoBody || r.ContentLength == 0 {
		return b, nil
	}
	if r.ContentLength > h.MaxBody {
		b.replayable = false
		return b, nil
	}

	if r.ContentLength > h.MemoryBuffer {
		f, err := h.createSpillFile(b)
		if err != nil {
			return nil, err
		}
		n, err := io.Copy(f, io.LimitReader(r.Body, h.MaxBody+1))
		if err != nil {
			b.cleanup()
			return nil, requestBodyReadError(r, err)
		}
		b.size = n
		if b.size > h.MaxBody {
			markBodyPassthrough(r, b)
		}
		return b, nil
	}

	var buf bytes.Buffer
	growBufferForContentLength(&buf, r.ContentLength)
	n, err := io.Copy(&buf, io.LimitReader(r.Body, h.MemoryBuffer+1))
	if err != nil {
		// failed to read the body from the client
		return nil, requestBodyReadError(r, err)
	}
	if n <= h.MemoryBuffer {
		b.mem = buf.Bytes()
		b.size = n
		return b, nil
	}

	// Spill anything beyond memory_buffer to a temp file
	f, err := h.createSpillFile(b)
	if err != nil {
		return nil, err
	}
	if _, err := f.Write(buf.Bytes()); err != nil {
		b.cleanup()
		return nil, caddyhttp.Error(http.StatusInternalServerError, err)
	}
	rest, err := io.Copy(f, io.LimitReader(r.Body, h.MaxBody-n+1))
	if err != nil {
		b.cleanup()
		return nil, requestBodyReadError(r, err)
	}
	b.size = n + rest

	if b.size > h.MaxBody {
		// Over the limit. Stitch the read part to the unread rest and fall back
		// to a single execution
		markBodyPassthrough(r, b)
	}
	return b, nil
}

func markBodyPassthrough(r *http.Request, b *bufferedBody) {
	b.replayable = false
	r.Body = &passthroughBody{
		Reader: io.MultiReader(io.NewSectionReader(b.file, 0, b.size), r.Body),
		closer: r.Body,
	}
}

func (h *Handler) createSpillFile(b *bufferedBody) (*os.File, error) {
	f, err := os.CreateTemp(h.TempDir, "retry-response-*")
	if err != nil {
		return nil, caddyhttp.Error(http.StatusInternalServerError, err)
	}
	// Unlink immediately on platforms that allow it. If that fails, remember
	// the name so cleanup can remove it after close.
	b.fileName = f.Name()
	if err := os.Remove(b.fileName); err == nil {
		b.fileName = ""
	}
	b.file = f
	return f, nil
}

func growBufferForContentLength(buf *bytes.Buffer, contentLength int64) {
	if contentLength <= 0 {
		return
	}
	if int64(int(contentLength)) != contentLength {
		return
	}
	buf.Grow(int(contentLength))
}

func requestBodyReadError(r *http.Request, err error) error {
	if ctxErr := r.Context().Err(); ctxErr != nil {
		return caddyhttp.Error(statusClientClosedRequest, ctxErr)
	}
	return caddyhttp.Error(http.StatusBadRequest, err)
}

// passthroughBody is a one-shot reader stitching the buffered part to the unread body
type passthroughBody struct {
	io.Reader
	closer io.Closer
}

func (p *passthroughBody) Close() error { return p.closer.Close() }

// replayRequest builds a per-attempt request carrying the buffered body.
// Headers are cloned so downstream mutations don't bleed across attempts.
// Caddy vars live in the request context, so context-scoped mutations are
// intentionally shared across attempts.
func replayRequest(r *http.Request, body *bufferedBody) *http.Request {
	req := r.Clone(r.Context())
	req.Body = body.newReader()
	req.ContentLength = body.size
	req.TransferEncoding = nil
	if body.size > 0 {
		// Even bodies received as chunked are fully buffered, so the length is known
		req.Header.Set("Content-Length", strconv.FormatInt(body.size, 10))
		req.Header.Del("Transfer-Encoding")
	}
	return req
}

// attemptWriter is the ResponseWriter used for every attempt except the last.
// It withholds writes to the real ResponseWriter until WriteHeader is called, then:
//   - for a retryable status, discards the entire response (header/body)
//   - for any other status, streams through to the real ResponseWriter
//
// The header map handed to the attempt is a clone, so headers set by a discarded
// attempt never leak into a later attempt's response.
// Note: Unwrap is deliberately not implemented. Implementing it would let
// http.ResponseController bypass this wrapper and flush a response that should
// have been discarded to the client
type attemptWriter struct {
	rw        http.ResponseWriter
	header    http.Header
	retryable map[int]struct{}

	status      int
	wroteHeader bool
	committed   bool
	hijacked    bool
}

func newAttemptWriter(w http.ResponseWriter, retryable map[int]struct{}) *attemptWriter {
	return &attemptWriter{
		rw: w,
		// Snapshot so headers preset by the server or outer middleware
		// (Server: Caddy etc.) are visible to — and deletable by — the attempt
		header:    w.Header().Clone(),
		retryable: retryable,
	}
}

// Header implements http.ResponseWriter.
func (a *attemptWriter) Header() http.Header { return a.header }

// WriteHeader implements http.ResponseWriter.
func (a *attemptWriter) WriteHeader(code int) {
	if a.hijacked || a.wroteHeader {
		return
	}
	if code >= 100 && code <= 199 {
		// informational responses are not final statuses; forward them as-is
		a.syncHeader()
		a.rw.WriteHeader(code)
		return
	}
	a.wroteHeader = true
	a.status = code
	if _, retry := a.retryable[code]; retry {
		// Retryable: write nothing to the real writer, discarding this attempt's response
		return
	}
	a.syncHeader()
	a.rw.WriteHeader(code)
	a.committed = true
}

// syncHeader replaces the real ResponseWriter's header map with this attempt's headers
func (a *attemptWriter) syncHeader() {
	dst := a.rw.Header()
	for k := range dst {
		if _, ok := a.header[k]; !ok {
			delete(dst, k)
		}
	}
	maps.Copy(dst, a.header)
}

// Write implements http.ResponseWriter.
func (a *attemptWriter) Write(p []byte) (int, error) {
	if a.hijacked {
		return 0, http.ErrHijacked
	}
	if !a.wroteHeader {
		a.WriteHeader(http.StatusOK)
	}
	if a.committed {
		return a.rw.Write(p)
	}
	// Swallow the body of a discarded attempt
	return len(p), nil
}

// Flush implements http.Flusher.
func (a *attemptWriter) Flush() {
	// Like a real Flusher, lock in a 200 if the status is still undecided
	if !a.wroteHeader && !a.hijacked {
		a.WriteHeader(http.StatusOK)
	}
	if a.committed {
		_ = http.NewResponseController(a.rw).Flush()
	}
}

// Hijack implements http.Hijacker. Hijacked connections (WebSocket etc.) are never retried
func (a *attemptWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	conn, rw, err := http.NewResponseController(a.rw).Hijack()
	if err == nil {
		a.hijacked = true
	}
	return conn, rw, err
}

// UnmarshalCaddyfile implements caddyfile.Unmarshaler. Syntax:
//
//	retry_response {
//	    statuses      <code...>
//	    attempts      <n>
//	    memory_buffer <size>
//	    max_body      <size>
//	    temp_dir      <path>
//	}
func (h *Handler) UnmarshalCaddyfile(d *caddyfile.Dispenser) error {
	d.Next() // consume directive name
	if d.NextArg() {
		return d.ArgErr()
	}
	for d.NextBlock(0) {
		switch d.Val() {
		case "statuses":
			args := d.RemainingArgs()
			if len(args) == 0 {
				return d.ArgErr()
			}
			for _, arg := range args {
				status, err := strconv.Atoi(arg)
				if err != nil {
					return d.Errf("parsing status %q: %v", arg, err)
				}
				h.Statuses = append(h.Statuses, status)
			}
		case "attempts":
			if !d.NextArg() {
				return d.ArgErr()
			}
			attempts, err := strconv.Atoi(d.Val())
			if err != nil {
				return d.Errf("parsing attempts %q: %v", d.Val(), err)
			}
			h.Attempts = attempts
		case "memory_buffer":
			size, err := parseSizeArg(d)
			if err != nil {
				return err
			}
			h.MemoryBuffer = size
		case "max_body":
			size, err := parseSizeArg(d)
			if err != nil {
				return err
			}
			h.MaxBody = size
		case "temp_dir":
			if !d.NextArg() {
				return d.ArgErr()
			}
			h.TempDir = d.Val()
		default:
			return d.Errf("unrecognized subdirective %q", d.Val())
		}
	}
	return nil
}

func parseSizeArg(d *caddyfile.Dispenser) (int64, error) {
	subdirective := d.Val()
	if !d.NextArg() {
		return 0, d.ArgErr()
	}
	size, err := humanize.ParseBytes(d.Val())
	if err != nil {
		return 0, d.Errf("parsing %s %q: %v", subdirective, d.Val(), err)
	}
	if size > uint64(1<<62) {
		return 0, d.Errf("%s too large: %s", subdirective, d.Val())
	}
	return int64(size), nil
}

func parseCaddyfile(helper httpcaddyfile.Helper) (caddyhttp.MiddlewareHandler, error) {
	var h Handler
	if err := h.UnmarshalCaddyfile(helper.Dispenser); err != nil {
		return nil, err
	}
	return &h, nil
}

// Interface guards
var (
	_ caddy.Provisioner           = (*Handler)(nil)
	_ caddy.Validator             = (*Handler)(nil)
	_ caddyhttp.MiddlewareHandler = (*Handler)(nil)
	_ caddyfile.Unmarshaler       = (*Handler)(nil)
	_ http.Flusher                = (*attemptWriter)(nil)
	_ http.Hijacker               = (*attemptWriter)(nil)
)
