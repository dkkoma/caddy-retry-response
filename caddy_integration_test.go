package retryresponse

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/caddyconfig"
	_ "github.com/caddyserver/caddy/v2/modules/caddyhttp/reverseproxy"
	_ "github.com/caddyserver/caddy/v2/modules/filestorage"
)

func TestCaddyfileAdapterOrdersRetryResponseBeforeRoute(t *testing.T) {
	cfg := adaptCaddyfile(t, fmt.Sprintf(`{
	admin off
	auto_https off
	persist_config off
	storage file_system %s
	storage_clean_interval off
}

:9080 {
	route {
		respond "ok"
	}

	retry_response {
		statuses 429 503
		attempts 5
		memory_buffer 1MiB
		max_body 10MiB
		temp_dir /tmp/caddy-retry
	}
}
`, t.TempDir()))

	handles := cfg["apps"].(map[string]any)["http"].(map[string]any)["servers"].(map[string]any)["srv0"].(map[string]any)["routes"].([]any)[0].(map[string]any)["handle"].([]any)
	if len(handles) < 2 {
		t.Fatalf("expected at least two handlers, got %#v", handles)
	}

	retry := handles[0].(map[string]any)
	if got := retry["handler"]; got != "retry_response" {
		t.Fatalf("first handler = %v, want retry_response; handles=%#v", got, handles)
	}
	if got := retry["attempts"]; got != float64(5) {
		t.Errorf("attempts = %v, want 5", got)
	}
	if got := retry["memory_buffer"]; got != float64(1<<20) {
		t.Errorf("memory_buffer = %v, want %d", got, 1<<20)
	}
	if got := retry["max_body"]; got != float64(10<<20) {
		t.Errorf("max_body = %v, want %d", got, 10<<20)
	}
	if got := retry["statuses"]; !equalJSONNumbers(got, []float64{429, 503}) {
		t.Errorf("statuses = %#v, want [429 503]", got)
	}
	if got := retry["temp_dir"]; got != "/tmp/caddy-retry" {
		t.Errorf("temp_dir = %v, want /tmp/caddy-retry", got)
	}

	if got := handles[1].(map[string]any)["handler"]; got != "subroute" {
		t.Fatalf("second handler = %v, want subroute; handles=%#v", got, handles)
	}
}

func TestCaddyRuntimeRetriesReverseProxyResponse(t *testing.T) {
	caddyPort := freeTCPPort(t)
	payload := strings.Repeat("json-payload:", 100)
	payloadHashBytes := sha256.Sum256([]byte(payload))
	payloadHash := hex.EncodeToString(payloadHashBytes[:])

	var calls atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempt := calls.Add(1)

		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("attempt %d: read body: %v", attempt, err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		if string(body) != payload {
			t.Errorf("attempt %d: body mismatch: got %d bytes", attempt, len(body))
			http.Error(w, "body mismatch", http.StatusInternalServerError)
			return
		}
		if got := r.Header.Get("Content-Length"); got != fmt.Sprint(len(payload)) {
			t.Errorf("attempt %d: Content-Length = %q, want %d", attempt, got, len(payload))
		}

		w.Header().Set("X-From-Attempt", fmt.Sprint(attempt))
		w.Header().Set("X-Body-SHA256", payloadHash)
		if attempt == 1 {
			w.Header().Set("Set-Cookie", "leak=1")
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = w.Write([]byte("discarded"))
			return
		}

		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	}))
	defer upstream.Close()

	upstreamAddr := strings.TrimPrefix(upstream.URL, "http://")
	loadCaddyfile(t, fmt.Sprintf(`{
	admin off
	auto_https off
	persist_config off
	storage file_system %s
	storage_clean_interval off
}

http://localhost:%d {
	retry_response {
		attempts 3
		memory_buffer 1MiB
		max_body 10MiB
	}

	reverse_proxy %s
}
`, t.TempDir(), caddyPort, upstreamAddr))

	req, err := http.NewRequest(http.MethodPost, fmt.Sprintf("http://localhost:%d/api", caddyPort), strings.NewReader(payload))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("request through Caddy: %v", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)

	if got := calls.Load(); got != 2 {
		t.Fatalf("upstream calls = %d, want 2", got)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%q", resp.StatusCode, body)
	}
	if string(body) != "ok" {
		t.Errorf("body = %q, want ok", body)
	}
	if got := resp.Header.Get("X-From-Attempt"); got != "2" {
		t.Errorf("X-From-Attempt = %q, want 2", got)
	}
	if got := resp.Header.Get("X-Body-SHA256"); got != payloadHash {
		t.Errorf("X-Body-SHA256 = %q, want %q", got, payloadHash)
	}
	if got := resp.Header.Get("Set-Cookie"); got != "" {
		t.Errorf("discarded attempt Set-Cookie leaked: %q", got)
	}
	if bytes.Contains(body, []byte("discarded")) {
		t.Errorf("discarded attempt body leaked: %q", body)
	}
}

func adaptCaddyfile(t *testing.T, caddyfile string) map[string]any {
	t.Helper()

	adapter := caddyconfig.GetAdapter("caddyfile")
	if adapter == nil {
		t.Fatal("caddyfile adapter is not registered")
	}
	cfgJSON, warnings, err := adapter.Adapt([]byte(caddyfile), nil)
	if err != nil {
		t.Fatalf("adapt Caddyfile: %v", err)
	}
	for _, warning := range warnings {
		t.Logf("Caddyfile warning: line=%d directive=%s message=%s", warning.Line, warning.Directive, warning.Message)
	}

	var cfg map[string]any
	if err := json.Unmarshal(cfgJSON, &cfg); err != nil {
		t.Fatalf("decode adapted JSON: %v\n%s", err, cfgJSON)
	}
	return cfg
}

func loadCaddyfile(t *testing.T, caddyfile string) {
	t.Helper()

	adapter := caddyconfig.GetAdapter("caddyfile")
	if adapter == nil {
		t.Fatal("caddyfile adapter is not registered")
	}
	cfgJSON, warnings, err := adapter.Adapt([]byte(caddyfile), nil)
	if err != nil {
		t.Fatalf("adapt Caddyfile: %v", err)
	}
	for _, warning := range warnings {
		t.Logf("Caddyfile warning: line=%d directive=%s message=%s", warning.Line, warning.Directive, warning.Message)
	}

	if err := caddy.Load(cfgJSON, true); err != nil {
		t.Fatalf("load Caddy config: %v\n%s", err, cfgJSON)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		done := make(chan error, 1)
		go func() { done <- caddy.Stop() }()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("stop Caddy: %v", err)
			}
		case <-ctx.Done():
			t.Errorf("stop Caddy timed out: %v", ctx.Err())
		}
	})
}

func freeTCPPort(t *testing.T) int {
	t.Helper()

	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("allocate TCP port: %v", err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

func equalJSONNumbers(got any, want []float64) bool {
	values, ok := got.([]any)
	if !ok || len(values) != len(want) {
		return false
	}
	for i := range values {
		value, ok := values[i].(float64)
		if !ok || value != want[i] {
			return false
		}
	}
	return true
}
