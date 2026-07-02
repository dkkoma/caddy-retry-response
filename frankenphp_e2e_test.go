//go:build e2e

package retryresponse

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestFrankenPHPE2ERetriesPHPServerResponse(t *testing.T) {
	frankenphpBin := os.Getenv("FRANKENPHP_BIN")
	if frankenphpBin == "" {
		t.Skip("set FRANKENPHP_BIN to a FrankenPHP binary built with this module")
	}

	caddyPort := freeTCPPort(t)
	adminPort := freeTCPPort(t)
	dir := t.TempDir()
	publicDir := filepath.Join(dir, "public")
	retryTempDir := filepath.Join(dir, "retry")
	countPath := filepath.Join(dir, "attempts.txt")

	if err := os.MkdirAll(publicDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(retryTempDir, 0o700); err != nil {
		t.Fatal(err)
	}

	countPathLiteral, err := json.Marshal(countPath)
	if err != nil {
		t.Fatal(err)
	}
	indexPHP := fmt.Sprintf(`<?php
$countFile = %s;
$attempt = file_exists($countFile) ? ((int) file_get_contents($countFile)) + 1 : 1;
file_put_contents($countFile, (string) $attempt);

$body = file_get_contents('php://input');
header('X-PHP-Attempt: ' . $attempt);
header('X-Body-SHA256: ' . hash('sha256', $body));

if ($attempt === 1) {
    header('Set-Cookie: leak=1');
    http_response_code(429);
    echo "discarded";
    return;
}

http_response_code(200);
echo "attempt=" . $attempt . "\n";
echo $body;
`, string(countPathLiteral))

	if err := os.WriteFile(filepath.Join(publicDir, "index.php"), []byte(indexPHP), 0o644); err != nil {
		t.Fatal(err)
	}

	caddyfile := fmt.Sprintf(`{
	admin localhost:%d
	auto_https off
	persist_config off
	storage file_system %s
	storage_clean_interval off
}

http://localhost:%d {
	root * %s

	respond /healthz "ok"

	retry_response {
		attempts 3
		memory_buffer 1MiB
		max_body 10MiB
		temp_dir %s
	}

	php_server
}
`, adminPort, filepath.Join(dir, "caddy-storage"), caddyPort, publicDir, retryTempDir)
	caddyfilePath := filepath.Join(dir, "Caddyfile")
	if err := os.WriteFile(caddyfilePath, []byte(caddyfile), 0o644); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var output bytes.Buffer
	cmd := exec.CommandContext(ctx, frankenphpBin, "run", "--config", caddyfilePath, "--adapter", "caddyfile")
	cmd.Stdout = &output
	cmd.Stderr = &output
	if err := cmd.Start(); err != nil {
		t.Fatalf("start FrankenPHP: %v", err)
	}

	var waitMu sync.Mutex
	var waitErr error
	done := make(chan struct{})
	go func() {
		err := cmd.Wait()
		waitMu.Lock()
		waitErr = err
		waitMu.Unlock()
		close(done)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Errorf("FrankenPHP did not stop within timeout")
		}
	})

	waitForFrankenPHP(t, caddyPort, done, func() error {
		waitMu.Lock()
		defer waitMu.Unlock()
		return waitErr
	}, &output)

	payload := strings.Repeat("frankenphp-payload:", 64)
	hashBytes := sha256.Sum256([]byte(payload))
	payloadHash := hex.EncodeToString(hashBytes[:])

	req, err := http.NewRequest(http.MethodPost, fmt.Sprintf("http://localhost:%d/", caddyPort), strings.NewReader(payload))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "text/plain")

	resp, err := (&http.Client{Timeout: 5 * time.Second}).Do(req)
	if err != nil {
		t.Fatalf("request through FrankenPHP: %v\n%s", err, output.String())
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%q\n%s", resp.StatusCode, body, output.String())
	}
	if got := resp.Header.Get("X-PHP-Attempt"); got != "2" {
		t.Errorf("X-PHP-Attempt = %q, want 2", got)
	}
	if got := resp.Header.Get("X-Body-SHA256"); got != payloadHash {
		t.Errorf("X-Body-SHA256 = %q, want %q", got, payloadHash)
	}
	if got := resp.Header.Get("Set-Cookie"); got != "" {
		t.Errorf("discarded attempt Set-Cookie leaked: %q", got)
	}
	if !strings.Contains(string(body), "attempt=2\n"+payload) {
		t.Errorf("body = %q, want attempt=2 and original payload", body)
	}

	countBytes, err := os.ReadFile(countPath)
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.TrimSpace(string(countBytes)); got != "2" {
		t.Errorf("PHP execution count = %q, want 2", got)
	}
}

func waitForFrankenPHP(t *testing.T, port int, done <-chan struct{}, waitErr func() error, output *bytes.Buffer) {
	t.Helper()

	deadline := time.Now().Add(10 * time.Second)
	url := fmt.Sprintf("http://localhost:%d/healthz", port)
	client := &http.Client{Timeout: 500 * time.Millisecond}
	for time.Now().Before(deadline) {
		select {
		case <-done:
			t.Fatalf("FrankenPHP exited before serving requests: %v\n%s", waitErr(), output.String())
		default:
		}

		resp, err := client.Get(url)
		if err == nil {
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("FrankenPHP did not become ready at %s\n%s", url, output.String())
}
