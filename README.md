# caddy-retry-response

A Caddy HTTP middleware module (`http.handlers.retry_response`) that transparently
re-executes a request when the downstream handler responds with a retryable HTTP
status — the Caddy-layer equivalent of nginx's
`fastcgi_next_upstream http_429 non_idempotent;`.

## Motivation

Some applications signal "this request failed for a transient reason, run it again"
by returning a specific HTTP status. A concrete example: a Laravel app on Google
Cloud Spanner catches the transaction-abort exception (`AbortedException`), sleeps
for the recommended retry delay, and converts it into an HTTP 429. Under php-fpm,
nginx picked that 429 up and re-executed the request on another upstream. Caddy
(and therefore [FrankenPHP](https://frankenphp.dev/)) has no built-in equivalent —
this module fills that gap.

When the module sees a retryable status (429 by default), it discards that response
and replays the exact same request against the downstream handler, up to a
configurable number of attempts. The client only ever sees the final response.

## Behavior

- If the response status is retryable (default: 429), the request is re-executed
  instead of the response being sent to the client.
- The request body is buffered so it can be replayed: in memory up to
  `memory_buffer`, spilling to a temp file beyond that. Temp files are unlinked
  immediately after creation, so nothing lingers even on abnormal exit.
- Bodies larger than `max_body` are neither buffered nor retried — the request
  runs exactly once, and the site's own `request_body` limits apply as usual.
- If the final attempt still yields a retryable status, that response is returned
  to the client as-is.
- Headers and body from discarded attempts never leak into the response the
  client receives. Each attempt gets a cloned header map, so mutations
  (including deleting server-preset headers like `Server: Caddy`) stay isolated.
- Responses with a non-retryable status stream straight through, including
  `Flush`-based streaming.
- Hijacked connections (WebSockets etc.) are passed through and never retried.
- Handler errors (as opposed to error-status responses) propagate to Caddy's
  normal error handling without retrying.
- Retrying stops early if the client disconnects; the request is logged with the
  non-standard status 499 (client closed request), as nginx does.
- The module does **not** wait between attempts. If a backoff is needed, the
  application is expected to sleep before responding (as in the Spanner example
  above, where the app sleeps for the server-recommended retry delay).

## Installation

Build Caddy with this module using [xcaddy](https://github.com/caddyserver/xcaddy):

```sh
xcaddy build --with github.com/dkkoma/caddy-retry-response
```

## Caddyfile

```caddyfile
example.com {
    retry_response {
        statuses      429
        attempts      10
        memory_buffer 50MiB
        max_body      1GiB
        temp_dir      /tmp/caddy-retry
    }

    reverse_proxy localhost:9000
}
```

The directive is ordered `before route`, so when written at the top level of a
site block it wraps handlers like `reverse_proxy`, `php_server`, and
`file_server` regardless of where it appears in the block.

### Directives

All subdirectives are optional.

| Subdirective | Default | Description |
|---|---|---|
| `statuses <code...>` | `429` | HTTP response statuses that trigger a retry. Must be 200–599 (1xx can never be a final status). |
| `attempts <n>` | `10` | Total number of tries, **including the first one**. `attempts 1` disables retrying. |
| `memory_buffer <size>` | `50MiB` | Request body size kept in memory. Anything beyond spills to a temp file. Accepts human-readable sizes (`50MiB`, `1GB`, ...). A zero value means "use the default", following Caddy's usual zero-value convention; it cannot be used to force every body directly to disk. |
| `max_body <size>` | `1GiB` | Largest request body to buffer. Larger bodies run exactly once, without retry. The default is intentionally broad; set this explicitly for production sites. |
| `temp_dir <path>` | OS temp dir | Directory for spill files. Created with mode 0700 if missing. |

### JSON config

```json
{
  "handler": "retry_response",
  "statuses": [429],
  "attempts": 10,
  "memory_buffer": 52428800,
  "max_body": 1073741824,
  "temp_dir": "/tmp/caddy-retry"
}
```

## Use with FrankenPHP

Build FrankenPHP with the module:

```sh
xcaddy build \
    --output frankenphp \
    --with github.com/dunglas/frankenphp/caddy \
    --with github.com/dkkoma/caddy-retry-response
```

Then wrap `php_server` with it. A snippet keeps per-site tuning in one place:

```caddyfile
(retry_transaction) {
    retry_response {
        statuses 429
        attempts 10
        memory_buffer 50MiB
        max_body {args[0]}
        temp_dir /tmp/caddy-retry
    }
}

example.com {
    root /app/public

    import retry_transaction 100MiB

    request_body {
        max_size 100MiB
    }

    php_server
}
```

## Operational notes

- Keep `max_body` aligned with the site's `request_body max_size`. The module
  runs before the site's request-body limit and buffers **outside** that limit,
  so the default `1GiB` can allow large requests to be written to disk before
  `request_body` rejects them. Set `max_body` explicitly instead of relying on
  the default for production sites.
- Only the final attempt's status appears in Caddy's access log (same as nginx).
  Intermediate attempts are visible in the module's debug log
  (`retrying request` with `uri`, `attempt`, and `status` fields).
- The module retries on the configured statuses indiscriminately. If the
  application also returns 429 for rate limiting, those responses are retried
  too — either use a status you control end-to-end or make sure that's the
  behavior you want.
- Retried requests are re-executed regardless of HTTP method idempotency
  (that's the `non_idempotent` part of the nginx equivalent). The assumption is
  that the application only emits the retryable status when re-execution is safe.

## Out of scope

- **Streaming responses**: once a non-retryable status is written, the response
  streams through; a retryable status arriving *after* streaming started cannot
  be (and is not) retried.
- **Hijacked connections** (WebSockets etc.): passed through untouched.
- **Trailers on pass-through attempts**: HTTP trailers set after `WriteHeader`
  on a non-final pass-through attempt are not preserved. This module is designed
  for conventional request/response handlers such as `php_server`, not gRPC
  trailer semantics.
- **ResponseController deadlines**: the wrapper intentionally does not implement
  `Unwrap`, so `http.ResponseController` operations other than the explicitly
  supported `Flush` and `Hijack` may return `ErrNotSupported` on non-final
  attempts.
- **Waiting/backoff between attempts**: intentionally not implemented; see
  Behavior above.
- **`Retry-After` interpretation**: the response is discarded without reading it.

## Testing

```sh
go test ./...
```

The default test suite includes unit tests, Caddyfile adapter/order coverage, and
an in-process Caddy HTTP integration test using `reverse_proxy`. These are safe
to run in CI and do not require FrankenPHP.

An optional FrankenPHP smoke test is available for manual verification. Build a
FrankenPHP binary that includes this module, then run:

```sh
FRANKENPHP_BIN=/path/to/frankenphp go test -tags=e2e ./...
```

There is also a Docker-based FrankenPHP E2E check which builds a FrankenPHP
image with this module, serves a tiny PHP app through `php_server`, and verifies
that a first-attempt 429 is retried without leaking its response:

```sh
./e2e/frankenphp/run.sh
```

## License

[MIT](LICENSE)
