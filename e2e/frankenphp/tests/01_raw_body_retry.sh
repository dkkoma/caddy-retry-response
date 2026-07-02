#!/bin/sh
set -eu

PAYLOAD="${PAYLOAD:-frankenphp retry response payload}"

header_value() {
	awk -v name="$2" -F': *' 'tolower($1) == tolower(name) { gsub(/\r/, "", $2); print $2; exit }' "$1"
}

file_hash() {
	if command -v sha256sum >/dev/null 2>&1; then
		sha256sum "$1" | awk '{print $1}'
	else
		shasum -a 256 "$1" | awk '{print $1}'
	fi
}

payload_file="$(mktemp)"
headers="$(mktemp)"
body="$(mktemp)"
printf '%s' "$PAYLOAD" > "$payload_file"
expected_hash="$(file_hash "$payload_file")"

status="$(
	curl -sS \
		--connect-timeout 2 \
		--max-time 10 \
		-o "$body" \
		-D "$headers" \
		-w "%{http_code}" \
		-H "X-Retry-Test-ID: $TEST_ID-raw" \
		-H "Content-Type: text/plain" \
		--data "$PAYLOAD" \
		"$BASE_URL/"
)"

php_attempt="$(header_value "$headers" X-PHP-Attempt)"
body_hash="$(header_value "$headers" X-Body-SHA256)"
method_seen="$(header_value "$headers" X-Request-Method)"
length_seen="$(header_value "$headers" X-Content-Length-Seen)"

if [ "$status" != "200" ]; then
	echo "unexpected status: $status" >&2
	cat "$headers" >&2
	cat "$body" >&2
	docker logs "$CONTAINER" >&2 || true
	exit 1
fi

if [ "$php_attempt" != "2" ]; then
	echo "X-PHP-Attempt = $php_attempt, want 2" >&2
	cat "$headers" >&2
	exit 1
fi

if [ "$body_hash" != "$expected_hash" ]; then
	echo "X-Body-SHA256 = $body_hash, want $expected_hash" >&2
	exit 1
fi

if [ "$method_seen" != "POST" ]; then
	echo "X-Request-Method = $method_seen, want POST" >&2
	exit 1
fi

if [ "$length_seen" != "${#PAYLOAD}" ]; then
	echo "X-Content-Length-Seen = $length_seen, want ${#PAYLOAD}" >&2
	exit 1
fi

if grep -qi '^Set-Cookie: leak=1' "$headers"; then
	echo "discarded attempt Set-Cookie leaked" >&2
	cat "$headers" >&2
	exit 1
fi

if ! grep -q "attempt=2" "$body" || ! grep -q "$PAYLOAD" "$body"; then
	echo "unexpected body:" >&2
	cat "$body" >&2
	exit 1
fi
