#!/bin/sh
set -eu

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

upload_file="$(mktemp)"
headers="$(mktemp)"
body="$(mktemp)"
printf '%s' "uploaded file payload from FrankenPHP E2E" > "$upload_file"
expected_hash="$(file_hash "$upload_file")"
expected_size="$(wc -c < "$upload_file" | tr -d ' ')"

status="$(
	curl -sS \
		--connect-timeout 2 \
		--max-time 10 \
		-o "$body" \
		-D "$headers" \
		-w "%{http_code}" \
		-H "X-Retry-Test-ID: $TEST_ID-upload" \
		-F "upload=@$upload_file;filename=sample.txt;type=text/plain" \
		"$BASE_URL/?mode=upload"
)"

php_attempt="$(header_value "$headers" X-PHP-Attempt)"
upload_hash="$(header_value "$headers" X-Upload-SHA256)"
upload_error="$(header_value "$headers" X-Upload-Error)"
upload_size="$(header_value "$headers" X-Upload-Size)"
upload_tmp_exists="$(header_value "$headers" X-Upload-Tmp-Exists)"
upload_moved_exists="$(header_value "$headers" X-Upload-Moved-Exists)"

if [ "$status" != "200" ]; then
	echo "unexpected upload status: $status" >&2
	cat "$headers" >&2
	cat "$body" >&2
	docker logs "$CONTAINER" >&2 || true
	exit 1
fi

if [ "$php_attempt" != "2" ]; then
	echo "upload X-PHP-Attempt = $php_attempt, want 2" >&2
	cat "$headers" >&2
	exit 1
fi

if [ "$upload_error" != "0" ]; then
	echo "upload error = $upload_error, want 0" >&2
	exit 1
fi

if [ "$upload_hash" != "$expected_hash" ]; then
	echo "upload X-Upload-SHA256 = $upload_hash, want $expected_hash" >&2
	exit 1
fi

if [ "$upload_size" != "$expected_size" ]; then
	echo "upload X-Upload-Size = $upload_size, want $expected_size" >&2
	exit 1
fi

if [ "$upload_tmp_exists" != "yes" ]; then
	echo "upload tmp file was not present on final attempt" >&2
	cat "$headers" >&2
	exit 1
fi

if [ "$upload_moved_exists" != "yes" ]; then
	echo "first attempt moved file was not observed on final attempt" >&2
	cat "$headers" >&2
	exit 1
fi

if grep -qi '^Set-Cookie: upload-leak=1' "$headers"; then
	echo "discarded upload attempt Set-Cookie leaked" >&2
	cat "$headers" >&2
	exit 1
fi

if ! grep -q "upload-attempt=2" "$body" || ! grep -q "sample.txt" "$body"; then
	echo "unexpected upload body:" >&2
	cat "$body" >&2
	exit 1
fi
