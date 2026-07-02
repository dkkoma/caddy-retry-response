#!/bin/sh
set -eu

IMAGE="${IMAGE:-caddy-retry-response-frankenphp-e2e}"
CONTAINER="${CONTAINER:-caddy-retry-response-frankenphp-e2e}"
TEST_ID="${TEST_ID:-$(date +%s)-$$}"

cleanup() {
	docker rm -f "$CONTAINER" >/dev/null 2>&1 || true
}
trap cleanup EXIT INT TERM

echo "Building $IMAGE..."
docker build ${DOCKER_BUILD_ARGS:-} -f e2e/frankenphp/Dockerfile -t "$IMAGE" .

cleanup
echo "Starting $CONTAINER..."
docker run -d --rm --name "$CONTAINER" -p 127.0.0.1::8080 "$IMAGE" >/dev/null

PORT=""
READY=0
for _ in 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20; do
	PORT="$(docker port "$CONTAINER" 8080/tcp 2>/dev/null | sed 's/.*://')"
	if [ -n "$PORT" ] && curl -fsS --connect-timeout 2 --max-time 5 "http://127.0.0.1:$PORT/healthz" >/dev/null 2>&1; then
		READY=1
		break
	fi
	sleep 0.5
done

if [ -z "$PORT" ] || [ "$READY" != "1" ]; then
	echo "container did not become ready on port 8080" >&2
	docker logs "$CONTAINER" >&2 || true
	exit 1
fi

export BASE_URL="http://127.0.0.1:$PORT"
export CONTAINER
export TEST_ID

for test_case in e2e/frankenphp/tests/*.sh; do
	echo "Running $test_case..."
	"$test_case"
done

echo "FrankenPHP E2E passed: $BASE_URL retried php_server responses successfully"
