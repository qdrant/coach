#!/usr/bin/env bash

set -e

RUN_TIME=${1:-300}
QDRANT_VERSION=${2:-dev}
# Extra space-separated KEY=VALUE env vars for the qdrant container, applied after the defaults (last one wins)
QDRANT_EXTRA_ENV=${QDRANT_EXTRA_ENV:-}
# Extra args passed to coach, e.g. "--drills-to-run high_concurrency"
COACH_EXTRA_ARGS=${COACH_EXTRA_ARGS:-}
# Where to write qdrant logs and resource stats
OUTPUT_DIR=${OUTPUT_DIR:-./coach-output}

# A full image reference (contains '/' or ':') is used as is, otherwise it is a Docker Hub tag
if [[ "$QDRANT_VERSION" == */* || "$QDRANT_VERSION" == *:* ]]; then
  QDRANT_IMAGE="$QDRANT_VERSION"
else
  QDRANT_IMAGE="qdrant/qdrant:${QDRANT_VERSION}"
fi

REST_PORT="6333"
GRPC_PORT="6334"
QDRANT_HOST=localhost:${REST_PORT}

echo "Running for $RUN_TIME seconds"
echo "QDRANT_HOST: $QDRANT_HOST"
echo "QDRANT_IMAGE: $QDRANT_IMAGE"
echo "QDRANT_EXTRA_ENV: $QDRANT_EXTRA_ENV"
echo "COACH_EXTRA_ARGS: $COACH_EXTRA_ARGS"

mkdir -p "$OUTPUT_DIR"

extra_env_args=()
for kv in $QDRANT_EXTRA_ENV; do
  extra_env_args+=(-e "$kv")
done

# start qdrant docker
docker run -d --rm \
           -p ${REST_PORT}:${REST_PORT} \
           -p ${GRPC_PORT}:${GRPC_PORT} \
           -e QDRANT__SERVICE__GRPC_PORT=${GRPC_PORT} \
           -e QDRANT__SERVICE__HARDWARE_REPORTING=false \
           -e QDRANT__STORAGE__COLLECTION_STRICT_MODE=true \
           -e QDRANT__FEATURE_FLAGS__ALL=true \
           -e QDRANT__CLUSTER__ENABLED=true \
           "${extra_env_args[@]}" \
           --name qdrant_test "${QDRANT_IMAGE}" ./qdrant --disable-telemetry --uri http://127.0.0.1:6335

trap stop_docker SIGINT
trap stop_docker ERR

docker inspect --format '{{.Image}} {{index .Config.Labels "org.opencontainers.image.revision"}}' qdrant_test || true

# sample container resource usage every 10s
(
  while docker inspect qdrant_test >/dev/null 2>&1; do
    docker stats --no-stream --format "$(date -u +%FT%TZ) {{.CPUPerc}} {{.MemUsage}} {{.BlockIO}}" qdrant_test 2>/dev/null
    sleep 10
  done
) > "$OUTPUT_DIR/qdrant-stats.log" &

until $(curl --output /dev/null --silent --get --fail http://$QDRANT_HOST/collections); do
  printf 'waiting for server to start...'
  sleep 5
done

# start coach against qdrant
COACH_CMD=(
    cargo run --release
    --
    --parallel-drills 3
    --stop-at-first-error
)
# shellcheck disable=SC2206
COACH_CMD+=($COACH_EXTRA_ARGS)

echo ""
echo "${COACH_CMD[*]}"
"${COACH_CMD[@]}" &

pid=$!

echo "The coach PID is $pid"

function cleanup() {
    if ps -p $pid >/dev/null
    then
        kill -KILL $pid
        stop_docker
    fi
}

function stop_docker()
{
  # the container runs with --rm, save its logs before stopping it
  docker logs qdrant_test > "$OUTPUT_DIR/qdrant.log" 2>&1 || true
  echo "stopping qdrant_test"
  docker stop qdrant_test
}


trap cleanup EXIT

trap 'exit $?' ERR
trap exit INT

started=$(date +%s)

while ps -p $pid >/dev/null && (( $(date +%s) - started < RUN_TIME ))
do
    sleep 10
done

if ps -p $pid >/dev/null
then
    echo "The process is still running. Stopping the process..."
    kill $pid
    stop_docker
    echo "OK"
else
    echo "The process has unexpectedly terminated on its own. Check the logs."
    stop_docker
    exit 1
fi
