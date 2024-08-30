#!/usr/bin/env bash

set -e

RUN_TIME=${1:-300}
QDRANT_VERSION=${2:-dev}

REST_PORT="6333"
GRPC_PORT="6334"
QDRANT_HOST=localhost:${REST_PORT}

echo "Running for $RUN_TIME seconds"
echo "QDRANT_HOST: $QDRANT_HOST"
echo "QDRANT_VERSION: $QDRANT_VERSION"

# start qdrant docker
docker run -d --rm \
           -p ${REST_PORT}:${REST_PORT} \
           -p ${GRPC_PORT}:${GRPC_PORT} \
           -e QDRANT__SERVICE__GRPC_PORT=${GRPC_PORT} \
           --name qdrant_test qdrant/qdrant:"${QDRANT_VERSION}" ./qdrant --disable-telemetry

trap stop_docker SIGINT
trap stop_docker ERR

until $(curl --output /dev/null --silent --get --fail http://$QDRANT_HOST/collections); do
  printf 'waiting for server to start...'
  sleep 5
done

# start coach against qdrant
COACH_CMD=(
    cargo run --release
    --
    --parallel_drills 3
    --stop-at-first-error
)

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
