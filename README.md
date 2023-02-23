# coach

A coach running drills against a qdrant deployment (single node & distributed).

Those drills are run continuously to detect unexpected behaviors over a long period of time.

## drills

- `collection_churn`: drill that keeps on creating and deleting the same collection
- `points_churn`: drill that creates and deletes points in a collection.
- `points_search`: drill that performs search requests on a collection.
- `points_upsert`: drill that consistently updates the same points.
- `collection_snapshot_churn`: drill that keeps on creating and deleting the same collection snapshot.

## usage

```text
Tool for coaching Qdrant instances

Usage: coach [OPTIONS]

Options:
      --uris <URIS>
          Qdrant gRPC service URIs (can be used several times to specify several URIs) [default: http://localhost:6334]
  -p, --parallel-drills <PARALLEL_DRILLS>
          Number of parallel drills to run [default: 3]
      --replication-factor <REPLICATION_FACTOR>
          Replication factor for collections [default: 1]
      --indexing-threshold <INDEXING_THRESHOLD>
          Optimizer indexing threshold
      --recreate-collection
          Always create collection before the first run of a drill
      --stop-at-first-error
          Stop all drills at the first error encountered
      --only-healthcheck
          Only run the healthcheck for the input URI, no drills executed
      --health-check-delay-ms <HEALTH_CHECK_DELAY_MS>
          Delay between health checks [default: 200]
      --ignored-drills <IGNORED_DRILLS>
          Name of the drills to ignore
      --drills-to-run <DRILLS_TO_RUN>
          Name of the drills to run, ignore others
      --grpc-timeout-ms <GRPC_TIMEOUT_MS>
          Timeout of gRPC client [default: 1000]
      --grpc-health-check-timeout-ms <GRPC_HEALTH_CHECK_TIMEOUT_MS>
          Timeout of gRPC health client [default: 20]
  -h, --help
          Print help information
  -V, --version
          Print version information

```

e.g for a distributed deployment

```bash
./coach --uris "http://localhost:6334" \
        --uris "http://localhost:6344" \
        --uris "http://localhost:6354" \
        --recreate-collection \
        --replication-factor 2 \
        --indexing-threshold 1000
```

## docker

Docker image available on [Docker Hub](https://hub.docker.com/r/qdrant/coach).

```bash
# Download image
docker pull qdrant/coach:latest
# Run the tool
docker run qdrant/coach ./coach
```

## API key

```bash
export QDRANT_API_KEY='X3CXTPlA....lLZi8y5gA'
```

or `-e QDRANT_API_KEY='X3CXTPlA....lLZi8y5gA'` with Docker.