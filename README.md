# coach

A coach running drills against a qdrant deployment.

## status

WIP

## usage

```bash
Tool for coaching Qdrant instances

Usage: coach [OPTIONS]

Options:
      --uri <URI>                                Qdrant service URI [default: http://localhost:6334]
  -p, --parallel-drills <PARALLEL_DRILLS>        Number of parallel drills to run [default: 3]
  -r, --replication-factor <REPLICATION_FACTOR>  Replication factor for collections [default: 1]
  -i, --indexing-threshold <INDEXING_THRESHOLD>  Optimizer indexing threshold
      --recreate-collection                      Always create collection before each drill
  -h, --help                                     Print help information
  -V, --version                                  Print version information
```