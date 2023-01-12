# Cluster sharding prototype

1. Build clushard `go build -o clushard cmd/clushard/main.go`
2. Run first node `./clushard -p 8000`
3. Run second node and join to cluster `../clushard -j localhost:8000 -p 8001`
4. Run third node and join to cluster `../clushard -j localhost:8000 -p 8002`
5. Run benchmark in `clushard_test.go` and check how actors distributed across cluster nodes