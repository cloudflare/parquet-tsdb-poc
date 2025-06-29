# parquet-tsdb-poc

POC for a parquet based TSDB in object storage.

## Why

This project was inspired by [this excellent talk](https://www.youtube.com/watch?v=V8Y4VuUwg8I) by Shopify's Filip Petkovski. It is an attempt to build a service that can convert Prometheus TSDB blocks into [parquet](https://parquet.apache.org/) files and serve [PromQL](https://prometheus.io) queries to backfill a [Thanos deployment](https://thanos.io/).

## Developing

We recommend to use `nix` to fulfill all development dependencies. Visit [Nix Download](https://nixos.org/download/) to get started. To activate the development environment simply run `nix-shell` in the project root.

- to build the binary run `nix-shell --run 'make build'`
- to run tests run `nix-shell --run 'make test'`

## Running

### Server

Once built, you can run the server using something like:

```bash
parquet-gateway serve \
    --storage.prefix my-prefix \
    --http.internal.port=6060 \
    --http.prometheus.port=9090 \
    --http.thanos.port=9091 \
    --block.syncer.interval=30m \
    --block.syncer.concurrency=32 \
    --block.discovery.interval=30m \
    --block.discovery.concurrency=32 \
    --query.external-label=prometheus=my-prometheus \
    --query.external-label=replica=ha-1
```

This will:

- load blocks from the `.data/my-prefix` directory
- expose internal metrics and readiness handlers on port 6060
- expose a subset of the Prometheus HTTP API on port 9090
- expose an Thanos Info, Series and Query gRPC service on port 9091

You can now query it by pointing a Thanos Querier at it or through curl:

```bash
curl 'http://0.0.0.0:9000/api/v1/query' \
  -sq \
  -H 'content-type: application/x-www-form-urlencoded' \
  --data-urlencode 'query=vector(1)' | jq
{
  "status": "success",
  "data": {
    "resultType": "vector",
    "result": [
      {
        "metric": {},
        "value": [
          1741267893.103,
          "1"
        ]
      }
    ]
  }
}

```

### Converter

To convert TSDB blocks in the `.data/source` directory that overlap `09/2021` and write the resulting parquet files into the `.data/destination` directory.

```bash
parquet-gateway convert \
    --tsdb.storage.prefix source \
    --parquet.storage.prefix destination \
    --convert.sorting.label=__name__ \
    --convert.sorting.label=namespace
```

## Note

The code has significant overlap with the work in "https://github.com/prometheus-community/parquet-common". We are in the process of upstreaming and eventually plan to use it as a library.
