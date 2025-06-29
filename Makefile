.PHONY: all ci build proto lint test-norace test deps

all: build

ci: test-norace build lint

build: protos parquet-gateway

GO = go
GOTOOL = $(GO) tool -modfile=go.tools.mod
GOIMPORTS = $(GOTOOL) golang.org/x/tools/cmd/goimports
REVIVE = $(GOTOOL) github.com/mgechev/revive
MODERNIZE = $(GOTOOL) golang.org/x/tools/gopls/internal/analysis/modernize/cmd/modernize
PROTOC = protoc

lint: $(wildcard **/*.go)
	@echo ">> running lint..."
	$(REVIVE) -config revive.toml ./...
	$(MODERNIZE) -test ./...
	find . -name '*.go' ! -path './proto/*' | xargs $(GOIMPORTS) -l -w -local $(head -n 1 go.mod | cut -d ' ' -f 2)

test-norace: $(wildcard **/*.go)
	@echo ">> running tests without checking for races..."
	@mkdir -p .cover
	$(GO) test -v -tags stringlabels -short -count=1 ./... -coverprofile .cover/cover.out

test: $(wildcard **/*.go)
	@echo ">> running tests..."
	@mkdir -p .cover
	$(GO) test -v -tags stringlabels -race -short -count=1 ./... -coverprofile .cover/cover.out

parquet-gateway: $(shell find . -type f -name '*.go')
	@echo ">> building binaries..."
	@$(GO) build -tags stringlabels -o parquet-gateway github.com/cloudflare/parquet-tsdb-poc/cmd

protos: proto/metapb/meta.pb.go

proto/metapb/meta.pb.go: proto/metapb/meta.proto
	@echo ">> compiling protos..."
	@$(PROTOC) -I=proto/metapb/ --go_out=paths=source_relative:./proto/metapb/ proto/metapb/meta.proto
