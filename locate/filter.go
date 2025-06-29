// Copyright (c) 2025 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

package locate

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/thanos-io/thanos/pkg/info/infopb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/cloudflare/parquet-tsdb-poc/db"
	"github.com/cloudflare/parquet-tsdb-poc/internal/util"
	"github.com/cloudflare/parquet-tsdb-poc/schema"
)

type MetaFilter interface {
	filterMetas(map[string]schema.Meta) map[string]schema.Meta
	filterBlocks([]*db.Block) []*db.Block
}

var AllMetasMetaFilter = allMetas{}

type allMetas struct {
}

func (mf allMetas) filterMetas(metas map[string]schema.Meta) map[string]schema.Meta { return metas }
func (mf allMetas) filterBlocks(blocks []*db.Block) []*db.Block                     { return blocks }

type ThanosBackfillMetaFilter struct {
	endpoint string
	overlap  int64

	mu         sync.Mutex
	mint, maxt int64
}

func NewThanosBackfillMetaFilter(endpoint string, overlap time.Duration) *ThanosBackfillMetaFilter {
	return &ThanosBackfillMetaFilter{endpoint: endpoint, overlap: overlap.Milliseconds()}
}

func (tp *ThanosBackfillMetaFilter) filterMetas(metas map[string]schema.Meta) map[string]schema.Meta {
	tp.mu.Lock()
	defer tp.mu.Unlock()

	res := make(map[string]schema.Meta, len(metas))
	for k, v := range metas {
		if util.Contains(min(tp.mint+tp.overlap, tp.maxt), tp.maxt, v.Mint, v.Maxt) {
			continue
		}
		res[k] = v
	}
	return res
}

func (tp *ThanosBackfillMetaFilter) filterBlocks(blks []*db.Block) []*db.Block {
	tp.mu.Lock()
	defer tp.mu.Unlock()

	res := make([]*db.Block, 0, len(blks))
	for _, blk := range blks {
		blkMint, blkMaxt := blk.Timerange()
		if util.Contains(min(tp.mint+tp.overlap, tp.maxt), tp.maxt, blkMint, blkMaxt) {
			continue
		}
		res = append(res, blk)
	}
	return res
}

func (tp *ThanosBackfillMetaFilter) Update(ctx context.Context) error {
	// Note: we assume that thanos runs close to this server, we dont need TLS here
	cc, err := grpc.NewClient(tp.endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("unable to connect: %w", err)
	}
	client := infopb.NewInfoClient(cc)

	info, err := client.Info(ctx, &infopb.InfoRequest{})
	if err != nil {
		return fmt.Errorf("unable to get store time range from thanos: %w", err)
	}

	tp.mu.Lock()
	defer tp.mu.Unlock()

	tp.mint = info.Store.MinTime
	tp.maxt = info.Store.MaxTime

	return nil
}
