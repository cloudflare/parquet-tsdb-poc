// Copyright (c) 2025 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

package locate

import (
	"slices"
	"testing"
	"time"

	"github.com/thanos-io/objstore/providers/filesystem"

	"github.com/cloudflare/parquet-tsdb-poc/db"
	"github.com/cloudflare/parquet-tsdb-poc/internal/util"
	"github.com/cloudflare/parquet-tsdb-poc/schema"
)

func TestSyncer(t *testing.T) {
	t.Run("Syncer always has the blocks from the discovered metas", func(tt *testing.T) {
		ctx := tt.Context()
		bkt, err := filesystem.NewBucket(tt.TempDir())
		if err != nil {
			tt.Fatalf("unable to create bucket: %s", err)
		}
		syncer := NewSyncer(bkt)

		d := util.BeginOfDay(time.UnixMilli(0)).UTC()
		if err := createBlockForDay(ctx, tt, bkt, d); err != nil {
			tt.Fatalf("unable to create block for day: %s", err)
		}

		m := map[string]schema.Meta{
			"1970/01/01": {
				Version: schema.V1,
				Name:    "1970/01/01",
				Mint:    d.UnixMilli(),
				Maxt:    d.AddDate(0, 0, 1).UnixMilli(),
				Shards:  1,
			},
		}

		if err := syncer.Sync(ctx, m); err != nil {
			tt.Fatalf("unable to sync blocks: %s", err)
		}

		if expect, got := []string{"1970/01/01"}, syncer.Blocks(); !slices.EqualFunc(expect, got, func(l string, r *db.Block) bool { return l == r.Meta().Name }) {
			tt.Errorf("expected: %+v, got: %+v", expect, got)
		}

		delete(m, "1970/01/01")
		if err := syncer.Sync(ctx, m); err != nil {
			tt.Fatalf("unable to sync blocks: %s", err)
		}

		if expect, got := []string{}, syncer.Blocks(); !slices.EqualFunc(expect, got, func(l string, r *db.Block) bool { return l == r.Meta().Name }) {
			tt.Errorf("expected: %+v, got: %+v", expect, got)
		}
	})
}
