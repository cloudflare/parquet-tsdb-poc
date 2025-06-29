// Copyright (c) 2025 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

package locate

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"sync"

	"github.com/alecthomas/units"
	"github.com/parquet-go/parquet-go"
	"github.com/prometheus/prometheus/tsdb/fileutil"
	"github.com/thanos-io/objstore"

	"github.com/cloudflare/parquet-tsdb-poc/db"
	"github.com/cloudflare/parquet-tsdb-poc/schema"
)

type Syncer struct {
	bkt objstore.Bucket

	blockOpts   []BlockOption
	metaFilter  MetaFilter
	concurrency int

	mu     sync.Mutex
	blocks map[string]*db.Block

	cached []*db.Block
}

type SyncerOption func(*syncerConfig)

type syncerConfig struct {
	blockOpts   []BlockOption
	metaFilter  MetaFilter
	concurrency int
}

func BlockOptions(opts ...BlockOption) SyncerOption {
	return func(cfg *syncerConfig) {
		cfg.blockOpts = opts
	}
}

func FilterMetas(f MetaFilter) SyncerOption {
	return func(cfg *syncerConfig) {
		cfg.metaFilter = f
	}
}

func BlockConcurrency(c int) SyncerOption {
	return func(cfg *syncerConfig) {
		cfg.concurrency = c
	}
}

type BlockOption func(*blockConfig)

type blockConfig struct {
	readBufferSize units.Base2Bytes
	labelFilesDir  string
}

func ReadBufferSize(sz units.Base2Bytes) BlockOption {
	return func(cfg *blockConfig) {
		cfg.readBufferSize = sz
	}
}

func LabelFilesDir(d string) BlockOption {
	return func(cfg *blockConfig) {
		cfg.labelFilesDir = d
	}
}

func NewSyncer(bkt objstore.Bucket, opts ...SyncerOption) *Syncer {
	cfg := syncerConfig{
		metaFilter:  AllMetasMetaFilter,
		concurrency: 1,
	}

	for _, o := range opts {
		o(&cfg)
	}

	return &Syncer{
		bkt:         bkt,
		blocks:      make(map[string]*db.Block),
		blockOpts:   cfg.blockOpts,
		metaFilter:  cfg.metaFilter,
		concurrency: cfg.concurrency,
	}
}

func (s *Syncer) Blocks() []*db.Block {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.filterBlocks(s.cached)
}

func (s *Syncer) Sync(ctx context.Context, metas map[string]schema.Meta) error {
	type blockOrError struct {
		blk *db.Block
		err error
	}

	blkC := make(chan blockOrError)
	go func() {
		defer close(blkC)

		workerC := make(chan schema.Meta, s.concurrency)
		go func() {
			defer close(workerC)

			for k, v := range s.filterMetas(metas) {
				if _, ok := s.blocks[k]; ok {
					continue
				}
				workerC <- v
			}
		}()

		var wg sync.WaitGroup
		defer wg.Wait()

		for range s.concurrency {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for m := range workerC {
					blk, err := newBlockForMeta(ctx, s.bkt, m, s.blockOpts...)
					if err != nil {
						blkC <- blockOrError{err: fmt.Errorf("unable to read block %q: %w", m.Name, err)}
					} else {
						blkC <- blockOrError{blk: blk}
					}
				}
			}()
		}
	}()

	blocks := make(map[string]*db.Block, 0)
	for b := range blkC {
		if b.err != nil {
			return fmt.Errorf("unable to read block: %w", b.err)
		}
		blocks[b.blk.Meta().Name] = b.blk
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// delete blocks that are not in meta map
	maps.DeleteFunc(s.blocks, func(k string, _ *db.Block) bool { _, ok := metas[k]; return !ok })

	// add new blocks that we just loaded
	maps.Copy(s.blocks, blocks)

	s.cached = slices.Collect(maps.Values(s.blocks))
	sort.Slice(s.cached, func(i, j int) bool {
		ls, _ := s.cached[i].Timerange()
		rs, _ := s.cached[j].Timerange()
		return ls < rs
	})

	if len(s.cached) != 0 {
		syncMinTime.WithLabelValues(whatSyncer).Set(float64(s.cached[0].Meta().Mint))
		syncMaxTime.WithLabelValues(whatSyncer).Set(float64(s.cached[len(s.cached)-1].Meta().Maxt))
	}
	syncLastSuccessfulTime.WithLabelValues(whatSyncer).SetToCurrentTime()

	return nil
}

func (s *Syncer) filterMetas(metas map[string]schema.Meta) map[string]schema.Meta {
	return s.metaFilter.filterMetas(metas)
}

func (s *Syncer) filterBlocks(blks []*db.Block) []*db.Block {
	return s.metaFilter.filterBlocks(blks)
}

/*
TODO: the following functions should be abstracted into syncer somehow, the syncer should decide
where shards live (s3, disk, memory) and should also do regular maintenance on its on disk state,
i.e. downloading files that it wants on disk, deleting files that are out of retention, etc.
*/

func newBlockForMeta(ctx context.Context, bkt objstore.Bucket, m schema.Meta, opts ...BlockOption) (*db.Block, error) {
	cfg := blockConfig{
		readBufferSize: 8 * units.MiB,
		labelFilesDir:  os.TempDir(),
	}
	for _, o := range opts {
		o(&cfg)
	}

	shards, err := readShards(ctx, bkt, m, cfg)
	if err != nil {
		return nil, fmt.Errorf("unable to read shards: %w", err)
	}

	return db.NewBlock(m, shards...), nil
}

func readShards(ctx context.Context, bkt objstore.Bucket, m schema.Meta, cfg blockConfig) ([]*db.Shard, error) {
	shards := make([]*db.Shard, 0, m.Shards)
	for i := range int(m.Shards) {
		shard, err := readShard(ctx, bkt, m, i, cfg)
		if err != nil {
			return nil, fmt.Errorf("unable to read shard %d: %w", i, err)
		}
		shards = append(shards, shard)
	}
	return shards, nil
}

func bucketReaderFromContext(bkt objstore.Bucket, name string) func(context.Context) io.ReaderAt {
	return func(ctx context.Context) io.ReaderAt {
		return newBucketReaderAt(ctx, bkt, name)
	}
}

func readShard(ctx context.Context, bkt objstore.Bucket, m schema.Meta, i int, cfg blockConfig) (s *db.Shard, err error) {
	chunkspfile := schema.ChunksPfileNameForShard(m.Name, i)
	attrs, err := bkt.Attributes(ctx, chunkspfile)
	if err != nil {
		return nil, fmt.Errorf("unable to attr chunks parquet file %q: %w", chunkspfile, err)
	}

	bktRdrAtFromCtx := bucketReaderFromContext(bkt, chunkspfile)

	chunkspf, err := parquet.OpenFile(bktRdrAtFromCtx(ctx), attrs.Size,
		parquet.FileReadMode(parquet.ReadModeAsync),
		parquet.ReadBufferSize(int(cfg.readBufferSize)),
		parquet.SkipMagicBytes(true),
		parquet.SkipBloomFilters(true),
		parquet.OptimisticRead(true),
	)
	if err != nil {
		return nil, fmt.Errorf("unable to open chunks parquet file %q: %w", chunkspfile, err)
	}

	labelspfile := schema.LabelsPfileNameForShard(m.Name, i)
	labelspfilePath := filepath.Join(cfg.labelFilesDir, url.PathEscape(labelspfile))

	// If we were not able to read the file for any reason we delete it and retry.
	// This is also executed on paths that have nothing to do with the file, i.e. loading
	// its content from object storage, but just makes sure that we dont forget cleanup.
	defer func() {
		if err != nil {
			if cerr := os.RemoveAll(labelspfilePath); cerr != nil {
				err = errors.Join(err, fmt.Errorf("unable to remove labels parquet file %q: %w", labelspfilePath, cerr))
			}
		}
	}()

	// if the file was corrupted on its way to disk we remove it and will retry downloading it next try
	if stat, err := os.Stat(labelspfilePath); err != nil {
		if !os.IsNotExist(err) {
			return nil, fmt.Errorf("unable to stat label parquet file %q from disk: %w", labelspfile, err)
			// file didnt exist - we need to download and save it to disk
		}
	} else {
		f, err := fileutil.OpenMmapFileWithSize(labelspfilePath, int(stat.Size()))
		if err != nil {
			return nil, fmt.Errorf("unable to mmap label parquet file %q: %w", labelspfile, err)
		}
		labelspf, err := parquet.OpenFile(bytes.NewReader(f.Bytes()), stat.Size())
		if err != nil {
			syncCorruptedLabelFile.Add(1)
			rerr := fmt.Errorf("unable to read label parquet file %q: %w", labelspfile, err)
			if cerr := f.Close(); cerr != nil {
				return nil, errors.Join(rerr, fmt.Errorf("unable to close memory mapped parquet file %q: %w", labelspfile, cerr))
			}
			return nil, rerr
		}
		return db.NewShard(m, chunkspf, labelspf, bktRdrAtFromCtx), nil
	}
	rdr, err := bkt.Get(ctx, labelspfile)
	if err != nil {
		return nil, fmt.Errorf("unable to get %q: %w", labelspfile, err)
	}
	defer rdr.Close()

	f, err := os.Create(labelspfilePath)
	if err != nil {
		return nil, fmt.Errorf("unable to create label parquet file %q on disk: %w", labelspfile, err)
	}
	defer f.Close()

	n, err := io.Copy(f, rdr)
	if err != nil {
		return nil, fmt.Errorf("unable to copy label parquet file %q to disk: %w", labelspfile, err)
	}
	if err := f.Sync(); err != nil {
		return nil, fmt.Errorf("unable to close label parquet file %q: %w", labelspfile, err)
	}

	mf, err := fileutil.OpenMmapFileWithSize(labelspfilePath, int(n))
	if err != nil {
		return nil, fmt.Errorf("unable to mmap label parquet file %q: %w", labelspfile, err)
	}
	labelspf, err := parquet.OpenFile(bytes.NewReader(mf.Bytes()), int64(n))
	if err != nil {
		syncCorruptedLabelFile.Add(1)
		rerr := fmt.Errorf("unable to read label parquet file %q: %w", labelspfile, err)
		if cerr := f.Close(); cerr != nil {
			return nil, errors.Join(rerr, fmt.Errorf("unable to close memory mapped parquet file %q: %w", labelspfile, cerr))
		}
		return nil, rerr
	}
	return db.NewShard(m, chunkspf, labelspf, bktRdrAtFromCtx), nil
}
