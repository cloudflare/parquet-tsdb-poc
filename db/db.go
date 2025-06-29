// Copyright (c) 2025 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

package db

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"

	"github.com/alecthomas/units"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/annotations"
	"go.opentelemetry.io/otel/attribute"
	"golang.org/x/sync/errgroup"

	"github.com/cloudflare/parquet-tsdb-poc/internal/limits"
	"github.com/cloudflare/parquet-tsdb-poc/internal/tracing"
	"github.com/cloudflare/parquet-tsdb-poc/internal/util"
	"github.com/cloudflare/parquet-tsdb-poc/internal/warnings"
)

// DB is a horizontal partitioning of multiple non-overlapping blocks that are
// aligned to 24h and span exactely 24h.
type DB struct {
	syncer    syncer
	extLabels labels.Labels
}

type syncer interface {
	Blocks() []*Block
}

type dbConfig struct {
	extLabels labels.Labels
}

type DBOption func(*dbConfig)

func ExternalLabels(extlabels labels.Labels) DBOption {
	return func(cfg *dbConfig) {
		cfg.extLabels = extlabels
	}
}

func NewDB(syncer syncer, opts ...DBOption) *DB {
	cfg := dbConfig{extLabels: labels.EmptyLabels()}
	for _, o := range opts {
		o(&cfg)
	}
	return &DB{syncer: syncer, extLabels: cfg.extLabels}
}

func (db *DB) Timerange() (int64, int64) {
	blocks := db.syncer.Blocks()

	mint := int64(math.MaxInt64)
	maxt := int64(math.MinInt64)

	for _, blk := range blocks {
		bmint, bmaxt := blk.Timerange()
		mint = min(mint, bmint)
		maxt = max(maxt, bmaxt)
	}
	return mint, maxt
}

func (db *DB) Extlabels() labels.Labels {
	return db.extLabels
}

type queryableConfig struct {
	replicaLabelsNames    []string
	selectChunkBytesQuota *limits.Quota
	selectRowCountQuota   *limits.Quota

	selectChunkPartitionMaxRange       uint64
	selectChunkPartitionMaxGap         uint64
	selectChunkPartitionMaxConcurrency int
}

type QueryableOption func(*queryableConfig)

func DropReplicaLabels(s ...string) QueryableOption {
	return func(cfg *queryableConfig) {
		cfg.replicaLabelsNames = append(cfg.replicaLabelsNames, s...)
	}
}

func SelectChunkBytesQuota(maxBytes units.Base2Bytes) QueryableOption {
	return func(cfg *queryableConfig) {
		cfg.selectChunkBytesQuota = limits.NewQuota(int64(maxBytes))
	}
}

func SelectRowCountQuota(maxRows int64) QueryableOption {
	return func(cfg *queryableConfig) {
		cfg.selectRowCountQuota = limits.NewQuota(int64(maxRows))
	}
}

func SelectChunkPartitionMaxRange(maxRange units.Base2Bytes) QueryableOption {
	return func(cfg *queryableConfig) {
		cfg.selectChunkPartitionMaxRange = uint64(maxRange)
	}
}

func SelectChunkPartitionMaxGap(maxGap units.Base2Bytes) QueryableOption {
	return func(cfg *queryableConfig) {
		cfg.selectChunkPartitionMaxGap = uint64(maxGap)
	}
}

func SelectChunkPartitionMaxConcurrency(n int) QueryableOption {
	return func(cfg *queryableConfig) {
		cfg.selectChunkPartitionMaxConcurrency = n
	}
}

// Queryable returns a storage.Queryable that drops replica labels at runtime. Replica labels are
// labels that identify a replica, i.e. one member of an HA pair of Prometheus servers. Thanos
// might request at query time to drop those labels so that we can deduplicate results into one view.
// Common replica labels are 'prometheus', 'host', etc.
// It also enforces various quotas over its lifetime.
func (db *DB) Queryable(opts ...QueryableOption) storage.Queryable {
	cfg := queryableConfig{
		selectChunkBytesQuota:              limits.UnlimitedQuota(),
		selectRowCountQuota:                limits.UnlimitedQuota(),
		selectChunkPartitionMaxRange:       math.MaxUint64,
		selectChunkPartitionMaxGap:         math.MaxUint64,
		selectChunkPartitionMaxConcurrency: 0,
	}
	for i := range opts {
		opts[i](&cfg)
	}

	return &DBQueryable{
		blocks:                             db.syncer.Blocks(),
		extLabels:                          db.extLabels,
		replicaLabelNames:                  cfg.replicaLabelsNames,
		selectChunkBytesQuota:              cfg.selectChunkBytesQuota,
		selectRowCountQuota:                cfg.selectRowCountQuota,
		selectChunkPartitionMaxRange:       cfg.selectChunkPartitionMaxRange,
		selectChunkPartitionMaxGap:         cfg.selectChunkPartitionMaxGap,
		selectChunkPartitionMaxConcurrency: cfg.selectChunkPartitionMaxConcurrency,
	}
}

type DBQueryable struct {
	blocks []*Block

	// extLabels are added to all series in the result set overriding any internal labels.
	extLabels labels.Labels

	// replicaLabelNames are names of labels that identify replicas, they are dropped
	// after extLabels were applied.
	replicaLabelNames []string

	// selectChunkBytesQuota is the limit of bytes that "Select" calls can fetch from chunk columns.
	selectChunkBytesQuota *limits.Quota

	// selectRowCountQuota is the limit of rows that "Select" calls can touch.
	selectRowCountQuota *limits.Quota

	// selectChunkPartitionMaxRange is the maximum range of chunk pages that get coalesced into a
	// range that is concurrently scheduled to be fetched from object storage.
	selectChunkPartitionMaxRange uint64

	// selectChunkPartitionMaxGap is the maximum gap that we tolerate when coalescing nearby pages into ranges.
	selectChunkPartitionMaxGap uint64

	// selectChunkPartitionMaxConcurrency is the maximum amount of parallel object storage requests we run per select.
	selectChunkPartitionMaxConcurrency int
}

func (db *DBQueryable) Querier(mint, maxt int64) (storage.Querier, error) {
	qs := make([]storage.Querier, 0, len(db.blocks))
	for _, blk := range db.blocks {
		bmint, bmaxt := blk.Timerange()
		if !util.Intersects(mint, maxt, bmint, bmaxt) {
			continue
		}
		start, end := util.Intersection(mint, maxt, bmint, bmaxt)
		q, err := blk.Queryable(
			db.extLabels,
			db.replicaLabelNames,
			db.selectChunkBytesQuota,
			db.selectRowCountQuota,
			db.selectChunkPartitionMaxRange,
			db.selectChunkPartitionMaxGap,
			db.selectChunkPartitionMaxConcurrency,
		).Querier(start, end)
		if err != nil {
			return nil, fmt.Errorf("unable to get block querier: %w", err)
		}
		qs = append(qs, q)
	}
	return &DBQuerier{mint: mint, maxt: maxt, blocks: qs}, nil
}

type DBQuerier struct {
	mint, maxt int64

	blocks []storage.Querier
}

var _ storage.Querier = &DBQuerier{}

func (q DBQuerier) Close() error {
	errs := make([]error, 0)
	for i, q := range q.blocks {
		if err := q.Close(); err != nil {
			errs = append(errs, fmt.Errorf("unable to close block %q: %w", i, err))
		}
	}
	return errors.Join(errs...)
}

func (q DBQuerier) LabelValues(ctx context.Context, name string, hints *storage.LabelHints, matchers ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	var annos annotations.Annotations

	var mu sync.Mutex
	g, ctx := errgroup.WithContext(ctx)

	res := make([]string, 0)
	for _, blk := range q.blocks {
		g.Go(func() error {
			lvals, lannos, err := blk.LabelValues(ctx, name, hints, matchers...)
			if err != nil {
				return fmt.Errorf("unable to query label values for block: %w", err)
			}
			annos = annos.Merge(lannos)
			mu.Lock()
			res = append(res, lvals...)
			mu.Unlock()

			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return nil, nil, fmt.Errorf("unable to query label values: %w", err)
	}

	limit := hints.Limit

	res = util.SortUnique(res)
	if limit > 0 && len(res) > limit {
		res = res[:limit]
		annos = annos.Add(warnings.ErrorTruncatedResponse)
	}
	return res, annos, nil
}

func (q DBQuerier) LabelNames(ctx context.Context, hints *storage.LabelHints, matchers ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	var annos annotations.Annotations

	var mu sync.Mutex
	g, ctx := errgroup.WithContext(ctx)

	res := make([]string, 0)
	for _, blk := range q.blocks {
		g.Go(func() error {
			lnames, lannos, err := blk.LabelNames(ctx, hints, matchers...)
			if err != nil {
				return fmt.Errorf("unable to query label names for block: %w", err)
			}
			annos = annos.Merge(lannos)
			mu.Lock()
			res = append(res, lnames...)
			mu.Unlock()

			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return nil, nil, fmt.Errorf("unable to query label names: %w", err)
	}

	limit := hints.Limit

	res = util.SortUnique(res)
	if limit > 0 && len(res) > limit {
		res = res[:limit]
		annos = annos.Add(warnings.ErrorTruncatedResponse)
	}
	return res, annos, nil
}

func (q DBQuerier) Select(ctx context.Context, sorted bool, hints *storage.SelectHints, matchers ...*labels.Matcher) storage.SeriesSet {
	return newLazySeriesSet(ctx, q.selectFn, sorted, hints, matchers...)
}

func (q DBQuerier) selectFn(ctx context.Context, sorted bool, hints *storage.SelectHints, matchers ...*labels.Matcher) storage.SeriesSet {
	ctx, span := tracing.Tracer().Start(ctx, "Select DB")
	defer span.End()

	span.SetAttributes(attribute.Bool("sorted", sorted))
	span.SetAttributes(attribute.StringSlice("matchers", matchersToStringSlice(matchers)))
	span.SetAttributes(attribute.Int("block.shards", len(q.blocks)))

	// If we need to merge multiple series sets vertically we need them sorted
	sorted = sorted || len(q.blocks) > 1

	sss := make([]storage.SeriesSet, 0, len(q.blocks))
	for _, q := range q.blocks {
		sss = append(sss, q.Select(ctx, sorted, hints, matchers...))
	}

	if len(sss) == 0 {
		return storage.EmptySeriesSet()
	}
	return storage.NewMergeSeriesSet(sss, hints.Limit, storage.ChainedSeriesMerge)
}
