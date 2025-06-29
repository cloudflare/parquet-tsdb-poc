// Copyright (c) 2025 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

package search

import (
	"context"
	"fmt"
	"io"
	"slices"
	"strings"
	"sync"

	"golang.org/x/sync/errgroup"

	"github.com/parquet-go/parquet-go"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/util/annotations"

	"github.com/cloudflare/parquet-tsdb-poc/internal/limits"
	"github.com/cloudflare/parquet-tsdb-poc/internal/util"
	"github.com/cloudflare/parquet-tsdb-poc/internal/warnings"
	"github.com/cloudflare/parquet-tsdb-poc/schema"
)

type SeriesChunks struct {
	LsetHash uint64
	Lset     labels.Labels
	Chunks   []chunks.Meta
}

type SelectReadMeta struct {
	// The actual data for this Select call and metadata for how to read it
	Meta       schema.Meta
	LabelPfile *parquet.File
	ChunkPfile *parquet.File

	// We smuggle a bucket reader here so we can create a prepared ReaderAt
	// for chunks that is scoped to this query and can be traced.
	ChunkFileReaderFromContext func(ctx context.Context) io.ReaderAt

	// Quotas for the query that issued this Select call
	ChunkBytesQuota *limits.Quota
	RowCountQuota   *limits.Quota

	// Hints about how we should partition page ranges for chunks.
	// Page ranges are coalesced into a partition that is more efficient
	// to read in object storage. I.e we will merge adjacent (up to a max gap) Pages
	// into bigger ranges (up to a max size). These ranges are scheduled to be read
	// concurrently.
	ChunkPagePartitionMaxRange       uint64
	ChunkPagePartitionMaxGap         uint64
	ChunkPagePartitionMaxConcurrency int

	// Thanos labels processing hints
	ExternalLabels    labels.Labels
	ReplicaLabelNames []string
}

func Select(
	ctx context.Context,
	meta SelectReadMeta,
	mint int64,
	maxt int64,
	hints *storage.SelectHints,
	ms ...*labels.Matcher,
) ([]SeriesChunks, annotations.Annotations, error) {
	ctx = contextWithMethod(ctx, methodSelect)

	ms, ok := matchExternalLabels(meta.ExternalLabels, ms)
	if !ok {
		return nil, nil, nil
	}

	// label and chunk files have same number of rows and rowgroups, just pick either
	labelRowGroups := meta.LabelPfile.RowGroups()
	numRowGroups := len(labelRowGroups)

	var (
		mu    sync.Mutex
		annos annotations.Annotations
	)

	res := make([]SeriesChunks, 0)

	g, ctx := errgroup.WithContext(ctx)
	for i := range numRowGroups {
		g.Go(func() error {
			cs, err := matchersToConstraint(ms...)
			if err != nil {
				return fmt.Errorf("unable to convert matchers to constraints: %w", err)
			}
			if err := initialize(meta.LabelPfile.Schema(), cs...); err != nil {
				return fmt.Errorf("unable to initialize constraints: %w", err)
			}

			rrs, err := filter(ctx, labelRowGroups[i], cs...)
			if err != nil {
				return fmt.Errorf("unable to compute ranges: %w", err)
			}
			if len(rrs) == 0 {
				return nil
			}
			series, warns, err := materializeSeries(
				ctx,
				meta,
				i,
				mint,
				maxt,
				hints,
				rrs,
			)
			if err != nil {
				return fmt.Errorf("unable to materialize series: %w", err)
			}

			mu.Lock()
			res = append(res, series...)
			annos = annos.Merge(warns)
			mu.Unlock()

			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return nil, annos, fmt.Errorf("unable to process row groups: %w", err)
	}
	return res, annos, nil
}

type LabelValuesReadMeta struct {
	// The actual data for this LabelValues call and metadata for how to read it
	Meta       schema.Meta
	LabelPfile *parquet.File

	// Thanos labels processing hints
	ExternalLabels    labels.Labels
	ReplicaLabelNames []string
}

func LabelValues(
	ctx context.Context,
	meta LabelValuesReadMeta,
	name string,
	hints *storage.LabelHints,
	ms ...*labels.Matcher,
) ([]string, annotations.Annotations, error) {
	ctx = contextWithMethod(ctx, methodLabelValues)

	ms, ok := matchExternalLabels(meta.ExternalLabels, ms)
	if !ok {
		return nil, nil, nil
	}
	extVal := externalLabelValues(meta.ExternalLabels, meta.ReplicaLabelNames, name)

	var (
		res   []string
		annos annotations.Annotations
	)

	if len(ms) == 0 {
		// No matchers means we can read label values from the column dictionaries
		vals := make([]string, 0)
		for _, rg := range meta.LabelPfile.RowGroups() {
			lc, ok := rg.Schema().Lookup(schema.LabelNameToColumn(name))
			if !ok {
				continue
			}
			pg := rg.ColumnChunks()[lc.ColumnIndex].Pages()
			defer pg.Close()

			p, err := pg.ReadPage()
			if err != nil {
				return nil, annos, fmt.Errorf("unable to read page: %w", err)
			}
			d := p.Dictionary()

			for i := range d.Len() {
				vals = append(vals, d.Index(int32(i)).Clone().String())
			}
		}
		if len(extVal) != 0 {
			if len(vals) != 0 {
				annos = annos.Add(warnings.ErrorDroppedLabelValuesAfterExternalLabelMangling)
			}
			res = append(res, extVal)
		} else {
			res = append(res, vals...)
		}
	} else {
		// matchers means we need to actually read values of the column
		labelRowGroups := meta.LabelPfile.RowGroups()
		numRowGroups := len(labelRowGroups)

		var mu sync.Mutex

		g, ctx := errgroup.WithContext(ctx)
		for i := range numRowGroups {
			g.Go(func() error {
				cs, err := matchersToConstraint(ms...)
				if err != nil {
					return fmt.Errorf("unable to convert matchers to constraints: %w", err)
				}
				if err := initialize(meta.LabelPfile.Schema(), cs...); err != nil {
					return fmt.Errorf("unable to initialize constraints: %w", err)
				}

				rrs, err := filter(ctx, labelRowGroups[i], cs...)
				if err != nil {
					return fmt.Errorf("unable to compute ranges: %w", err)
				}
				if len(rrs) == 0 {
					return nil
				}

				labelValues, warns, err := materializeLabelValues(ctx, meta, name, i, rrs)
				if err != nil {
					return fmt.Errorf("unable to materialize label values: %w", err)
				}

				mu.Lock()
				defer mu.Unlock()

				annos = annos.Merge(warns)

				if len(extVal) != 0 {
					if len(labelValues) != 0 {
						annos = annos.Add(warnings.ErrorDroppedLabelValuesAfterExternalLabelMangling)
					}
					res = append(res, extVal)
				} else {
					res = append(res, labelValues...)
				}
				return nil
			})
		}

		if err := g.Wait(); err != nil {
			return nil, annos, fmt.Errorf("unable to process row groups: %w", err)
		}
	}
	limit := hints.Limit

	res = util.SortUnique(res)
	if limit > 0 && len(res) > limit {
		res = res[:limit]
		annos = annos.Add(warnings.ErrorTruncatedResponse)
	}
	return res, annos, nil
}

type LabelNamesReadMeta struct {
	// The actual data for this LabelNames call and metadata for how to read it
	Meta       schema.Meta
	LabelPfile *parquet.File

	// Thanos labels processing hints
	ExternalLabels    labels.Labels
	ReplicaLabelNames []string
}

func LabelNames(
	ctx context.Context,
	meta LabelNamesReadMeta,
	hints *storage.LabelHints,
	ms ...*labels.Matcher,
) ([]string, annotations.Annotations, error) {
	ctx = contextWithMethod(ctx, methodLabelNames)

	ms, ok := matchExternalLabels(meta.ExternalLabels, ms)
	if !ok {
		return nil, nil, nil
	}
	var (
		res   []string
		annos annotations.Annotations
	)

	if len(ms) == 0 {
		// No matchers means we can read label names from the schema
		for _, c := range meta.LabelPfile.Schema().Columns() {
			if strings.HasPrefix(c[0], schema.LabelColumnPrefix) {
				res = append(res, schema.ColumnToLabelName(c[0]))
			}
		}
		res = append(res, externalLabelNames(meta.ExternalLabels, meta.ReplicaLabelNames)...)
	} else {
		// matchers means we need to fetch label names for matching rows from the table
		labelRowGroups := meta.LabelPfile.RowGroups()
		numRowGroups := len(labelRowGroups)

		var mu sync.Mutex

		g, ctx := errgroup.WithContext(ctx)
		for i := range numRowGroups {
			g.Go(func() error {
				cs, err := matchersToConstraint(ms...)
				if err != nil {
					return fmt.Errorf("unable to convert matchers to constraints: %w", err)
				}
				if err := initialize(meta.LabelPfile.Schema(), cs...); err != nil {
					return fmt.Errorf("unable to initialize constraints: %w", err)
				}

				rrs, err := filter(ctx, labelRowGroups[i], cs...)
				if err != nil {
					return fmt.Errorf("unable to compute ranges: %w", err)
				}
				if len(rrs) == 0 {
					return nil
				}
				labelNames, warns, err := materializeLabelNames(ctx, meta, i, rrs)
				if err != nil {
					return fmt.Errorf("unable to materialize label names: %w", err)
				}

				mu.Lock()
				defer mu.Unlock()

				res = append(res, externalLabelNames(meta.ExternalLabels, meta.ReplicaLabelNames)...)
				res = append(res, labelNames...)
				annos = annos.Merge(warns)

				return nil
			})
		}
		if err := g.Wait(); err != nil {
			return nil, annos, fmt.Errorf("unable to process row groups: %w", err)
		}
	}
	limit := hints.Limit

	res = util.SortUnique(res)
	if limit > 0 && len(res) > limit {
		res = res[:limit]
		annos = annos.Add(warnings.ErrorTruncatedResponse)
	}
	return res, annos, nil
}

func matchExternalLabels(extLabels labels.Labels, matchers []*labels.Matcher) ([]*labels.Matcher, bool) {
	// if the matchers match on some external label, we need to consume that matcher here
	remain := make([]*labels.Matcher, 0, len(matchers))
	for i := range matchers {
		exclude, consumed := false, false
		extLabels.Range(func(lbl labels.Label) {
			if matchers[i].Name != lbl.Name {
				return
			}
			exclude = exclude || !matchers[i].Matches(lbl.Value)
			consumed = true
		})
		if exclude {
			return nil, false
		}
		if !consumed {
			remain = append(remain, matchers[i])
		}
	}
	return remain, true
}

func externalLabelValues(extLabels labels.Labels, replicaLabelNames []string, name string) string {
	extVal := ""
	extLabels.Range(func(lbl labels.Label) {
		if slices.Contains(replicaLabelNames, lbl.Name) {
			return
		}
		if lbl.Name != name {
			return
		}
		extVal = lbl.Value
	})
	return extVal
}

func externalLabelNames(extLabels labels.Labels, replicaLabelNames []string) []string {
	res := make([]string, 0, extLabels.Len())
	extLabels.Range(func(lbl labels.Label) {
		if slices.Contains(replicaLabelNames, lbl.Name) {
			return
		}
		res = append(res, lbl.Name)
	})
	return res
}
