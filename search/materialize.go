// Copyright (c) 2025 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

package search

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"iter"
	"maps"
	"math"

	"slices"
	"sync"
	"time"

	"github.com/parquet-go/parquet-go"
	"go.opentelemetry.io/otel/attribute"
	"golang.org/x/sync/errgroup"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/util/annotations"

	"github.com/cloudflare/parquet-tsdb-poc/internal/encoding"
	"github.com/cloudflare/parquet-tsdb-poc/internal/limits"
	"github.com/cloudflare/parquet-tsdb-poc/internal/tracing"
	"github.com/cloudflare/parquet-tsdb-poc/internal/warnings"
	"github.com/cloudflare/parquet-tsdb-poc/schema"
)

// materializeSeries reconstructs the ChunkSeries that belong to the specified row ranges (rr).
// It uses the row group index (rgi) and time bounds (mint, maxt) to filter and decode the series.
func materializeSeries(
	ctx context.Context,
	m SelectReadMeta,
	rgi int,
	mint,
	maxt int64,
	hints *storage.SelectHints,
	rr []rowRange,
) ([]SeriesChunks, annotations.Annotations, error) {
	var annos annotations.Annotations

	if limit := hints.Limit; limit > 0 {
		annos.Add(warnings.ErrorTruncatedResponse)
		// Series are all different so we can actually limit the rowranges themselves
		// This would not work for Label APIs but for series its ok.
		rr = limitRowRanges(int64(limit), rr)
	}

	if err := checkRowQuota(m.RowCountQuota, rr); err != nil {
		return nil, annos, err
	}
	res := make([]SeriesChunks, totalRows(rr))

	g, ctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		sLbls, err := materializeLabels(ctx, m, rgi, rr)
		if err != nil {
			return fmt.Errorf("error materializing labels: %w", err)
		}

		for i, s := range sLbls {
			m.ExternalLabels.Range(func(lbl labels.Label) { s.Set(lbl.Name, lbl.Value) })
			s.Del(m.ReplicaLabelNames...)

			lbls := s.Labels()
			h := lbls.Hash()

			res[i].Lset = lbls
			res[i].LsetHash = h
		}
		return nil
	})

	g.Go(func() error {
		if hints.Func == "series" {
			return nil
		}
		chks, err := materializeChunks(ctx, m, rgi, mint, maxt, rr)
		if err != nil {
			return fmt.Errorf("unable to materialize chunks: %w", err)
		}

		for i, c := range chks {
			for j := range c {
				res[i].Chunks = append(res[i].Chunks, c[j])
			}
		}
		return nil
	})

	if err := g.Wait(); err != nil {
		return res, annos, fmt.Errorf("unable to materialize series: %w", err)
	}
	return res, annos, nil
}

func materializeLabels(ctx context.Context, m SelectReadMeta, rgi int, rr []rowRange) ([]labels.Builder, error) {
	switch v := m.Meta.Version; v {
	case schema.V0:
		return materializeLabelsV0(ctx, m, rgi, rr)
	case schema.V1, schema.V2:
		return materializeLabelsV1(ctx, m, rgi, rr)
	default:
		return nil, fmt.Errorf("unable to materialize labels for block of version %q", v)
	}
}

// v0 blocks have a map to resolve column names for a metric
func materializeLabelsV0(ctx context.Context, m SelectReadMeta, rgi int, rr []rowRange) ([]labels.Builder, error) {
	rowCount := totalRows(rr)

	metricNameColumn := schema.LabelNameToColumn(labels.MetricName)

	lc, ok := m.LabelPfile.Schema().Lookup(metricNameColumn)
	if !ok {
		return nil, fmt.Errorf("unable to to find column %q", metricNameColumn)
	}

	rg := m.LabelPfile.RowGroups()[rgi]

	cc := rg.ColumnChunks()[lc.ColumnIndex]
	metricNames, err := materializeLabelColumn(ctx, rg, cc, rr)
	if err != nil {
		return nil, fmt.Errorf("unable to materialize metric name column: %w", err)
	}

	seen := make(map[string]struct{})
	colsMap := make(map[int]*[]parquet.Value, 10)

	v := make([]parquet.Value, 0, rowCount)
	colsMap[lc.ColumnIndex] = &v
	for _, nm := range metricNames {
		key := yoloString(nm.ByteArray())
		if _, ok := seen[key]; !ok {
			cols := m.Meta.ColumnsForName[key]
			for _, c := range cols {
				lc, ok := m.LabelPfile.Schema().Lookup(c)
				if !ok {
					continue
				}
				colsMap[lc.ColumnIndex] = &[]parquet.Value{}
			}
			seen[key] = struct{}{}
		}
	}

	g, ctx := errgroup.WithContext(ctx)
	for cIdx, v := range colsMap {
		g.Go(func() error {
			cc := rg.ColumnChunks()[cIdx]
			values, err := materializeLabelColumn(ctx, rg, cc, rr)
			if err != nil {
				return fmt.Errorf("unable to materialize labels values: %w", err)
			}
			*v = values
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return nil, err
	}

	builders := make([]labels.Builder, rowCount)
	for cIdx, values := range colsMap {
		colName := m.LabelPfile.Schema().Columns()[cIdx][0]
		labelName := schema.ColumnToLabelName(colName)

		for i, value := range *values {
			if value.IsNull() {
				continue
			}
			builders[i].Set(labelName, yoloString(value.ByteArray()))
		}
	}
	return builders, nil
}

// v1 blocks have a cf_meta_index column that contains an index which columns to resolve for a metric
func materializeLabelsV1(ctx context.Context, m SelectReadMeta, rgi int, rr []rowRange) ([]labels.Builder, error) {
	rowCount := totalRows(rr)

	lc, ok := m.LabelPfile.Schema().Lookup(schema.LabelIndexColumn)
	if !ok {
		return nil, fmt.Errorf("unable to to find label index column %q", schema.LabelIndexColumn)
	}

	rg := m.LabelPfile.RowGroups()[rgi]

	cc := rg.ColumnChunks()[lc.ColumnIndex]
	colsIdxs, err := materializeLabelColumn(ctx, rg, cc, rr)
	if err != nil {
		return nil, fmt.Errorf("unable to materialize label index column: %w", err)
	}

	seen := make(map[string]struct{})
	colsMap := make(map[int]*[]parquet.Value, 10)
	for _, colsIdx := range colsIdxs {
		key := yoloString(colsIdx.ByteArray())
		if _, ok := seen[key]; !ok {
			idxs, err := encoding.DecodeLabelColumnIndex(colsIdx.ByteArray())
			if err != nil {
				return nil, fmt.Errorf("unable to decode column index: %w", err)
			}
			for _, idx := range idxs {
				colsMap[idx] = &[]parquet.Value{}
			}
			seen[key] = struct{}{}
		}
	}

	g, ctx := errgroup.WithContext(ctx)
	for cIdx, v := range colsMap {
		g.Go(func() error {
			cc := rg.ColumnChunks()[cIdx]
			values, err := materializeLabelColumn(ctx, rg, cc, rr)
			if err != nil {
				return fmt.Errorf("unable to materialize labels values: %w", err)
			}
			*v = values
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return nil, err
	}

	builders := make([]labels.Builder, rowCount)
	for cIdx, values := range colsMap {
		colName := m.LabelPfile.Schema().Columns()[cIdx][0]
		labelName := schema.ColumnToLabelName(colName)

		for i, value := range *values {
			if value.IsNull() {
				continue
			}
			builders[i].Set(labelName, yoloString(value.ByteArray()))
		}
	}
	return builders, nil
}

func materializeChunks(
	ctx context.Context,
	m SelectReadMeta,
	rgi int,
	mint int64,
	maxt int64,
	rr []rowRange) ([][]chunks.Meta, error) {
	rowCount := totalRows(rr)

	minChunkCol, ok := schema.ChunkColumnIndex(m.Meta, time.UnixMilli(mint))
	if !ok {
		return nil, errors.New("unable to find min chunk column")
	}
	maxChunkCol, ok := schema.ChunkColumnIndex(m.Meta, time.UnixMilli(maxt))
	if !ok {
		return nil, errors.New("unable to find max chunk column")
	}
	rg := m.ChunkPfile.RowGroups()[rgi]

	r := make([][]chunks.Meta, rowCount)

	var mu sync.Mutex
	g, ctx := errgroup.WithContext(ctx)
	for i := minChunkCol; i <= maxChunkCol; i++ {
		colName, ok := schema.ChunkColumnName(i)
		if !ok {
			return nil, fmt.Errorf("unable to find chunk column for column index %d", i)
		}
		col, ok := rg.Schema().Lookup(colName)
		if !ok {
			return nil, fmt.Errorf("unable to find chunk column for column name %q", colName)
		}

		cc := rg.ColumnChunks()[col.ColumnIndex]
		g.Go(func() error {
			values, err := materializeChunkColumn(
				ctx,
				rg,
				cc,
				rr,
				withByteQuota(m.ChunkBytesQuota),
				withReaderFromContext(m.ChunkFileReaderFromContext),
				withPartitionMaxRangeSize(m.ChunkPagePartitionMaxRange),
				withPartitionMaxGapSize(m.ChunkPagePartitionMaxGap),
				withPartitionMaxConcurrency(m.ChunkPagePartitionMaxConcurrency),
			)
			if err != nil {
				return fmt.Errorf("unable to materialize column: %w", err)
			}

			for j, chkVal := range values {
				chks := make([]chunks.Meta, 0, 12)
				bs := chkVal.ByteArray()
				for len(bs) != 0 {
					enc := chunkenc.Encoding(binary.BigEndian.Uint32(bs[:4]))
					bs = bs[4:]
					cmint := encoding.ZigZagDecode(binary.BigEndian.Uint64(bs[:8]))
					bs = bs[8:]
					cmaxt := encoding.ZigZagDecode(binary.BigEndian.Uint64(bs[:8]))
					bs = bs[8:]
					l := binary.BigEndian.Uint32(bs[:4])
					bs = bs[4:]
					chk, err := chunkenc.FromData(enc, bs[:l])
					if err != nil {
						return fmt.Errorf("unable to create chunk from data: %w", err)
					}
					chks = append(chks, chunks.Meta{MinTime: cmint, MaxTime: cmaxt, Chunk: chk})
					bs = bs[l:]
				}

				mu.Lock()
				r[j] = append(r[j], chks...)
				mu.Unlock()
			}
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return nil, fmt.Errorf("unable to process chunks: %w", err)
	}

	for i := range r {
		slices.SortFunc(r[i], func(a, b chunks.Meta) int { return int(a.MinTime - b.MinTime) })
	}

	return r, nil
}

func materializeLabelNames(ctx context.Context, meta LabelNamesReadMeta, rgi int, rr []rowRange) ([]string, annotations.Annotations, error) {
	switch v := meta.Meta.Version; v {
	case schema.V0:
		return materializeLabelNamesV0(ctx, meta, rgi, rr)
	case schema.V1, schema.V2:
		return materializeLabelNamesV1(ctx, meta, rgi, rr)
	default:
		return nil, nil, fmt.Errorf("unable to materialize labels names for block of version %q", v)
	}
}

func materializeLabelNamesV0(ctx context.Context, meta LabelNamesReadMeta, rgi int, rr []rowRange) ([]string, annotations.Annotations, error) {
	var annos annotations.Annotations

	metricNameColumn := schema.LabelNameToColumn(labels.MetricName)

	lc, ok := meta.LabelPfile.Schema().Lookup(metricNameColumn)
	if !ok {
		return nil, annos, fmt.Errorf("unable to to find column %q", metricNameColumn)
	}

	rg := meta.LabelPfile.RowGroups()[rgi]

	cc := rg.ColumnChunks()[lc.ColumnIndex]
	metricNames, err := materializeLabelColumn(ctx, rg, cc, rr)
	if err != nil {
		return nil, annos, fmt.Errorf("unable to materialize metric name column: %w", err)
	}

	seen := make(map[string]struct{})
	colIdxs := make(map[int]struct{})
	for _, mn := range metricNames {
		key := yoloString(mn.ByteArray())
		if _, ok := seen[key]; !ok {
			cols := meta.Meta.ColumnsForName[key]
			for _, c := range cols {
				lc, ok := meta.LabelPfile.Schema().Lookup(c)
				if !ok {
					continue
				}
				colIdxs[lc.ColumnIndex] = struct{}{}
			}
		}
		seen[key] = struct{}{}
	}

	cols := meta.LabelPfile.Schema().Columns()

	res := make([]string, 0, len(colIdxs))
	for k := range colIdxs {
		res = append(res, schema.ColumnToLabelName(cols[k][0]))
	}
	return res, annos, nil

}

func materializeLabelNamesV1(ctx context.Context, meta LabelNamesReadMeta, rgi int, rr []rowRange) ([]string, annotations.Annotations, error) {
	var annos annotations.Annotations

	lc, ok := meta.LabelPfile.Schema().Lookup(schema.LabelIndexColumn)
	if !ok {
		return nil, annos, fmt.Errorf("unable to to find label index column %q", schema.LabelIndexColumn)
	}

	rg := meta.LabelPfile.RowGroups()[rgi]

	cc := rg.ColumnChunks()[lc.ColumnIndex]
	colsIdxs, err := materializeLabelColumn(ctx, rg, cc, rr)
	if err != nil {
		return nil, annos, fmt.Errorf("unable to materialize label index column: %w", err)
	}

	seen := make(map[string]struct{})
	colIdxs := make(map[int]struct{})
	for _, colsIdx := range colsIdxs {
		key := yoloString(colsIdx.ByteArray())
		if _, ok := seen[key]; !ok {
			idxs, err := encoding.DecodeLabelColumnIndex(colsIdx.ByteArray())
			if err != nil {
				return nil, annos, fmt.Errorf("materializer failed to decode column index: %w", err)
			}
			for _, idx := range idxs {
				colIdxs[idx] = struct{}{}
			}
			seen[key] = struct{}{}
		}
	}

	cols := meta.LabelPfile.Schema().Columns()

	res := make([]string, 0, len(colIdxs))
	for k := range colIdxs {
		res = append(res, schema.ColumnToLabelName(cols[k][0]))
	}
	return res, annos, nil
}

func materializeLabelValues(ctx context.Context, meta LabelValuesReadMeta, name string, rgi int, rr []rowRange) ([]string, annotations.Annotations, error) {
	switch v := meta.Meta.Version; v {
	case schema.V0, schema.V1, schema.V2:
		return materializeLabelValuesV0V1(ctx, meta, name, rgi, rr)
	default:
		return nil, nil, fmt.Errorf("unable to materialize labels values for block of version %q", v)
	}
}

func materializeLabelValuesV0V1(ctx context.Context, meta LabelValuesReadMeta, name string, rgi int, rr []rowRange) ([]string, annotations.Annotations, error) {
	var annos annotations.Annotations

	lc, ok := meta.LabelPfile.Schema().Lookup(schema.LabelNameToColumn(name))
	if !ok {
		return nil, annos, nil
	}

	rg := meta.LabelPfile.RowGroups()[rgi]

	cc := rg.ColumnChunks()[lc.ColumnIndex]
	vals, err := materializeLabelColumn(ctx, rg, cc, rr)
	if err != nil {
		return nil, nil, fmt.Errorf("unable to materialize label %q column: %w", name, err)
	}

	seen := make(map[string]struct{})
	res := make([]string, 0)
	for _, v := range vals {
		if v.IsNull() {
			continue
		}
		key := yoloString(v.ByteArray())
		if _, ok := seen[key]; !ok {
			res = append(res, v.Clone().String())
			seen[key] = struct{}{}
		}
	}
	return res, annos, nil
}

type chunkMaterializeConfig struct {
	bytesQuota *limits.Quota

	readerFromContext readerFromContext

	partitionMaxRangeSize   uint64
	partitionMaxGapSize     uint64
	partitionMaxConcurrency int
}

type chunkMaterializeOption func(*chunkMaterializeConfig)

func withByteQuota(q *limits.Quota) chunkMaterializeOption {
	return func(cfg *chunkMaterializeConfig) {
		cfg.bytesQuota = q
	}
}

func withPartitionMaxRangeSize(maxRangeSize uint64) chunkMaterializeOption {
	return func(cfg *chunkMaterializeConfig) {
		cfg.partitionMaxRangeSize = maxRangeSize
	}
}

func withPartitionMaxGapSize(maxGapSize uint64) chunkMaterializeOption {
	return func(cfg *chunkMaterializeConfig) {
		cfg.partitionMaxGapSize = maxGapSize
	}
}

func withPartitionMaxConcurrency(maxConcurrency int) chunkMaterializeOption {
	return func(cfg *chunkMaterializeConfig) {
		cfg.partitionMaxConcurrency = maxConcurrency
	}
}

type readerFromContext func(ctx context.Context) io.ReaderAt

func withReaderFromContext(rx readerFromContext) chunkMaterializeOption {
	return func(cfg *chunkMaterializeConfig) {
		cfg.readerFromContext = rx
	}
}

func materializeChunkColumn(ctx context.Context, rg parquet.RowGroup, cc parquet.ColumnChunk, rr []rowRange, opts ...chunkMaterializeOption) ([]parquet.Value, error) {
	if len(rr) == 0 {
		return nil, nil
	}
	rowCount := totalRows(rr)
	column := rg.Schema().Columns()[cc.Column()][0]

	cfg := chunkMaterializeConfig{
		bytesQuota:            limits.UnlimitedQuota(),
		partitionMaxRangeSize: math.MaxUint64,
		partitionMaxGapSize:   math.MaxUint64,
	}
	for i := range opts {
		opts[i](&cfg)
	}
	ctx, span := tracing.Tracer().Start(ctx, "Materialize Column")
	defer span.End()

	span.SetAttributes(attribute.String("column", rg.Schema().Columns()[cc.Column()][0]))

	oidx, err := cc.OffsetIndex()
	if err != nil {
		return nil, fmt.Errorf("could not get offset index: %w", err)
	}

	cidx, err := cc.ColumnIndex()
	if err != nil {
		return nil, fmt.Errorf("could not get column index: %w", err)
	}

	pagesToRowsMap := make(map[int][]rowRange, len(rr))
	for i := range cidx.NumPages() {
		pr := rowRange{
			from: oidx.FirstRowIndex(i),
		}
		pr.count = rg.NumRows()

		if i < oidx.NumPages()-1 {
			pr.count = oidx.FirstRowIndex(i+1) - pr.from
		}

		for _, r := range rr {
			if intersect(pr, r) {
				pagesToRowsMap[i] = append(pagesToRowsMap[i], intersection(r, pr))
			}
		}
	}

	if err := checkByteQuota(cfg.bytesQuota, maps.Keys(pagesToRowsMap), oidx); err != nil {
		return nil, err
	}

	pageRanges := partitionPageRanges(
		cfg.partitionMaxRangeSize,
		cfg.partitionMaxGapSize,
		pagesToRowsMap,
		oidx,
	)

	r := make(map[rowRange][]parquet.Value, len(pageRanges))
	for _, v := range pageRanges {
		for _, rs := range v.rows {
			r[rs] = make([]parquet.Value, 0, rs.count)
		}
	}

	var mu sync.Mutex
	g, ctx := errgroup.WithContext(ctx)
	if limit := cfg.partitionMaxConcurrency; limit != 0 {
		g.SetLimit(limit)
	}

	method := methodFromContext(ctx)
	columnMaterialized.WithLabelValues(column, method).Add(1)
	rowsMaterialized.WithLabelValues(column, method).Add(float64(rowCount))

	for _, p := range pageRanges {
		g.Go(func() error {
			ctx, span := tracing.Tracer().Start(ctx, "Materialize Page Range")
			defer span.End()

			span.SetAttributes(attribute.IntSlice("pages", p.pages))

			rdrAt := cfg.readerFromContext(ctx)

			// TODO: read pages in one big read here - then use "PagesFrom" with a bytes.NewReader
			// that reads that byte slice - this prevents AsyncPages from overfetching and coalesces
			// small reads into one big read

			minOffset := oidx.Offset(p.pages[0])
			maxOffset := oidx.Offset(p.pages[len(p.pages)-1]) + oidx.CompressedPageSize(p.pages[len(p.pages)-1])

			bufRdrAt := newBufferedReaderAt(rdrAt, minOffset, maxOffset)

			pagesRead.WithLabelValues(column, method).Add(float64(len(p.pages)))
			pagesReadSize.WithLabelValues(column, method).Add(float64(maxOffset - minOffset))

			pgs := cc.(*parquet.FileColumnChunk).PagesFrom(bufRdrAt)
			defer func() { _ = pgs.Close() }()

			if err := pgs.SeekToRow(p.rows[0].from); err != nil {
				return fmt.Errorf("could not seek to row: %w", err)
			}

			vi := &chunkValuesIterator{}
			remainingRr := p.rows
			currentRr := remainingRr[0]
			next := currentRr.from
			remaining := currentRr.count
			currentRow := currentRr.from

			remainingRr = remainingRr[1:]
			for len(remainingRr) > 0 || remaining > 0 {
				page, err := pgs.ReadPage()
				if err != nil {
					return fmt.Errorf("unable to read page: %w", err)
				}

				vi.Reset(page)
				for vi.Next() {
					if currentRow == next {
						mu.Lock()
						r[currentRr] = append(r[currentRr], vi.At())
						mu.Unlock()
						remaining--
						if remaining > 0 {
							next = next + 1
						} else if len(remainingRr) > 0 {
							currentRr = remainingRr[0]
							next = currentRr.from
							remaining = currentRr.count
							remainingRr = remainingRr[1:]
						}
					}
					currentRow++
				}
				parquet.Release(page)

				if err := vi.Error(); err != nil {
					return fmt.Errorf("error during page iteration: %w", err)
				}
			}
			return nil
		})
	}
	if err = g.Wait(); err != nil {
		return nil, fmt.Errorf("unable to materialize columns: %w", err)
	}

	ranges := slices.Collect(maps.Keys(r))
	slices.SortFunc(ranges, func(a, b rowRange) int {
		return int(a.from - b.from)
	})

	res := make([]parquet.Value, 0, totalRows(rr))
	for _, v := range ranges {
		res = append(res, r[v]...)
	}
	return res, nil
}

func materializeLabelColumn(ctx context.Context, rg parquet.RowGroup, cc parquet.ColumnChunk, rr []rowRange) ([]parquet.Value, error) {
	if len(rr) == 0 {
		return nil, nil
	}
	rowCount := totalRows(rr)
	column := rg.Schema().Columns()[cc.Column()][0]

	oidx, err := cc.OffsetIndex()
	if err != nil {
		return nil, fmt.Errorf("could not get offset index: %w", err)
	}

	cidx, err := cc.ColumnIndex()
	if err != nil {
		return nil, fmt.Errorf("could not get column index: %w", err)
	}

	pagesToRowsMap := make(map[int][]rowRange, len(rr))
	for i := range cidx.NumPages() {
		pr := rowRange{
			from: oidx.FirstRowIndex(i),
		}
		pr.count = rg.NumRows()

		if i < oidx.NumPages()-1 {
			pr.count = oidx.FirstRowIndex(i+1) - pr.from
		}

		for _, r := range rr {
			if intersect(pr, r) {
				pagesToRowsMap[i] = append(pagesToRowsMap[i], intersection(r, pr))
			}
		}
	}

	pageRanges := partitionPageRanges(
		math.MaxUint64,
		math.MaxUint64,
		pagesToRowsMap,
		oidx,
	)

	r := make(map[rowRange][]parquet.Value, len(pageRanges))
	for _, v := range pageRanges {
		for _, rs := range v.rows {
			r[rs] = make([]parquet.Value, 0, rs.count)
		}
	}

	var mu sync.Mutex
	g, ctx := errgroup.WithContext(ctx)

	method := methodFromContext(ctx)
	columnMaterialized.WithLabelValues(column, method).Add(1)
	rowsMaterialized.WithLabelValues(column, method).Add(float64(rowCount))

	for _, p := range pageRanges {
		g.Go(func() error {
			minOffset := oidx.Offset(p.pages[0])
			maxOffset := oidx.Offset(p.pages[len(p.pages)-1]) + oidx.CompressedPageSize(p.pages[len(p.pages)-1])

			pagesRead.WithLabelValues(column, method).Add(float64(len(p.pages)))
			pagesReadSize.WithLabelValues(column, method).Add(float64(maxOffset - minOffset))

			pgs := cc.Pages()
			defer func() { _ = pgs.Close() }()

			if err := pgs.SeekToRow(p.rows[0].from); err != nil {
				return fmt.Errorf("could not seek to row: %w", err)
			}

			vi := &chunkValuesIterator{}
			remainingRr := p.rows
			currentRr := remainingRr[0]
			next := currentRr.from
			remaining := currentRr.count
			currentRow := currentRr.from

			remainingRr = remainingRr[1:]
			for len(remainingRr) > 0 || remaining > 0 {
				page, err := pgs.ReadPage()
				if err != nil {
					return fmt.Errorf("unable to read page: %w", err)
				}

				vi.Reset(page)
				for vi.Next() {
					if currentRow == next {
						mu.Lock()
						r[currentRr] = append(r[currentRr], vi.At())
						mu.Unlock()
						remaining--
						if remaining > 0 {
							next = next + 1
						} else if len(remainingRr) > 0 {
							currentRr = remainingRr[0]
							next = currentRr.from
							remaining = currentRr.count
							remainingRr = remainingRr[1:]
						}
					}
					currentRow++
				}
				parquet.Release(page)

				if err := vi.Error(); err != nil {
					return fmt.Errorf("error during page iteration: %w", err)
				}
			}
			return nil
		})
	}
	if err = g.Wait(); err != nil {
		return nil, fmt.Errorf("unable to materialize columns: %w", err)
	}

	ranges := slices.Collect(maps.Keys(r))
	slices.SortFunc(ranges, func(a, b rowRange) int {
		return int(a.from - b.from)
	})

	res := make([]parquet.Value, 0, totalRows(rr))
	for _, v := range ranges {
		res = append(res, r[v]...)
	}
	return res, nil
}

func totalRows(rr []rowRange) int64 {
	res := int64(0)
	for _, r := range rr {
		res += r.count
	}
	return res
}

func totalBytes(pages iter.Seq[int], oidx parquet.OffsetIndex) int64 {
	res := int64(0)
	for i := range pages {
		res += oidx.CompressedPageSize(i)
	}
	return res
}

func checkRowQuota(rowQuota *limits.Quota, rr []rowRange) error {
	if err := rowQuota.Reserve(totalRows(rr)); err != nil {
		return fmt.Errorf("would use too many rows: %w", err)
	}
	return nil
}

func checkByteQuota(byteQuota *limits.Quota, pages iter.Seq[int], oidx parquet.OffsetIndex) error {
	if err := byteQuota.Reserve(totalBytes(pages, oidx)); err != nil {
		return fmt.Errorf("would use too many bytes: %w", err)
	}
	return nil
}

type pageEntryRead struct {
	pages []int
	rows  []rowRange
}

func partitionPageRanges(
	maxRangeSize uint64,
	maxGapSize uint64,
	pageIdx map[int][]rowRange,
	offset parquet.OffsetIndex,
) []pageEntryRead {
	partitioner := newGapBasedPartitioner(maxRangeSize, maxGapSize)
	if len(pageIdx) == 0 {
		return []pageEntryRead{}
	}
	idxs := make([]int, 0, len(pageIdx))
	for idx := range pageIdx {
		idxs = append(idxs, idx)
	}

	slices.Sort(idxs)

	parts := partitioner.partition(len(idxs), func(i int) (uint64, uint64) {
		return uint64(offset.Offset(idxs[i])), uint64(offset.Offset(idxs[i]) + offset.CompressedPageSize(idxs[i]))
	})

	r := make([]pageEntryRead, 0, len(parts))
	for _, part := range parts {
		pagesToRead := pageEntryRead{}
		for i := part.elemRng[0]; i < part.elemRng[1]; i++ {
			pagesToRead.pages = append(pagesToRead.pages, idxs[i])
			pagesToRead.rows = append(pagesToRead.rows, pageIdx[idxs[i]]...)
		}
		pagesToRead.rows = simplify(pagesToRead.rows)
		r = append(r, pagesToRead)
	}
	return r
}

type labelValuesIterator struct {
	p parquet.Page

	cachedSymbols map[int32]parquet.Value
	st            symbolTable

	vr parquet.ValueReader

	current            int
	buffer             []parquet.Value
	currentBufferIndex int
	err                error
}

func (vi *labelValuesIterator) Reset(p parquet.Page) {
	vi.p = p
	vi.vr = p.Values()
	vi.st.Reset(p)
	vi.cachedSymbols = make(map[int32]parquet.Value, p.Dictionary().Len())
	vi.current = -1
}

func (vi *labelValuesIterator) Next() bool {
	if vi.err != nil {
		return false
	}

	vi.current++
	if vi.current >= int(vi.p.NumRows()) {
		return false
	}

	vi.currentBufferIndex++

	if vi.currentBufferIndex == len(vi.buffer) {
		n, err := vi.vr.ReadValues(vi.buffer[:cap(vi.buffer)])
		if err != nil && err != io.EOF {
			vi.err = err
			return false
		}
		vi.buffer = vi.buffer[:n]
		vi.currentBufferIndex = 0
	}
	return true
}

func (vi *labelValuesIterator) Error() error {
	return vi.err
}

func (vi *labelValuesIterator) At() parquet.Value {
	sym := vi.st.GetIndex(vi.current)
	// Cache a clone of the current symbol table entry.
	// This allows us to release the original page while avoiding unnecessary future clones.
	if _, ok := vi.cachedSymbols[sym]; !ok {
		vi.cachedSymbols[sym] = vi.st.Get(vi.current).Clone()
	}
	return vi.cachedSymbols[sym]
}

type chunkValuesIterator struct {
	p parquet.Page

	vr parquet.ValueReader

	current            int
	buffer             []parquet.Value
	currentBufferIndex int
	err                error
}

func (vi *chunkValuesIterator) Reset(p parquet.Page) {
	vi.p = p
	vi.vr = p.Values()
	vi.buffer = make([]parquet.Value, 0, 128)
	vi.currentBufferIndex = -1
	vi.current = -1
}

func (vi *chunkValuesIterator) Next() bool {
	if vi.err != nil {
		return false
	}

	vi.current++
	if vi.current >= int(vi.p.NumRows()) {
		return false
	}

	vi.currentBufferIndex++

	if vi.currentBufferIndex == len(vi.buffer) {
		n, err := vi.vr.ReadValues(vi.buffer[:cap(vi.buffer)])
		if err != nil && err != io.EOF {
			vi.err = err
			return false
		}
		vi.buffer = vi.buffer[:n]
		vi.currentBufferIndex = 0
	}
	return true
}

func (vi *chunkValuesIterator) Error() error {
	return vi.err
}

func (vi *chunkValuesIterator) At() parquet.Value {
	return vi.buffer[vi.currentBufferIndex].Clone()
}

type bufferedReaderAt struct {
	r      io.ReaderAt
	b      []byte
	offset int64
}

func (b bufferedReaderAt) ReadAt(p []byte, off int64) (n int, err error) {
	if off >= b.offset && off < b.offset+int64(len(b.b)) {
		diff := off - b.offset
		n := copy(p, b.b[diff:])
		return n, nil
	}
	return b.r.ReadAt(p, off)
}

func newBufferedReaderAt(r io.ReaderAt, minOffset, maxOffset int64) io.ReaderAt {
	if minOffset < maxOffset {
		b := make([]byte, maxOffset-minOffset)
		n, err := r.ReadAt(b, minOffset)
		if err == nil {
			return &bufferedReaderAt{r: r, b: b[:n], offset: minOffset}
		}
	}
	return r
}
