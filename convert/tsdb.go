// Copyright (c) 2025 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

package convert

import (
	"context"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"

	"github.com/parquet-go/parquet-go"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/index"

	"github.com/cloudflare/parquet-tsdb-poc/internal/encoding"
	"github.com/cloudflare/parquet-tsdb-poc/schema"
)

type indexRowReader struct {
	ctx context.Context

	closers []io.Closer

	seriesSet storage.ChunkSeriesSet

	schema     *parquet.Schema
	rowBuilder *parquet.RowBuilder

	concurrency int

	chunksColumn0    int
	chunksColumn1    int
	chunksColumn2    int
	labelIndexColumn int
	labelHashColumn  int
}

type indexReaderOpts struct {
	sortLabels  []string
	concurrency int
}

var _ parquet.RowReader = &indexRowReader{}

func newIndexRowReader(ctx context.Context, mint, maxt int64, blks []Convertable, opts indexReaderOpts) (*indexRowReader, error) {
	var (
		lbls       = make([]string, 0)
		seriesSets = make([]storage.ChunkSeriesSet, 0, len(blks))
		closers    = make([]io.Closer, 0, len(blks))
	)

	compareFunc := func(a, b labels.Labels) int {
		for _, lb := range opts.sortLabels {
			if c := strings.Compare(a.Get(lb), b.Get(lb)); c != 0 {
				return c
			}
		}
		return labels.Compare(a, b)
	}

	for _, blk := range blks {
		indexr, err := blk.Index()
		if err != nil {
			return nil, fmt.Errorf("unable to get index reader from block: %s", err)
		}
		closers = append(closers, indexr)

		chunkr, err := blk.Chunks()
		if err != nil {
			return nil, fmt.Errorf("unable to get chunk reader from block: %s", err)
		}
		closers = append(closers, chunkr)

		tombsr, err := blk.Tombstones()
		if err != nil {
			return nil, fmt.Errorf("unable to get tombstone reader from block: %s", err)
		}
		closers = append(closers, tombsr)

		lblns, err := indexr.LabelNames(ctx)
		if err != nil {
			return nil, fmt.Errorf("unable to get label names from block: %s", err)
		}
		lbls = append(lbls, lblns...)

		postings := sortedPostings(ctx, indexr, compareFunc)
		seriesSet := tsdb.NewBlockChunkSeriesSet(blk.Meta().ULID, indexr, chunkr, tombsr, postings, mint, maxt, false)
		seriesSets = append(seriesSets, seriesSet)
	}
	slices.Sort(lbls)

	cseriesSet := newMergeChunkSeriesSet(seriesSets, compareFunc, storage.NewConcatenatingChunkSeriesMerger())
	s := schema.BuildSchemaFromLabels(slices.Compact(lbls))

	return &indexRowReader{
		ctx:       ctx,
		seriesSet: cseriesSet,
		closers:   closers,

		schema:     s,
		rowBuilder: parquet.NewRowBuilder(s),

		concurrency: opts.concurrency,

		chunksColumn0:    columnIDForKnownColumn(s, schema.ChunksColumn0),
		chunksColumn1:    columnIDForKnownColumn(s, schema.ChunksColumn1),
		chunksColumn2:    columnIDForKnownColumn(s, schema.ChunksColumn2),
		labelIndexColumn: columnIDForKnownColumn(s, schema.LabelIndexColumn),
		labelHashColumn:  columnIDForKnownColumn(s, schema.LabelHashColumn),
	}, nil
}

func columnIDForKnownColumn(schema *parquet.Schema, columnName string) int {
	lc, _ := schema.Lookup(columnName)
	return lc.ColumnIndex
}

func (rr *indexRowReader) Close() error {
	errs := make([]error, 0)
	for i := range rr.closers {
		if err := rr.closers[i].Close(); err != nil {
			errs = append(errs, fmt.Errorf("unable to close %q-th closer: %w", i, err))
		}
	}
	return errors.Join(errs...)
}

func (rr *indexRowReader) Schema() *parquet.Schema {
	return rr.schema
}

func (rr *indexRowReader) ReadRows(buf []parquet.Row) (int, error) {
	select {
	case <-rr.ctx.Done():
		return 0, rr.ctx.Err()
	default:
	}

	type chkBytesOrError struct {
		chkBytes [schema.ChunkColumnsPerDay][]byte
		err      error
	}
	type chunkSeriesPromise struct {
		s storage.ChunkSeries
		c chan chkBytesOrError
	}

	chunkSeriesC := make(chan chunkSeriesPromise, rr.concurrency)

	go func() {
		defer close(chunkSeriesC)
		for j := 0; j < len(buf) && rr.seriesSet.Next(); j++ {
			s := rr.seriesSet.At()
			it := s.Iterator(nil)

			promise := chunkSeriesPromise{
				s: s,
				c: make(chan chkBytesOrError, 1),
			}

			chunkSeriesC <- promise

			go func() {
				chkBytes, err := collectChunks(it)
				promise.c <- chkBytesOrError{chkBytes: chkBytes, err: err}
				close(promise.c)
			}()
		}
	}()

	colIdxSlice := make([]int, 0)
	i, j := 0, 0
	for promise := range chunkSeriesC {
		j++

		rr.rowBuilder.Reset()
		colIdxSlice = colIdxSlice[:0]

		chkBytesOrError := <-promise.c
		if err := chkBytesOrError.err; err != nil {
			return i, err
		}
		chkBytes := chkBytesOrError.chkBytes
		chkLbls := promise.s.Labels()

		chkLbls.Range(func(l labels.Label) {
			colName := schema.LabelNameToColumn(l.Name)
			lc, _ := rr.schema.Lookup(colName)
			rr.rowBuilder.Add(lc.ColumnIndex, parquet.ValueOf(l.Value))
			// we need to address for projecting chunk columns away later so we need to correct for the offset here
			colIdxSlice = append(colIdxSlice, lc.ColumnIndex-schema.ChunkColumnsPerDay)
		})
		rr.rowBuilder.Add(rr.labelIndexColumn, parquet.ValueOf(encoding.EncodeLabelColumnIndex(colIdxSlice)))
		rr.rowBuilder.Add(rr.labelHashColumn, parquet.ValueOf(chkLbls.Hash()))

		if allChunksEmpty(chkBytes) {
			continue
		}
		for idx, chk := range chkBytes {
			if len(chk) == 0 {
				continue
			}
			switch idx {
			case 0:
				rr.rowBuilder.Add(rr.chunksColumn0, parquet.ValueOf(chk))
			case 1:
				rr.rowBuilder.Add(rr.chunksColumn1, parquet.ValueOf(chk))
			case 2:
				rr.rowBuilder.Add(rr.chunksColumn2, parquet.ValueOf(chk))
			}
		}

		buf[i] = rr.rowBuilder.AppendRow(buf[i][:0])
		i++
	}
	if j < len(buf) {
		return i, io.EOF
	}
	return i, rr.seriesSet.Err()
}

func sortedPostings(ctx context.Context, indexr tsdb.IndexReader, compare func(a, b labels.Labels) int) index.Postings {
	p := tsdb.AllSortedPostings(ctx, indexr)

	type s struct {
		ref    storage.SeriesRef
		labels labels.Labels
	}
	series := make([]s, 0, 128)

	lb := labels.NewScratchBuilder(10)
	for p.Next() {
		select {
		case <-ctx.Done():
			return index.ErrPostings(ctx.Err())
		default:
		}
		lb.Reset()
		if err := indexr.Series(p.At(), &lb, nil); err != nil {
			return index.ErrPostings(fmt.Errorf("unable to expand series: %w", err))
		}
		series = append(series, s{labels: lb.Labels(), ref: p.At()})
	}
	if err := p.Err(); err != nil {
		return index.ErrPostings(fmt.Errorf("unable to expand postings: %w", err))
	}

	slices.SortFunc(series, func(a, b s) int {
		return compare(a.labels, b.labels)
	})

	// Convert back to list.
	ep := make([]storage.SeriesRef, 0, len(series))
	for _, p := range series {
		ep = append(ep, p.ref)
	}
	return index.NewListPostings(ep)
}

func allChunksEmpty(chkBytes [schema.ChunkColumnsPerDay][]byte) bool {
	for _, chk := range chkBytes {
		if len(chk) != 0 {
			return false
		}
	}
	return true
}
