// Copyright (c) 2025 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

package convert

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/alecthomas/units"
	"github.com/parquet-go/parquet-go"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/tombstones"
	"github.com/prometheus/prometheus/util/zeropool"
	"github.com/thanos-io/objstore"
	"google.golang.org/protobuf/proto"

	"github.com/cloudflare/parquet-tsdb-poc/internal/encoding"
	"github.com/cloudflare/parquet-tsdb-poc/internal/util"
	"github.com/cloudflare/parquet-tsdb-poc/proto/metapb"
	"github.com/cloudflare/parquet-tsdb-poc/schema"
)

type Convertable interface {
	Index() (tsdb.IndexReader, error)
	Chunks() (tsdb.ChunkReader, error)
	Tombstones() (tombstones.Reader, error)
	Meta() tsdb.BlockMeta
}

type convertOpts struct {
	numRowGroups        int
	rowGroupSize        int
	encodingConcurrency int

	sortLabels         []string
	sortingColumns     [][]string
	bloomfilterColumns [][]string
	labelBufferPool    parquet.BufferPool
	chunkbufferPool    parquet.BufferPool

	labelPageBufferSize int
	chunkPageBufferSize int
}

func (cfg convertOpts) buildBloomfilterColumns() []parquet.BloomFilterColumn {
	cols := make([]parquet.BloomFilterColumn, 0, len(cfg.bloomfilterColumns))

	for i := range cfg.bloomfilterColumns {
		cols = append(cols,
			parquet.SplitBlockFilter(10, cfg.bloomfilterColumns[i]...))
	}
	return cols
}

func (cfg convertOpts) buildSortingColumns() []parquet.SortingColumn {
	cols := make([]parquet.SortingColumn, 0, len(cfg.bloomfilterColumns))

	for i := range cfg.sortingColumns {
		cols = append(cols,
			parquet.Ascending(cfg.sortingColumns[i]...))
	}
	return cols
}

type ConvertOption func(*convertOpts)

func RowGroupSize(rbs int) ConvertOption {
	return func(opts *convertOpts) {
		opts.rowGroupSize = rbs
	}
}

func RowGroupCount(rc int) ConvertOption {
	return func(opts *convertOpts) {
		opts.numRowGroups = rc
	}
}

func SortBy(labels ...string) ConvertOption {
	return func(opts *convertOpts) {
		sortingColumns := make([][]string, len(labels))
		for i := range labels {
			sortingColumns[i] = []string{schema.LabelNameToColumn(labels[i])}
		}
		opts.sortingColumns = sortingColumns
		opts.sortLabels = labels
	}
}

func LabelPageBufferSize(pb units.Base2Bytes) ConvertOption {
	return func(opts *convertOpts) {
		opts.labelPageBufferSize = int(pb)
	}
}

func ChunkPageBufferSize(pb units.Base2Bytes) ConvertOption {
	return func(opts *convertOpts) {
		opts.chunkPageBufferSize = int(pb)
	}
}

func LabelBufferPool(p parquet.BufferPool) ConvertOption {
	return func(opts *convertOpts) {
		opts.labelBufferPool = p
	}
}

func ChunkBufferPool(p parquet.BufferPool) ConvertOption {
	return func(opts *convertOpts) {
		opts.chunkbufferPool = p
	}
}

func EncodingConcurrency(c int) ConvertOption {
	return func(opts *convertOpts) {
		opts.encodingConcurrency = c
	}
}

func ConvertTSDBBlock(
	ctx context.Context,
	bkt objstore.Bucket,
	day time.Time,
	blks []Convertable,
	opts ...ConvertOption,
) error {
	cfg := &convertOpts{
		rowGroupSize:        1_000_000,
		numRowGroups:        6,
		sortingColumns:      [][]string{{schema.LabelNameToColumn(labels.MetricName)}},
		bloomfilterColumns:  [][]string{{schema.LabelNameToColumn(labels.MetricName)}},
		labelBufferPool:     parquet.NewBufferPool(),
		chunkbufferPool:     parquet.NewBufferPool(),
		encodingConcurrency: 1,
		labelPageBufferSize: int(256 * units.KiB),
		chunkPageBufferSize: int(2 * units.MiB),
	}
	for i := range opts {
		opts[i](cfg)
	}
	start, end := util.BeginOfDay(day), util.EndOfDay(day)
	name, err := schema.BlockNameForDay(start)
	if err != nil {
		return fmt.Errorf("unable to get block name: %w", err)
	}
	rr, err := newIndexRowReader(ctx, start.UnixMilli(), end.UnixMilli(), blks, indexReaderOpts{
		sortLabels:  cfg.sortLabels,
		concurrency: cfg.encodingConcurrency,
	})
	if err != nil {
		return fmt.Errorf("unable to create index rowreader: %w", err)
	}
	defer rr.Close()

	converter := newConverter(
		name,
		start.UnixMilli(),
		end.UnixMilli(),
		rr,
		bkt,
		cfg.rowGroupSize,
		cfg.numRowGroups,
		cfg.buildSortingColumns(),
		cfg.buildBloomfilterColumns(),
		cfg.labelBufferPool,
		cfg.chunkbufferPool,
		cfg.labelPageBufferSize,
		cfg.chunkPageBufferSize,
	)

	if err := converter.convert(ctx); err != nil {
		return fmt.Errorf("unable to convert block: %w", err)
	}

	lastSuccessfulConvertTime.SetToCurrentTime()

	return nil
}

type converter struct {
	name       string
	mint, maxt int64

	currentShard   int
	seriesPerShard int
	rowGroupSize   int
	numRowGroups   int

	bkt objstore.Bucket

	rr *indexRowReader

	sortingColumns     []parquet.SortingColumn
	bloomfilterColumns []parquet.BloomFilterColumn
	labelBufferPool    parquet.BufferPool
	chunkBufferPool    parquet.BufferPool

	labelPageBufferSize int
	chunkPageBufferSize int
}

func newConverter(
	name string,
	mint int64,
	maxt int64,
	rr *indexRowReader,
	bkt objstore.Bucket,
	rowGroupSize int,
	numRowGroups int,
	sortingColumns []parquet.SortingColumn,
	bloomfilterColumns []parquet.BloomFilterColumn,
	labelBufferPool parquet.BufferPool,
	chunkBufferPool parquet.BufferPool,
	labelPageBufferSize int,
	chunkPageBufferSize int,

) *converter {
	return &converter{
		name: name,
		mint: mint,
		maxt: maxt,

		bkt: bkt,
		rr:  rr,

		rowGroupSize:       rowGroupSize,
		numRowGroups:       numRowGroups,
		sortingColumns:     sortingColumns,
		bloomfilterColumns: bloomfilterColumns,
		labelBufferPool:    labelBufferPool,
		chunkBufferPool:    chunkBufferPool,

		labelPageBufferSize: labelPageBufferSize,
		chunkPageBufferSize: chunkPageBufferSize,
	}
}

func (c *converter) labelWriterOptions() []parquet.WriterOption {
	return []parquet.WriterOption{
		parquet.MaxRowsPerRowGroup(int64(c.rowGroupSize)),
		parquet.SortingWriterConfig(parquet.SortingColumns(c.sortingColumns...)),
		parquet.BloomFilters(c.bloomfilterColumns...),
		parquet.SkipPageBounds(schema.LabelIndexColumn),
		parquet.ColumnPageBuffers(c.labelBufferPool),
		parquet.PageBufferSize(c.labelPageBufferSize),
	}
}

func (c *converter) chunkWriterOptions() []parquet.WriterOption {
	return []parquet.WriterOption{
		parquet.MaxRowsPerRowGroup(int64(c.rowGroupSize)),
		parquet.SkipPageBounds(schema.LabelHashColumn),
		parquet.SkipPageBounds(schema.ChunksColumn0),
		parquet.SkipPageBounds(schema.ChunksColumn1),
		parquet.SkipPageBounds(schema.ChunksColumn2),
		parquet.ColumnPageBuffers(c.chunkBufferPool),
		parquet.PageBufferSize(c.chunkPageBufferSize),
	}
}

func (c *converter) convert(ctx context.Context) error {
	if err := c.convertShards(ctx); err != nil {
		return fmt.Errorf("unable to convert shards: %w", err)
	}
	if err := c.optimizeShards(ctx); err != nil {
		return fmt.Errorf("unable to optimize shards: %w", err)
	}
	if err := c.writeMetaFile(ctx); err != nil {
		return fmt.Errorf("unable to write meta file: %w", err)
	}
	return nil
}

func (c *converter) writeMetaFile(ctx context.Context) error {
	meta := &metapb.Metadata{
		Version: schema.V2,
		Mint:    c.mint,
		Maxt:    c.maxt,
		Shards:  int64(c.currentShard) + 1,
	}

	metaBytes, err := proto.Marshal(meta)
	if err != nil {
		return fmt.Errorf("unable to marshal meta bytes: %w", err)
	}
	if err := c.bkt.Upload(ctx, schema.MetaFileNameForBlock(c.name), bytes.NewReader(metaBytes)); err != nil {
		return fmt.Errorf("unable to upload meta file: %w", err)
	}

	return nil
}

func (c *converter) convertShards(ctx context.Context) error {
	for {
		if ok, err := c.convertShard(ctx); err != nil {
			return fmt.Errorf("unable to convert shard: %w", err)
		} else if !ok {
			break
		}
	}
	return nil
}

func (c *converter) optimizeShards(ctx context.Context) error {
	g, ctx := errgroup.WithContext(ctx)
	for i := range c.currentShard + 1 {
		g.Go(func() error {
			return c.optimizeShard(ctx, i)
		})
	}
	return g.Wait()
}

// Since we have to compute the schema from the whole TSDB Block it is highly likely that the
// labels parquet file we wrote just now contains many empty columns - we project them away again
func (c *converter) optimizeShard(ctx context.Context, i int) error {
	rc, err := c.bkt.Get(ctx, schema.LabelsPfileNameForShard(c.name, i))
	if err != nil {
		return fmt.Errorf("unable to fetch labels parquet file: %w", err)
	}
	defer rc.Close()

	rbuf := bytes.NewBuffer(nil)
	if _, err := io.Copy(rbuf, rc); err != nil {
		return fmt.Errorf("unable to copy labels parquet file: %w", err)
	}

	pf, err := parquet.OpenFile(bytes.NewReader(rbuf.Bytes()), int64(rbuf.Len()))
	if err != nil {
		return fmt.Errorf("unable to open labels parquet file: %w", err)
	}

	s := pf.Schema()
	ns := schema.WithCompression(schema.RemoveNullColumns(pf))

	buf := bytes.NewBuffer(nil)
	w := parquet.NewGenericWriter[any](buf, append(c.labelWriterOptions(), ns)...)

	rb := parquet.NewRowBuilder(ns)
	rowBuf := make([]parquet.Row, 128)
	colIdxSlice := make([]int, 0)
	labelIndexColumn := columnIDForKnownColumn(ns, schema.LabelIndexColumn)

	for _, rg := range pf.RowGroups() {
		rows := rg.Rows()
		defer rows.Close()

		for {
			n, err := rows.ReadRows(rowBuf)
			if err != nil && !errors.Is(err, io.EOF) {
				return fmt.Errorf("unable to read rows: %w", err)
			}

			for i, row := range rowBuf[:n] {
				rb.Reset()
				colIdxSlice = colIdxSlice[:0]
				for j, v := range row {
					if !v.IsNull() {
						if lc, ok := ns.Lookup(s.Columns()[j]...); ok && lc.ColumnIndex != labelIndexColumn {
							colIdxSlice = append(colIdxSlice, lc.ColumnIndex)
							rb.Add(lc.ColumnIndex, v)
						}
					}
				}
				rb.Add(labelIndexColumn, parquet.ValueOf(encoding.EncodeLabelColumnIndex(colIdxSlice)))
				rowBuf[i] = rb.AppendRow(rowBuf[i][:0])
			}

			if m, err := w.WriteRows(rowBuf[:n]); err != nil {
				return fmt.Errorf("unable to write transformed rows: %w", err)
			} else if m != n {
				return fmt.Errorf("unable to write rows: %d != %d", n, m)
			}

			if errors.Is(err, io.EOF) {
				break
			}
		}
	}
	if err := w.Close(); err != nil {
		return fmt.Errorf("unable to close writer: %w", err)
	}

	if err := c.bkt.Upload(ctx, schema.LabelsPfileNameForShard(c.name, i), buf); err != nil {
		return fmt.Errorf("unable to override optimized labels parquet file: %w", err)
	}

	return nil
}

func (c *converter) convertShard(ctx context.Context) (bool, error) {
	s := c.rr.Schema()
	rowsToWrite := c.numRowGroups * c.rowGroupSize

	w, err := newSplitFileWriter(ctx, c.bkt, s, map[string]writerConfig{
		schema.LabelsPfileNameForShard(c.name, c.currentShard): {
			s:    schema.WithCompression(schema.LabelsProjection(s)),
			opts: c.labelWriterOptions(),
		},
		schema.ChunksPfileNameForShard(c.name, c.currentShard): {
			s:    schema.WithCompression(schema.ChunkProjection(s)),
			opts: c.chunkWriterOptions(),
		},
	},
	)
	if err != nil {
		return false, fmt.Errorf("unable to build multifile writer: %w", err)
	}

	n, err := parquet.CopyRows(w, newBufferedReader(ctx, newLimitReader(c.rr, rowsToWrite)))
	if err != nil {
		return false, fmt.Errorf("unable to copy rows to writer: %w", err)
	}
	if err := w.Close(); err != nil {
		return false, fmt.Errorf("unable to close multifile writer: %w", err)
	}

	if n < int64(rowsToWrite) {
		return false, nil
	}
	c.currentShard++
	return true, nil
}

type limitReader struct {
	parquet.RowReader
	limit int
	cur   int
}

func newLimitReader(r parquet.RowReader, limit int) parquet.RowReader {
	return &limitReader{RowReader: r, limit: limit}
}

func (lr *limitReader) ReadRows(buf []parquet.Row) (int, error) {
	n, err := lr.RowReader.ReadRows(buf)
	if err != nil {
		return n, err
	}
	lr.cur += n

	if lr.cur > lr.limit {
		return n, io.EOF
	}
	return n, nil
}

type fileWriter struct {
	pw   *parquet.GenericWriter[any]
	conv parquet.Conversion
	w    io.WriteCloser
	r    io.ReadCloser
}

type splitPipeFileWriter struct {
	fileWriters map[string]*fileWriter
	g           *errgroup.Group
}

type writerConfig struct {
	s    *parquet.Schema
	opts []parquet.WriterOption
}

func newSplitFileWriter(ctx context.Context, bkt objstore.Bucket, inSchema *parquet.Schema, files map[string]writerConfig) (*splitPipeFileWriter, error) {
	fileWriters := make(map[string]*fileWriter)
	g, ctx := errgroup.WithContext(ctx)
	for file, cfg := range files {
		conv, err := parquet.Convert(cfg.s, inSchema)
		if err != nil {
			return nil, fmt.Errorf("unable to convert schemas")
		}

		r, w := io.Pipe()
		bw := bufio.NewWriterSize(w, 32_000_000)
		br := bufio.NewReaderSize(r, 32_000_000)
		fileWriters[file] = &fileWriter{
			pw:   parquet.NewGenericWriter[any](bw, append(cfg.opts, cfg.s)...),
			w:    w,
			conv: conv,
		}
		g.Go(func() error {
			defer func() { _ = r.Close() }()
			return bkt.Upload(ctx, file, br)
		})
	}
	return &splitPipeFileWriter{
		fileWriters: fileWriters,
		g:           g,
	}, nil

}

func (s *splitPipeFileWriter) WriteRows(rows []parquet.Row) (int, error) {
	var g errgroup.Group
	for _, writer := range s.fileWriters {
		g.Go(func() error {
			rr := make([]parquet.Row, len(rows))
			for i, row := range rows {
				rr[i] = row.Clone()
			}
			_, err := writer.conv.Convert(rr)
			if err != nil {
				return fmt.Errorf("unable to convert rows: %w", err)
			}
			n, err := writer.pw.WriteRows(rr)
			if err != nil {
				return fmt.Errorf("unable to write rows: %w", err)
			}
			if n != len(rows) {
				return fmt.Errorf("unable to write rows: %d != %d", n, len(rows))
			}
			return nil
		})
	}
	return len(rows), g.Wait()
}

func (s *splitPipeFileWriter) Close() error {
	errs := make([]error, 0)
	for _, fw := range s.fileWriters {
		if err := fw.pw.Close(); err != nil {
			errs = append(errs, fmt.Errorf("unable to close pipewriter: %w", err))
		}
		if err := fw.w.Close(); err != nil {
			errs = append(errs, fmt.Errorf("unable to close writer: %w", err))
		}
	}
	if err := s.g.Wait(); err != nil {
		errs = append(errs, fmt.Errorf("unable to wait for group: %w", err))
	}
	return errors.Join(errs...)
}

type bufferedReader struct {
	rr parquet.RowReader

	ctx     context.Context
	c       chan []parquet.Row
	errCh   chan error
	rowPool zeropool.Pool[[]parquet.Row]

	current      []parquet.Row
	currentIndex int
}

func newBufferedReader(ctx context.Context, rr parquet.RowReader) *bufferedReader {
	br := &bufferedReader{
		rr:    rr,
		ctx:   ctx,
		c:     make(chan []parquet.Row, 128),
		errCh: make(chan error, 1),
		rowPool: zeropool.New(func() []parquet.Row {
			return make([]parquet.Row, 128)
		}),
	}

	go br.readRows()

	return br
}

func (b *bufferedReader) ReadRows(rows []parquet.Row) (int, error) {
	if b.current == nil {
		select {
		case next, ok := <-b.c:
			if !ok {
				return 0, io.EOF
			}
			b.current = next
			b.currentIndex = 0
		case err := <-b.errCh:
			return 0, err
		}
	}

	current := b.current[b.currentIndex:]
	i := min(len(current), len(rows))
	copy(rows[:i], current[:i])
	b.currentIndex += i
	if b.currentIndex >= len(b.current) {
		b.rowPool.Put(b.current[0:cap(b.current)])
		b.current = nil
	}
	return i, nil
}

func (b *bufferedReader) Close() {
	close(b.c)
	close(b.errCh)
}

func (b *bufferedReader) readRows() {
	for {
		select {
		case <-b.ctx.Done():
			b.errCh <- b.ctx.Err()
			return
		default:
			rows := b.rowPool.Get()
			n, err := b.rr.ReadRows(rows)
			if n > 0 {
				b.c <- rows[:n]
			}
			if err != nil {
				if err == io.EOF {
					close(b.c)
					return
				}
				b.errCh <- err
				return
			}
		}
	}
}
