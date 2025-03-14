// Copyright (c) 2025 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

package search

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/hashicorp/go-multierror"
	"github.com/parquet-go/parquet-go"

	"github.com/cloudflare/parquet-tsdb-poc/internal/tracing"
	"github.com/cloudflare/parquet-tsdb-poc/schema"
)

type Constraint interface {
	// rowRanges returns a set of non-overlapping increasing row indexes that may satisfy the constraint.
	rowRanges(rg parquet.RowGroup) ([]rowRange, error)
	// accept returns if this constraint is satisfied by the row.
	accept(r parquet.Row) bool
	// init initializes the constraint with respect to the file schema and projections.
	init(s *parquet.Schema) error
}

type RowReaderCloser interface {
	parquet.RowReader
	io.Closer
}

func Match(
	ctx context.Context,
	c Constraint,
	labelPfile *parquet.File,
	labelSchema *parquet.Schema,
	chunkPfile *parquet.File,
	chunkSchema *parquet.Schema,
) (RowReaderCloser, error) {
	ctx, span := tracing.Tracer().Start(ctx, "Match")
	defer span.End()

	labelRowGroups := labelPfile.RowGroups()
	chunkRowGroups := chunkPfile.RowGroups()

	joinedSchema := schema.Joined(labelSchema, chunkSchema)
	if err := c.init(joinedSchema); err != nil {
		return nil, fmt.Errorf("unable to initialize constraints: %w", err)
	}

	// label and chunk files have same number of rows and rowgroups, just pick either
	numRowGroups := len(labelRowGroups)

	rrs := make([]RowReaderCloser, 0, numRowGroups)
	for i := 0; i != numRowGroups; i++ {
		ranges, err := c.rowRanges(labelRowGroups[i])
		if err != nil {
			return nil, fmt.Errorf("unable to compute ranges for row group: %w", err)
		}
		if len(ranges) == 0 {
			continue
		}

		columnChunks := make([]parquet.ColumnChunk, 0, len(joinedSchema.Columns()))
		for _, p := range joinedSchema.Columns() {
			if col, ok := labelRowGroups[i].Schema().Lookup(p...); ok {
				columnChunks = append(columnChunks, labelRowGroups[i].ColumnChunks()[col.ColumnIndex])
			} else if col, ok := chunkRowGroups[i].Schema().Lookup(p...); ok {
				columnChunks = append(columnChunks, chunkRowGroups[i].ColumnChunks()[col.ColumnIndex])
			} else {
				// nothing to read here really
				continue
			}
		}
		rrs = append(rrs, newRangesRowReader(ranges, newRowGroupRows(joinedSchema, columnChunks)))
	}
	return newFilterRowReader(newConcatRowReader(rrs), c.accept), nil
}

type rangesRowReader struct {
	ranges []rowRange
	rows   parquet.Rows

	n       int
	rMaxRow int
	rCurRow int
}

func newRangesRowReader(ranges []rowRange, rows parquet.Rows) *rangesRowReader {
	return &rangesRowReader{ranges: ranges, rows: rows, n: -1}
}

func (r *rangesRowReader) next() error {
	if r.n == len(r.ranges)-1 {
		return io.EOF
	}
	r.n++
	r.rMaxRow = int(r.ranges[r.n].count)
	r.rCurRow = 0
	return r.rows.SeekToRow(r.ranges[r.n].from)
}

func (r *rangesRowReader) ReadRows(buf []parquet.Row) (int, error) {
	canRead := r.rMaxRow - r.rCurRow
	if canRead == 0 {
		if err := r.next(); err != nil {
			return 0, err
		}
		canRead = r.rMaxRow - r.rCurRow
	}
	buf = buf[:min(len(buf), canRead)]

	n, err := r.rows.ReadRows(buf)
	if err != nil {
		return n, err
	}
	r.rCurRow += n
	return n, err

}

func (r *rangesRowReader) Close() error {
	return r.rows.Close()
}

type concatRowReader struct {
	idx int
	rrs []RowReaderCloser
}

func newConcatRowReader(rrs []RowReaderCloser) *concatRowReader {
	return &concatRowReader{rrs: rrs}
}

func (f *concatRowReader) ReadRows(r []parquet.Row) (int, error) {
	if f.idx >= len(f.rrs) {
		return 0, io.EOF
	}
	n := 0
	for n != len(r) && f.idx != len(f.rrs) {
		m, err := f.rrs[f.idx].ReadRows(r[n:])
		n += m
		if err != nil {
			if err == io.EOF {
				f.idx++
			} else {
				return n, err
			}
		}
	}
	if n != len(r) {
		return n, io.EOF
	}
	return n, nil
}

func (f *concatRowReader) Close() error {
	var err *multierror.Error
	for i := range f.rrs {
		err = multierror.Append(err, f.rrs[i].Close())
	}
	return err.ErrorOrNil()
}

type filterRowReader struct {
	rr     parquet.RowReader
	closer io.Closer
}

func newFilterRowReader(rr RowReaderCloser, accept func(r parquet.Row) bool) *filterRowReader {
	return &filterRowReader{rr: parquet.FilterRowReader(rr, accept), closer: rr}
}

func (f *filterRowReader) ReadRows(r []parquet.Row) (int, error) {
	return f.rr.ReadRows(r)
}

func (f *filterRowReader) Close() error {
	return f.closer.Close()
}

// Copied from parquet-go https://github.com/parquet-go/parquet-go/blob/main/row_group.go
// Needs to be upstreamed eventually; Adapted to work with column chunks and joined schema

type columnChunkValueReader struct {
	pages   parquet.Pages
	page    parquet.Page
	values  parquet.ValueReader
	release func(parquet.Page)
}

func (r *columnChunkValueReader) clear() {
	if r.page != nil {
		r.release(r.page)
		r.page = nil
		r.values = nil
	}
}

func (r *columnChunkValueReader) Reset() {
	if r.pages != nil {
		// Ignore errors because we are resetting the reader, if the error
		// persists we will see it on the next read, and otherwise we can
		// read back from the beginning.
		r.pages.SeekToRow(0)
	}
	r.clear()
}

func (r *columnChunkValueReader) Close() error {
	var err error
	if r.pages != nil {
		err = r.pages.Close()
		r.pages = nil
	}
	r.clear()
	return err
}

func (r *columnChunkValueReader) ReadValues(values []parquet.Value) (int, error) {
	if r.pages == nil {
		return 0, io.EOF
	}

	for {
		if r.values == nil {
			p, err := r.pages.ReadPage()
			if err != nil {
				return 0, err
			}
			r.page = p
			r.values = p.Values()
		}

		n, err := r.values.ReadValues(values)
		if n > 0 {
			return n, nil
		}
		if err == nil {
			return 0, io.ErrNoProgress
		}
		if err != io.EOF {
			return 0, err
		}
		r.clear()
	}
}

func (r *columnChunkValueReader) SeekToRow(rowIndex int64) error {
	if r.pages == nil {
		return io.ErrClosedPipe
	}
	if err := r.pages.SeekToRow(rowIndex); err != nil {
		return err
	}
	r.clear()
	return nil
}

type rowGroupRows struct {
	schema   *parquet.Schema
	bufsize  int
	buffers  []parquet.Value
	columns  []columnChunkRows
	closed   bool
	rowIndex int64
}

type columnChunkRows struct {
	offset int32
	length int32
	reader columnChunkValueReader
}

func (r *rowGroupRows) buffer(i int) []parquet.Value {
	j := (i + 0) * r.bufsize
	k := (i + 1) * r.bufsize
	return r.buffers[j:k:k]
}

func newRowGroupRows(schema *parquet.Schema, columns []parquet.ColumnChunk) *rowGroupRows {
	bufferSize := 64
	r := &rowGroupRows{
		schema:   schema,
		bufsize:  bufferSize,
		buffers:  make([]parquet.Value, len(columns)*bufferSize),
		columns:  make([]columnChunkRows, len(columns)),
		rowIndex: -1,
	}

	for i, column := range columns {
		var release func(parquet.Page)
		// Only release pages that are not byte array because the values
		// that were read from the page might be retained by the program
		// after calls to ReadRows.
		switch column.Type().Kind() {
		case parquet.ByteArray, parquet.FixedLenByteArray:
			release = func(parquet.Page) {}
		default:
			release = parquet.Release
		}
		r.columns[i].reader.release = release
		r.columns[i].reader.pages = column.Pages()
	}
	return r
}

func (r *rowGroupRows) clear() {
	for i, c := range r.columns {
		r.columns[i] = columnChunkRows{reader: c.reader}
	}
	clear(r.buffers)
}

func (r *rowGroupRows) Reset() {
	for i := range r.columns {
		r.columns[i].reader.Reset()
	}
	r.clear()
}

func (r *rowGroupRows) Close() error {
	var errs []error
	for i := range r.columns {
		c := &r.columns[i]
		c.offset = 0
		c.length = 0
		if err := c.reader.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	r.clear()
	r.closed = true
	return errors.Join(errs...)
}

func (r *rowGroupRows) SeekToRow(rowIndex int64) error {
	if r.closed {
		return io.ErrClosedPipe
	}
	if rowIndex != r.rowIndex {
		for i := range r.columns {
			if err := r.columns[i].reader.SeekToRow(rowIndex); err != nil {
				return err
			}
		}
		r.clear()
		r.rowIndex = rowIndex
	}
	return nil
}

func (r *rowGroupRows) ReadRows(rows []parquet.Row) (int, error) {
	if r.closed {
		return 0, io.EOF
	}

	for rowIndex := range rows {
		rows[rowIndex] = rows[rowIndex][:0]
	}

	// When this is the first call to ReadRows, we issue a seek to the first row
	// because this starts prefetching pages asynchronously on columns.
	//
	// This condition does not apply if SeekToRow was called before ReadRows,
	// only when ReadRows is the very first method called on the row reader.
	if r.rowIndex < 0 {
		if err := r.SeekToRow(0); err != nil {
			return 0, err
		}
	}

	eofCount := 0
	rowCount := 0

readColumnValues:
	for columnIndex := range r.columns {
		c := &r.columns[columnIndex]
		b := r.buffer(columnIndex)
		eof := false

		for rowIndex := range rows {
			numValuesInRow := 1

			for {
				if c.offset == c.length {
					n, err := c.reader.ReadValues(b)
					c.offset = 0
					c.length = int32(n)

					if n == 0 {
						if err == io.EOF {
							eof = true
							eofCount++
							break
						}
						return 0, err
					}
				}

				values := b[c.offset:c.length:c.length]
				for numValuesInRow < len(values) && values[numValuesInRow].RepetitionLevel() != 0 {
					numValuesInRow++
				}
				if numValuesInRow == 0 {
					break
				}

				rows[rowIndex] = append(rows[rowIndex], values[:numValuesInRow]...)
				rowCount = max(rowCount, rowIndex+1)
				c.offset += int32(numValuesInRow)

				if numValuesInRow != len(values) {
					break
				}
				if eof {
					continue readColumnValues
				}
				numValuesInRow = 0
			}
		}
	}

	var err error
	if eofCount > 0 {
		err = io.EOF
	}
	r.rowIndex += int64(rowCount)
	return rowCount, err
}

func (r *rowGroupRows) Schema() *parquet.Schema {
	return r.schema
}
