// Copyright (c) 2025 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

package search

import (
	"bytes"
	"slices"
	"testing"

	"github.com/parquet-go/parquet-go"
	"github.com/prometheus/prometheus/model/labels"
)

func buildFile[T any](t testing.TB, rows []T) *parquet.File {
	buf := bytes.NewBuffer(nil)
	w := parquet.NewGenericWriter[T](buf, parquet.PageBufferSize(12), parquet.WriteBufferSize(0))
	for _, row := range rows {
		if _, err := w.Write([]T{row}); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	reader := bytes.NewReader(buf.Bytes())
	file, err := parquet.OpenFile(reader, reader.Size())
	if err != nil {
		t.Fatal(err)
	}
	return file
}

func mustNewFastRegexMatcher(t *testing.T, s string) *labels.FastRegexMatcher {
	res, err := labels.NewFastRegexMatcher(s)
	if err != nil {
		t.Fatalf("unable to build fast regex matcher: %s", err)
	}
	return res
}

func TestFilter(t *testing.T) {
	type expectation struct {
		constraints []Constraint
		expect      []rowRange
	}
	type testcase[T any] struct {
		rows         []T
		expectations []expectation
	}

	t.Run("", func(t *testing.T) {
		type s struct {
			A string `parquet:",optional,dict"`
			B string `parquet:",optional,dict"`
			C string `parquet:",optional,dict"`
		}
		for _, tc := range []testcase[s]{
			{
				rows: []s{
					{A: "1", B: "2", C: "a"},
					{A: "3", B: "4", C: "b"},
					{A: "7", B: "12", C: "c"},
					{A: "9", B: "22", C: "d"},
					{A: "0", B: "1", C: "e"},
					{A: "7", B: "1", C: "f"},
					{A: "7", B: "1", C: "g"},
					{A: "0", B: "1", C: "h"},
				},
				expectations: []expectation{
					{
						constraints: []Constraint{
							Equal("A", parquet.ValueOf("7")),
							Equal("C", parquet.ValueOf("g")),
						},
						expect: []rowRange{
							{from: 6, count: 1},
						},
					},
					{
						constraints: []Constraint{
							Equal("A", parquet.ValueOf("7")),
						},
						expect: []rowRange{
							{from: 2, count: 1},
							{from: 5, count: 2},
						},
					},
					{
						constraints: []Constraint{
							Equal("A", parquet.ValueOf("7")), Not(Equal("B", parquet.ValueOf("1"))),
						},
						expect: []rowRange{
							{from: 2, count: 1},
						},
					},
					{
						constraints: []Constraint{
							Equal("A", parquet.ValueOf("7")), Not(Equal("C", parquet.ValueOf("c"))),
						},
						expect: []rowRange{
							{from: 5, count: 2},
						},
					},
					{
						constraints: []Constraint{
							Not(Equal("A", parquet.ValueOf("227"))),
						},
						expect: []rowRange{
							{from: 0, count: 8},
						},
					},
					{
						constraints: []Constraint{
							Regex("C", mustNewFastRegexMatcher(t, "a|c|d")),
						},
						expect: []rowRange{
							{from: 0, count: 1},
							{from: 2, count: 2},
						},
					},
				},
			},
			{
				rows: []s{
					{A: "1", B: "2"},
					{A: "1", B: "3"},
					{A: "1", B: "4"},
					{A: "1", B: "4"},
					{A: "1", B: "5"},
					{A: "1", B: "5"},
					{A: "2", B: "5"},
					{A: "2", B: "5"},
					{A: "2", B: "5"},
					{A: "3", B: "5"},
					{A: "3", B: "6"},
					{A: "3", B: "2"},
					{A: "4", B: "8", C: "foo"},
				},
				expectations: []expectation{
					{
						constraints: []Constraint{
							Regex("C", mustNewFastRegexMatcher(t, "")),
						},
						expect: []rowRange{
							{from: 0, count: 12},
						},
					},
					{
						constraints: []Constraint{
							Equal("C", parquet.ValueOf("")),
						},
						expect: []rowRange{
							{from: 0, count: 12},
						},
					},
					{
						constraints: []Constraint{
							Not(Equal("A", parquet.ValueOf("3"))),
						},
						expect: []rowRange{
							{from: 0, count: 9},
							{from: 12, count: 1},
						},
					},
					{
						constraints: []Constraint{
							Not(Equal("A", parquet.ValueOf("3"))),
							Equal("B", parquet.ValueOf("5")),
						},
						expect: []rowRange{
							{from: 4, count: 5},
						},
					},
					{
						constraints: []Constraint{
							Not(Equal("A", parquet.ValueOf("3"))),
							Not(Equal("A", parquet.ValueOf("1"))),
						},
						expect: []rowRange{
							{from: 6, count: 3},
							{from: 12, count: 1},
						},
					},
					{
						constraints: []Constraint{
							Equal("A", parquet.ValueOf("2")),
							Not(Equal("B", parquet.ValueOf("5"))),
						},
						expect: []rowRange{},
					},
					{
						constraints: []Constraint{
							Equal("A", parquet.ValueOf("2")),
							Not(Equal("B", parquet.ValueOf("5"))),
						},
						expect: []rowRange{},
					},
					{
						constraints: []Constraint{
							Equal("A", parquet.ValueOf("3")),
							Not(Equal("B", parquet.ValueOf("2"))),
						},
						expect: []rowRange{
							{from: 9, count: 2},
						},
					},
				},
			},
			{
				rows: []s{
					{A: "1", B: "1"},
					{A: "1", B: "2"},
					{A: "2", B: "1"},
					{A: "2", B: "2"},
					{A: "1", B: "1"},
					{A: "1", B: "2"},
					{A: "2", B: "1"},
					{A: "2", B: "2"},
				},
				expectations: []expectation{
					{
						constraints: []Constraint{
							Not(Equal("A", parquet.ValueOf("1"))),
							Not(Equal("B", parquet.ValueOf("2"))),
						},
						expect: []rowRange{
							{from: 2, count: 1},
							{from: 6, count: 1},
						},
					},
				},
			},
			{
				rows: []s{
					{C: "foo"},
					{C: "bar"},
					{C: "foo"},
					{C: "buz"},
				},
				expectations: []expectation{
					{
						constraints: []Constraint{
							Regex("C", mustNewFastRegexMatcher(t, "f.*")),
						},
						expect: []rowRange{
							{from: 0, count: 1},
							{from: 2, count: 1},
						},
					},
					{
						constraints: []Constraint{
							Regex("C", mustNewFastRegexMatcher(t, "b.*")),
						},
						expect: []rowRange{
							{from: 1, count: 1},
							{from: 3, count: 1},
						},
					},
					{
						constraints: []Constraint{
							Regex("C", mustNewFastRegexMatcher(t, "f.*|b.*")),
						},
						expect: []rowRange{
							{from: 0, count: 4},
						},
					},
				},
			},
		} {

			ctx := contextWithMethod(t.Context(), "test")
			sfile := buildFile(t, tc.rows)
			for _, expectation := range tc.expectations {
				t.Run("", func(t *testing.T) {
					if err := initialize(sfile.Schema(), expectation.constraints...); err != nil {
						t.Fatal(err)
					}
					for _, rg := range sfile.RowGroups() {
						rr, err := filter(ctx, rg, expectation.constraints...)
						if err != nil {
							t.Fatal(err)
						}
						if !slices.Equal(rr, expectation.expect) {
							t.Fatalf("expected %+v, got %+v", expectation.expect, rr)
						}
					}
				})
			}
		}
	})
}
