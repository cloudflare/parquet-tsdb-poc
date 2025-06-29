// Copyright (c) 2025 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

package search

import (
	"math"
	"slices"
	"testing"
)

// TODO: add more tests and maybe even a sharper way to partition
func TestPartitioner(t *testing.T) {
	type page struct {
		fromBytes uint64
		toBytes   uint64
	}
	for _, tc := range []struct {
		maxRangeSize uint64
		maxGapSize   uint64
		pages        []page
		expect       []part
	}{
		{
			maxRangeSize: 15,
			maxGapSize:   2,
			pages: []page{
				{fromBytes: 0, toBytes: 3},
				{fromBytes: 4, toBytes: 6},
				{fromBytes: 6, toBytes: 15},
				{fromBytes: 10, toBytes: 40},
				{fromBytes: 43, toBytes: 44},
				{fromBytes: 46, toBytes: 58},
				{fromBytes: 58, toBytes: 59},
			},
			expect: []part{
				{start: 0, end: 15, elemRng: [2]int{0, 3}},
				{start: 10, end: 40, elemRng: [2]int{3, 4}},
				{start: 43, end: 58, elemRng: [2]int{4, 6}},
				{start: 58, end: 59, elemRng: [2]int{6, 7}},
			},
		},
		{
			maxRangeSize: 10,
			maxGapSize:   10,
			pages: []page{
				{fromBytes: 0, toBytes: 10},
				{fromBytes: 11, toBytes: 20},
				{fromBytes: 21, toBytes: 30},
				{fromBytes: 31, toBytes: 40},
			},
			expect: []part{
				{start: 0, end: 10, elemRng: [2]int{0, 1}},
				{start: 11, end: 20, elemRng: [2]int{1, 2}},
				{start: 21, end: 30, elemRng: [2]int{2, 3}},
				{start: 31, end: 40, elemRng: [2]int{3, 4}},
			},
		},
		{
			maxRangeSize: 20,
			maxGapSize:   100,
			pages: []page{
				{fromBytes: 0, toBytes: 10},
				{fromBytes: 21, toBytes: 30},
				{fromBytes: 31, toBytes: 40},
			},
			expect: []part{
				{start: 0, end: 10, elemRng: [2]int{0, 1}},
				{start: 21, end: 40, elemRng: [2]int{1, 3}},
			},
		},
		{
			maxRangeSize: 100,
			maxGapSize:   5,
			pages: []page{
				{fromBytes: 10, toBytes: 20},
				{fromBytes: 26, toBytes: 40},
				{fromBytes: 42, toBytes: 55},
				{fromBytes: 60, toBytes: 75},
				{fromBytes: 81, toBytes: 90},
			},
			expect: []part{
				{start: 10, end: 20, elemRng: [2]int{0, 1}},
				{start: 26, end: 75, elemRng: [2]int{1, 4}},
				{start: 81, end: 90, elemRng: [2]int{4, 5}},
			},
		},
		{
			maxRangeSize: 30,
			maxGapSize:   10,
			pages: []page{
				{fromBytes: 5, toBytes: 15},
				{fromBytes: 18, toBytes: 25},
				{fromBytes: 26, toBytes: 35},
				{fromBytes: 36, toBytes: 45},
				{fromBytes: 50, toBytes: 60},
			},
			expect: []part{
				{start: 5, end: 35, elemRng: [2]int{0, 3}},
				{start: 36, end: 60, elemRng: [2]int{3, 5}},
			},
		},
		{
			maxRangeSize: 100,
			maxGapSize:   20,
			pages: []page{
				{fromBytes: 10, toBytes: 30},
				{fromBytes: 50, toBytes: 70},
				{fromBytes: 91, toBytes: 100},
			},
			expect: []part{
				{start: 10, end: 70, elemRng: [2]int{0, 2}},
				{start: 91, end: 100, elemRng: [2]int{2, 3}},
			},
		},
		{
			maxRangeSize: 50,
			maxGapSize:   10,
			pages: []page{
				{fromBytes: 10, toBytes: 10},
				{fromBytes: 15, toBytes: 25},
				{fromBytes: 20, toBytes: 30},
				{fromBytes: 42, toBytes: 42},
				{fromBytes: 45, toBytes: 60},
			},
			expect: []part{
				{start: 10, end: 30, elemRng: [2]int{0, 3}},
				{start: 42, end: 60, elemRng: [2]int{3, 5}},
			},
		},
		{
			maxRangeSize: math.MaxUint64,
			maxGapSize:   math.MaxUint64,
			pages: []page{
				{fromBytes: 10, toBytes: 20},
				{fromBytes: 100, toBytes: 200},
				{fromBytes: 1000, toBytes: 2000},
				{fromBytes: 10000, toBytes: 20000},
			},
			expect: []part{
				{start: 10, end: 20000, elemRng: [2]int{0, 4}},
			},
		},
	} {
		t.Run("", func(tt *testing.T) {
			gen := func(i int) (uint64, uint64) {
				p := tc.pages[i]
				return p.fromBytes, p.toBytes
			}
			part := newGapBasedPartitioner(tc.maxRangeSize, tc.maxGapSize)

			if got := part.partition(len(tc.pages), gen); !slices.Equal(got, tc.expect) {
				tt.Fatalf("expected %+v, got %+v", tc.expect, got)
			}
		})
	}
}
