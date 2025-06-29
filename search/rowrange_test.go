// Copyright (c) 2025 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

package search

import (
	"slices"
	"testing"
)

func TestLimit(t *testing.T) {
	for _, tt := range []struct {
		rr     []rowRange
		limit  int64
		expect []rowRange
	}{
		{
			rr:     []rowRange{{from: 0, count: 4}},
			limit:  3,
			expect: []rowRange{{from: 0, count: 3}},
		},
		{
			rr:     []rowRange{{from: 0, count: 4}, {from: 5, count: 10}},
			limit:  5,
			expect: []rowRange{{from: 0, count: 4}, {from: 5, count: 1}},
		},
		{
			rr:     []rowRange{{from: 0, count: 1e6}},
			limit:  5,
			expect: []rowRange{{from: 0, count: 5}},
		},
	} {
		t.Run("", func(t *testing.T) {
			if res := limitRowRanges(tt.limit, tt.rr); !slices.Equal(res, tt.expect) {
				t.Fatalf("Expected %v to match %v", res, tt.expect)
			}
		})
	}
}

func TestIntersect(t *testing.T) {
	for _, tt := range []struct{ lhs, rhs, expect []rowRange }{
		{
			lhs:    []rowRange{{from: 0, count: 4}},
			rhs:    []rowRange{{from: 2, count: 6}},
			expect: []rowRange{{from: 2, count: 2}},
		},
		{
			lhs:    []rowRange{{from: 0, count: 4}},
			rhs:    []rowRange{{from: 6, count: 8}},
			expect: []rowRange{},
		},
		{
			lhs:    []rowRange{{from: 0, count: 4}},
			rhs:    []rowRange{{from: 0, count: 4}},
			expect: []rowRange{{from: 0, count: 4}},
		},
		{
			lhs:    []rowRange{{from: 0, count: 4}, {from: 8, count: 2}},
			rhs:    []rowRange{{from: 2, count: 9}},
			expect: []rowRange{{from: 2, count: 2}, {from: 8, count: 2}},
		},
		{
			lhs:    []rowRange{{from: 0, count: 1}, {from: 4, count: 1}},
			rhs:    []rowRange{{from: 2, count: 1}, {from: 6, count: 1}},
			expect: []rowRange{},
		},
		{
			lhs:    []rowRange{{from: 0, count: 2}, {from: 2, count: 2}},
			rhs:    []rowRange{{from: 1, count: 2}, {from: 3, count: 2}},
			expect: []rowRange{{from: 1, count: 3}},
		},
		{
			lhs:    []rowRange{{from: 0, count: 2}, {from: 5, count: 2}},
			rhs:    []rowRange{{from: 0, count: 10}},
			expect: []rowRange{{from: 0, count: 2}, {from: 5, count: 2}},
		},
		{
			lhs:    []rowRange{{from: 0, count: 2}, {from: 3, count: 1}, {from: 5, count: 2}, {from: 12, count: 10}},
			rhs:    []rowRange{{from: 0, count: 10}, {from: 15, count: 32}},
			expect: []rowRange{{from: 0, count: 2}, {from: 3, count: 1}, {from: 5, count: 2}, {from: 15, count: 7}},
		},
		{
			lhs:    []rowRange{},
			rhs:    []rowRange{{from: 0, count: 10}},
			expect: []rowRange{},
		},
		{
			lhs:    []rowRange{{from: 0, count: 10}},
			rhs:    []rowRange{},
			expect: []rowRange{},
		},
		{
			lhs:    []rowRange{},
			rhs:    []rowRange{},
			expect: []rowRange{},
		},
		{
			lhs:    []rowRange{{from: 0, count: 2}},
			rhs:    []rowRange{{from: 0, count: 1}, {from: 1, count: 1}, {from: 2, count: 1}},
			expect: []rowRange{{from: 0, count: 2}},
		},
	} {
		t.Run("", func(t *testing.T) {
			if res := intersectRowRanges(tt.lhs, tt.rhs); !slices.Equal(res, tt.expect) {
				t.Fatalf("Expected %v to match %v", res, tt.expect)
			}
		})
	}
}

func TestComplement(t *testing.T) {
	for _, tt := range []struct{ lhs, rhs, expect []rowRange }{
		{
			lhs:    []rowRange{{from: 4, count: 3}},
			rhs:    []rowRange{{from: 2, count: 1}, {from: 5, count: 2}},
			expect: []rowRange{{from: 2, count: 1}},
		},
		{
			lhs:    []rowRange{{from: 2, count: 4}},
			rhs:    []rowRange{{from: 0, count: 7}},
			expect: []rowRange{{from: 0, count: 2}, {from: 6, count: 1}},
		},
		{
			lhs:    []rowRange{{from: 2, count: 4}},
			rhs:    []rowRange{{from: 3, count: 7}},
			expect: []rowRange{{from: 6, count: 4}},
		},
		{
			lhs:    []rowRange{{from: 8, count: 10}},
			rhs:    []rowRange{{from: 3, count: 7}},
			expect: []rowRange{{from: 3, count: 5}},
		},
		{
			lhs:    []rowRange{{from: 16, count: 10}},
			rhs:    []rowRange{{from: 3, count: 7}},
			expect: []rowRange{{from: 3, count: 7}},
		},
		{
			lhs:    []rowRange{{from: 1, count: 2}, {from: 4, count: 2}},
			rhs:    []rowRange{{from: 2, count: 2}, {from: 5, count: 8}},
			expect: []rowRange{{from: 3, count: 1}, {from: 6, count: 7}},
		},
		// Empty input cases
		{
			lhs:    []rowRange{},
			rhs:    []rowRange{{from: 1, count: 5}},
			expect: []rowRange{{from: 1, count: 5}},
		},
		{
			lhs:    []rowRange{{from: 1, count: 5}},
			rhs:    []rowRange{},
			expect: []rowRange{},
		},
		{
			lhs:    []rowRange{},
			rhs:    []rowRange{},
			expect: []rowRange{},
		},
		// Adjacent ranges
		{
			lhs:    []rowRange{{from: 1, count: 3}},
			rhs:    []rowRange{{from: 1, count: 3}, {from: 4, count: 2}},
			expect: []rowRange{{from: 4, count: 2}},
		},
		// Ranges with gaps
		{
			lhs:    []rowRange{{from: 1, count: 2}, {from: 5, count: 2}},
			rhs:    []rowRange{{from: 0, count: 8}},
			expect: []rowRange{{from: 0, count: 1}, {from: 3, count: 2}, {from: 7, count: 1}},
		},
		// Zero-count ranges
		{
			lhs:    []rowRange{{from: 1, count: 0}},
			rhs:    []rowRange{{from: 1, count: 5}},
			expect: []rowRange{{from: 1, count: 5}},
		},
		// Completely disjoint ranges
		{
			lhs:    []rowRange{{from: 1, count: 2}},
			rhs:    []rowRange{{from: 5, count: 2}},
			expect: []rowRange{{from: 5, count: 2}},
		},
		// Multiple overlapping ranges
		{
			lhs:    []rowRange{{from: 1, count: 3}, {from: 4, count: 3}, {from: 8, count: 2}},
			rhs:    []rowRange{{from: 0, count: 11}},
			expect: []rowRange{{from: 0, count: 1}, {from: 7, count: 1}, {from: 10, count: 1}},
		},
	} {
		t.Run("", func(t *testing.T) {
			if res := complementRowRanges(tt.lhs, tt.rhs); !slices.Equal(res, tt.expect) {
				t.Fatalf("Expected %v to match %v", res, tt.expect)
			}
		})
	}
}

func TestSimplify(t *testing.T) {
	for _, tt := range []struct{ in, expect []rowRange }{
		{
			in: []rowRange{
				{from: 0, count: 15},
				{from: 4, count: 4},
			},
			expect: []rowRange{
				{from: 0, count: 15},
			},
		},
		{
			in: []rowRange{
				{from: 4, count: 4},
				{from: 4, count: 2},
			},
			expect: []rowRange{
				{from: 4, count: 4},
			},
		},
		{
			in: []rowRange{
				{from: 0, count: 4},
				{from: 1, count: 5},
				{from: 8, count: 10},
			},
			expect: []rowRange{
				{from: 0, count: 6},
				{from: 8, count: 10},
			},
		},
	} {
		t.Run("", func(t *testing.T) {
			if res := simplify(tt.in); !slices.Equal(res, tt.expect) {
				t.Fatalf("Expected %v to match %v", res, tt.expect)
			}
		})
	}
}

func TestIntersects(t *testing.T) {
	for _, tt := range []struct {
		a, b   rowRange
		expect bool
	}{
		// Identical ranges
		{
			a:      rowRange{from: 0, count: 5},
			b:      rowRange{from: 0, count: 5},
			expect: true,
		},
		// Completely disjoint ranges
		{
			a:      rowRange{from: 0, count: 3},
			b:      rowRange{from: 5, count: 3},
			expect: false,
		},
		// Adjacent ranges (should not overlap as ranges are half-open)
		{
			a:      rowRange{from: 0, count: 3},
			b:      rowRange{from: 3, count: 3},
			expect: false,
		},
		// One range completely contains the other
		{
			a:      rowRange{from: 0, count: 10},
			b:      rowRange{from: 2, count: 5},
			expect: true,
		},
		// Partial overlap from left
		{
			a:      rowRange{from: 0, count: 5},
			b:      rowRange{from: 3, count: 5},
			expect: true,
		},
		// Partial overlap from right
		{
			a:      rowRange{from: 3, count: 5},
			b:      rowRange{from: 0, count: 5},
			expect: true,
		},
		// Zero-count ranges
		{
			a:      rowRange{from: 0, count: 0},
			b:      rowRange{from: 0, count: 5},
			expect: false,
		},
		{
			a:      rowRange{from: 0, count: 5},
			b:      rowRange{from: 0, count: 0},
			expect: false,
		},
		// Negative ranges (edge case)
		{
			a:      rowRange{from: -5, count: 5},
			b:      rowRange{from: -3, count: 5},
			expect: true,
		},
	} {
		t.Run("", func(t *testing.T) {
			if res := intersect(tt.a, tt.b); res != tt.expect {
				t.Fatalf("Expected %v.Overlaps(%v) to be %v, got %v", tt.a, tt.b, tt.expect, res)
			}
			// Test symmetry
			if res := intersect(tt.b, tt.a); res != tt.expect {
				t.Fatalf("Expected %v.Overlaps(%v) to be %v, got %v", tt.b, tt.a, tt.expect, res)
			}
		})
	}
}
