// Copyright (c) 2025 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

package convert

import (
	"math"
	"slices"
	"testing"
	"time"

	"github.com/oklog/ulid/v2"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/thanos-io/thanos/pkg/block/metadata"

	"github.com/cloudflare/parquet-tsdb-poc/schema"
)

func TestPlanner(t *testing.T) {
	for _, tc := range []struct {
		name string

		notAfter     time.Time
		tsdbMetas    map[string]metadata.Meta
		parquetMetas map[string]schema.Meta

		expectOk    bool
		expectULIDS []ulid.ULID
		expectDates []time.Time
	}{
		{
			name:     "last day only partially covered",
			notAfter: time.UnixMilli(math.MaxInt64),
			tsdbMetas: map[string]metadata.Meta{
				"01JT0DPYGA1HPW5RBZ1KBXCNXK": {
					BlockMeta: tsdb.BlockMeta{
						ULID:    ulid.MustParse("01JT0DPYGA1HPW5RBZ1KBXCNXK"),
						MinTime: time.Date(2025, time.March, 2, 11, 0, 0, 0, time.UTC).UnixMilli(),
						MaxTime: time.Date(2025, time.March, 3, 5, 0, 0, 0, time.UTC).UnixMilli(),
					},
				},
			},
			parquetMetas: map[string]schema.Meta{
				"2025/03/02": {
					Name: "2025/03/02",
					Mint: time.Date(2025, time.March, 2, 0, 0, 0, 0, time.UTC).UnixMilli(),
					Maxt: time.Date(2025, time.March, 3, 0, 0, 0, 0, time.UTC).UnixMilli(),
				},
			},
			expectOk: false,
		},
		{
			name:     "three blocks cover a full day, previous parquet file for intial overlap",
			notAfter: time.UnixMilli(math.MaxInt64),
			tsdbMetas: map[string]metadata.Meta{
				"01JT0DPYGA1HPW5RBZ1KBXCNXK": {
					BlockMeta: tsdb.BlockMeta{
						ULID:    ulid.MustParse("01JT0DPYGA1HPW5RBZ1KBXCNXK"),
						MinTime: time.Date(2025, time.March, 2, 11, 0, 0, 0, time.UTC).UnixMilli(),
						MaxTime: time.Date(2025, time.March, 3, 5, 0, 0, 0, time.UTC).UnixMilli(),
					},
				},
				"01JT0DWY7YB7TE9TNHY5NTAYWT": {
					BlockMeta: tsdb.BlockMeta{
						ULID:    ulid.MustParse("01JT0DWY7YB7TE9TNHY5NTAYWT"),
						MinTime: time.Date(2025, time.March, 3, 5, 0, 0, 0, time.UTC).UnixMilli(),
						MaxTime: time.Date(2025, time.March, 3, 18, 0, 0, 0, time.UTC).UnixMilli(),
					},
				},
				"01JT0DYXS7CJ7VCFH63WD1S006": {
					BlockMeta: tsdb.BlockMeta{
						ULID:    ulid.MustParse("01JT0DYXS7CJ7VCFH63WD1S006"),
						MinTime: time.Date(2025, time.March, 3, 18, 0, 0, 0, time.UTC).UnixMilli(),
						MaxTime: time.Date(2025, time.March, 4, 4, 0, 0, 0, time.UTC).UnixMilli(),
					},
				},
			},
			parquetMetas: map[string]schema.Meta{
				"2025/03/02": {
					Name: "2025/03/02",
					Mint: time.Date(2025, time.March, 2, 0, 0, 0, 0, time.UTC).UnixMilli(),
					Maxt: time.Date(2025, time.March, 3, 0, 0, 0, 0, time.UTC).UnixMilli(),
				},
			},
			expectOk: true,
			expectULIDS: []ulid.ULID{
				ulid.MustParse("01JT0DPYGA1HPW5RBZ1KBXCNXK"),
				ulid.MustParse("01JT0DWY7YB7TE9TNHY5NTAYWT"),
				ulid.MustParse("01JT0DYXS7CJ7VCFH63WD1S006"),
			},
			expectDates: []time.Time{
				time.Date(2025, time.March, 3, 0, 0, 0, 0, time.UTC),
			},
		},
		{
			name:     "we dont convert blocks that are too young still",
			notAfter: time.Date(2025, time.March, 1, 0, 0, 0, 0, time.UTC),
			tsdbMetas: map[string]metadata.Meta{
				"01JT0DPYGA1HPW5RBZ1KBXCNXK": {
					BlockMeta: tsdb.BlockMeta{
						ULID:    ulid.MustParse("01JT0DPYGA1HPW5RBZ1KBXCNXK"),
						MinTime: time.Date(2025, time.March, 2, 0, 0, 0, 0, time.UTC).UnixMilli(),
						MaxTime: time.Date(2025, time.March, 3, 0, 0, 0, 0, time.UTC).UnixMilli(),
					},
				},
			},
			parquetMetas: map[string]schema.Meta{},
			expectOk:     false,
		},
		{
			name:     "we have all blocks already",
			notAfter: time.UnixMilli(math.MaxInt64),
			tsdbMetas: map[string]metadata.Meta{
				"01JT0DPYGA1HPW5RBZ1KBXCNXK": {
					BlockMeta: tsdb.BlockMeta{
						ULID:    ulid.MustParse("01JT0DPYGA1HPW5RBZ1KBXCNXK"),
						MinTime: time.Date(2020, time.January, 4, 12, 0, 0, 0, time.UTC).UnixMilli(),
						MaxTime: time.Date(2020, time.January, 24, 18, 0, 0, 0, time.UTC).UnixMilli(),
					},
				},
				"01JU0DPYGA1HPW5RBZ1KBXCNXK": {
					BlockMeta: tsdb.BlockMeta{
						ULID:    ulid.MustParse("01JU0DPYGA1HPW5RBZ1KBXCNXK"),
						MinTime: time.Date(2020, time.January, 24, 18, 0, 0, 0, time.UTC).UnixMilli(),
						MaxTime: time.Date(2020, time.January, 25, 12, 0, 0, 0, time.UTC).UnixMilli(),
					},
				},
			},
			parquetMetas: map[string]schema.Meta{
				"2020/01/04": {
					Name: "2020/01/04",
					Mint: time.Date(2020, time.January, 4, 0, 0, 0, 0, time.UTC).UnixMilli(),
					Maxt: time.Date(2020, time.January, 5, 0, 0, 0, 0, time.UTC).UnixMilli(),
				},
			},
			expectOk: true,
			expectULIDS: []ulid.ULID{
				ulid.MustParse("01JT0DPYGA1HPW5RBZ1KBXCNXK"),
			},
			expectDates: []time.Time{
				time.Date(2020, time.January, 5, 0, 0, 0, 0, time.UTC),
				time.Date(2020, time.January, 6, 0, 0, 0, 0, time.UTC),
				time.Date(2020, time.January, 7, 0, 0, 0, 0, time.UTC),
				time.Date(2020, time.January, 8, 0, 0, 0, 0, time.UTC),
				time.Date(2020, time.January, 9, 0, 0, 0, 0, time.UTC),
				time.Date(2020, time.January, 10, 0, 0, 0, 0, time.UTC),
				time.Date(2020, time.January, 11, 0, 0, 0, 0, time.UTC),
				time.Date(2020, time.January, 12, 0, 0, 0, 0, time.UTC),
				time.Date(2020, time.January, 13, 0, 0, 0, 0, time.UTC),
				time.Date(2020, time.January, 14, 0, 0, 0, 0, time.UTC),
				time.Date(2020, time.January, 15, 0, 0, 0, 0, time.UTC),
				time.Date(2020, time.January, 16, 0, 0, 0, 0, time.UTC),
				time.Date(2020, time.January, 17, 0, 0, 0, 0, time.UTC),
				time.Date(2020, time.January, 18, 0, 0, 0, 0, time.UTC),
				time.Date(2020, time.January, 19, 0, 0, 0, 0, time.UTC),
				time.Date(2020, time.January, 20, 0, 0, 0, 0, time.UTC),
				time.Date(2020, time.January, 21, 0, 0, 0, 0, time.UTC),
				time.Date(2020, time.January, 22, 0, 0, 0, 0, time.UTC),
				time.Date(2020, time.January, 23, 0, 0, 0, 0, time.UTC),
			},
		},
		{
			name:     "we can amoritize downloads by converting as many blocks as possible",
			notAfter: time.UnixMilli(math.MaxInt64),
			tsdbMetas: map[string]metadata.Meta{
				"01JT0DPYGA1HPW5RBZ1KBXCNXK": {
					BlockMeta: tsdb.BlockMeta{
						ULID:    ulid.MustParse("01JT0DPYGA1HPW5RBZ1KBXCNXK"),
						MinTime: time.Date(2020, time.January, 4, 12, 0, 0, 0, time.UTC).UnixMilli(),
						MaxTime: time.Date(2020, time.January, 7, 18, 0, 0, 0, time.UTC).UnixMilli(),
					},
				},
			},
			parquetMetas: map[string]schema.Meta{},
			expectOk:     true,
			expectULIDS: []ulid.ULID{
				ulid.MustParse("01JT0DPYGA1HPW5RBZ1KBXCNXK"),
			},
			expectDates: []time.Time{
				time.Date(2020, time.January, 4, 0, 0, 0, 0, time.UTC),
				time.Date(2020, time.January, 5, 0, 0, 0, 0, time.UTC),
				time.Date(2020, time.January, 6, 0, 0, 0, 0, time.UTC),
			},
		},
		{
			name:     "upload gap does not stall converter",
			notAfter: time.UnixMilli(math.MaxInt64),
			tsdbMetas: map[string]metadata.Meta{
				"01JS0DPYGA1HPW5RBZ1KBXCNXK": {
					BlockMeta: tsdb.BlockMeta{
						ULID:    ulid.MustParse("01JS0DPYGA1HPW5RBZ1KBXCNXK"),
						MinTime: time.Date(2020, time.January, 4, 12, 0, 0, 0, time.UTC).UnixMilli(),
						MaxTime: time.Date(2020, time.January, 5, 18, 0, 0, 0, time.UTC).UnixMilli(),
					},
				},
				"01JT0DPYGA1HPW5RBZ1KBXCNXK": {
					BlockMeta: tsdb.BlockMeta{
						ULID:    ulid.MustParse("01JT0DPYGA1HPW5RBZ1KBXCNXK"),
						MinTime: time.Date(2020, time.January, 5, 18, 0, 0, 0, time.UTC).UnixMilli(),
						MaxTime: time.Date(2020, time.January, 6, 12, 0, 0, 0, time.UTC).UnixMilli(),
					},
				},
				"01JU0DPYGA1HPW5RBZ1KBXCNXK": {
					BlockMeta: tsdb.BlockMeta{
						ULID:    ulid.MustParse("01JU0DPYGA1HPW5RBZ1KBXCNXK"),
						MinTime: time.Date(2020, time.January, 7, 18, 0, 0, 0, time.UTC).UnixMilli(),
						MaxTime: time.Date(2020, time.January, 8, 6, 0, 0, 0, time.UTC).UnixMilli(),
					},
				},
			},
			parquetMetas: map[string]schema.Meta{
				"2020/01/04": {
					Name: "2020/01/04",
					Mint: time.Date(2020, time.January, 4, 0, 0, 0, 0, time.UTC).UnixMilli(),
					Maxt: time.Date(2020, time.January, 5, 0, 0, 0, 0, time.UTC).UnixMilli(),
				},
				"2020/01/05": {
					Name: "2020/01/05",
					Mint: time.Date(2020, time.January, 5, 0, 0, 0, 0, time.UTC).UnixMilli(),
					Maxt: time.Date(2020, time.January, 6, 0, 0, 0, 0, time.UTC).UnixMilli(),
				},
			},
			expectOk: true,
			expectULIDS: []ulid.ULID{
				ulid.MustParse("01JT0DPYGA1HPW5RBZ1KBXCNXK"),
			},
			expectDates: []time.Time{
				time.Date(2020, time.January, 6, 0, 0, 0, 0, time.UTC),
			},
		},
	} {
		t.Run(tc.name, func(tt *testing.T) {
			plan, ok := NewPlanner(tc.notAfter).Plan(tc.tsdbMetas, tc.parquetMetas)
			if tc.expectOk != ok {
				tt.Fatalf("expected %t to equal %t", ok, tc.expectOk)
			}
			if !slices.Equal(tc.expectDates, plan.ConvertForDates) {
				tt.Fatalf("expected %q to equal %q", plan.ConvertForDates, tc.expectDates)
			}
			if !slices.EqualFunc(tc.expectULIDS, plan.Download, func(l ulid.ULID, r metadata.Meta) bool {
				return l == r.ULID
			}) {
				tt.Fatalf("expected %+v, to equal %q", plan.Download, tc.expectULIDS)
			}
		})
	}
}
