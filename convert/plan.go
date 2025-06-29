// Copyright (c) 2025 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

package convert

import (
	"sort"
	"time"

	"github.com/thanos-io/thanos/pkg/block/metadata"

	"github.com/cloudflare/parquet-tsdb-poc/internal/util"
	"github.com/cloudflare/parquet-tsdb-poc/schema"
)

type Plan struct {
	Download        []metadata.Meta
	ConvertForDates []time.Time
}

type Planner struct {
	// do not create parquet blocks that are younger then this
	notAfter time.Time
}

func NewPlanner(notAfter time.Time) Planner {
	return Planner{notAfter: notAfter}
}

func (p Planner) Plan(tsdbMetas map[string]metadata.Meta, parquetMetas map[string]schema.Meta) (Plan, bool) {
	var start, end time.Time

	for _, v := range tsdbMetas {
		bStart, bEnd := time.UnixMilli(v.MinTime).UTC(), time.UnixMilli(v.MaxTime).UTC()
		if start.IsZero() || bStart.Before(start) {
			start = bStart
		}
		if end.IsZero() || bEnd.After(end) {
			end = bEnd
		}
	}

	start, end = util.BeginOfDay(start), util.BeginOfDay(end)

	var first time.Time

	// find first block not covered by parquetMetas
L:
	for next := start.UTC(); !next.After(end); next = next.AddDate(0, 0, 1).UTC() {
		for _, m := range parquetMetas {
			if next.Equal(time.UnixMilli(m.Mint)) {
				// we already cover this day
				continue L
			}
		}
		first = next
		break
	}

	if first.IsZero() || first.After(p.notAfter) {
		return Plan{}, false
	}

	overlappingMetas := overlappingBlockMetas(tsdbMetas, first)
	if len(overlappingMetas) == 0 {
		return Plan{}, false
	}
	last := util.BeginOfDay(time.UnixMilli(overlappingMetas[len(overlappingMetas)-1].MaxTime).UTC())

	convertForDays := make([]time.Time, 0)
	convertForDays = append(convertForDays, first)

	if !first.Equal(last) {
		for next := first.AddDate(0, 0, 1).UTC(); !next.Equal(last); next = next.AddDate(0, 0, 1).UTC() {
			convertForDays = append(convertForDays, next)
		}
	} else if last.Equal(end) {
		// if we only convert one date and if that date is the end of the range that our TSDB
		// blocks cover fully - we just wait for more data, it could be that we get more data later
		// for this day.
		return Plan{}, false
	}

	// NOTE: if the metas cover more time, then we could amortize downloads by just converting all
	// dates the cover here. Think for example blocks in object storage that were compacted to two
	// weeks. We should only do this for full days that are covered by the downloaded blocks though.
	// We should also only consider dates that are not covered by parquet files already.

	return Plan{
		Download:        overlappingMetas,
		ConvertForDates: convertForDays,
	}, true
}

func overlappingBlockMetas(metas map[string]metadata.Meta, date time.Time) []metadata.Meta {
	res := make([]metadata.Meta, 0)
	for _, m := range metas {
		if date.AddDate(0, 0, 1).UnixMilli() >= m.MinTime && date.UnixMilli() <= m.MaxTime {
			res = append(res, m)
		}
	}
	sort.Slice(res, func(i, j int) bool {
		return res[i].MaxTime <= res[j].MaxTime
	})
	return res
}
