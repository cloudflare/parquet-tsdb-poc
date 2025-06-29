// Copyright (c) 2025 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

package search

import (
	"sort"
)

type rowRange struct {
	from  int64
	count int64
}

func intersect(a, b rowRange) bool {
	endA := a.from + a.count
	endB := b.from + b.count
	return a.from < endB && b.from < endA
}

func intersection(a, b rowRange) rowRange {
	os, oe := max(a.from, b.from), min(a.from+a.count, b.from+b.count)
	return rowRange{from: os, count: oe - os}
}

func limitRowRanges(limit int64, rr []rowRange) []rowRange {
	res := make([]rowRange, 0, len(rr))
	cur := int64(0)
	for i := range rr {
		if cur+rr[i].count > limit {
			res = append(res, rowRange{from: rr[i].from, count: rr[i].count - (rr[i].count - limit + cur)})
			break
		}
		res = append(res, rr[i])
		cur += rr[i].count
	}
	return simplify(res)
}

// intersect intersects the row ranges from left hand sight with the row ranges from rhs
// it assumes that lhs and rhs are simplified and returns a simplified result.
// it operates in o(l+r) time by cursoring through ranges with a two pointer approach.
func intersectRowRanges(lhs, rhs []rowRange) []rowRange {
	res := make([]rowRange, 0)
	for l, r := 0, 0; l < len(lhs) && r < len(rhs); {
		al, bl := lhs[l].from, lhs[l].from+lhs[l].count
		ar, br := rhs[r].from, rhs[r].from+rhs[r].count

		// check if rows intersect
		if al <= br && ar <= bl {
			os, oe := max(al, ar), min(bl, br)
			res = append(res, rowRange{from: os, count: oe - os})
		}

		// advance the cursor of the range that ends first
		if bl <= br {
			l++
		} else {
			r++
		}
	}
	return simplify(res)
}

// complementrowRanges returns the ranges that are in rhs but not in lhs.
// For example, if you have:
// lhs: [{from: 1, count: 3}]  // represents rows 1,2,3
// rhs: [{from: 0, count: 5}]  // represents rows 0,1,2,3,4
// The complement would be [{from: 0, count: 1}, {from: 4, count: 1}]  // represents rows 0,4
// because these are the rows in rhs that are not in lhs.
//
// The function assumes that lhs and rhs are simplified (no overlapping ranges)
// and returns a simplified result. It operates in O(l+r) time by using a two-pointer approach
// to efficiently process both ranges.
func complementRowRanges(lhs, rhs []rowRange) []rowRange {
	res := make([]rowRange, 0)

	l, r := 0, 0
	for l < len(lhs) && r < len(rhs) {
		al, bl := lhs[l].from, lhs[l].from+lhs[l].count
		ar, br := rhs[r].from, rhs[r].from+rhs[r].count

		// check if rows intersect
		switch {
		case al > br || ar > bl:
			// no intersection, advance cursor that ends first
			if bl <= br {
				l++
			} else {
				res = append(res, rowRange{from: ar, count: br - ar})
				r++
			}
		case al < ar && bl > br:
			// l contains r, complement of l in r is empty, advance r
			r++
		case al < ar && bl <= br:
			// l covers r from left but has room on top
			oe := min(bl, br)
			rhs[r].from += oe - ar
			rhs[r].count -= oe - ar
			l++
		case al >= ar && bl > br:
			// l covers r from right but has room on bottom
			os := max(al, ar)
			res = append(res, rowRange{from: ar, count: os - ar})
			r++
		case al >= ar && bl <= br:
			// l is included r
			os, oe := max(al, ar), min(bl, br)
			res = append(res, rowRange{from: rhs[r].from, count: os - rhs[r].from})
			rhs[r].from = oe
			rhs[r].count = br - oe
			l++
		}
	}

	for ; r < len(rhs); r++ {
		res = append(res, rhs[r])
	}

	return simplify(res)
}

func simplify(rr []rowRange) []rowRange {
	if len(rr) == 0 {
		return nil
	}

	sort.Slice(rr, func(i, j int) bool {
		return rr[i].from < rr[j].from
	})

	tmp := make([]rowRange, 0)
	l := rr[0]
	for i := 1; i < len(rr); i++ {
		r := rr[i]
		al, bl := l.from, l.from+l.count
		ar, br := r.from, r.from+r.count
		if bl < ar {
			tmp = append(tmp, l)
			l = r
			continue
		}

		from := min(al, ar)
		count := max(bl, br) - from
		if count == 0 {
			continue
		}

		l = rowRange{
			from:  from,
			count: count,
		}
	}

	tmp = append(tmp, l)
	res := make([]rowRange, 0, len(tmp))
	for i := range tmp {
		if tmp[i].count != 0 {
			res = append(res, tmp[i])
		}
	}

	return res
}
