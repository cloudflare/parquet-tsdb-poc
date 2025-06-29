// Copyright (c) 2025 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

package search

import "math"

type part struct {
	start uint64
	end   uint64

	elemRng [2]int
}

type partitioner interface {
	// Partition partitions length entries into n <= length ranges that cover all input ranges.
	// It supports overlapping ranges.
	// It expects range to be sorted by lower bound.
	partition(length int, rng func(int) (uint64, uint64)) []part
}

type gapBasedPartitioner struct {
	maxGapSize   uint64
	maxRangeSize uint64
}

func newGapBasedPartitioner(maxRangeSize, maxGapSize uint64) partitioner {
	return gapBasedPartitioner{
		maxGapSize:   maxGapSize,
		maxRangeSize: maxRangeSize,
	}
}

// partition partitions length entries into n <= length ranges that cover all
// input ranges by combining entries that are separated by reasonably small gaps.
// It is used to combine multiple small ranges from object storage into bigger, more efficient/cheaper ones.
func (g gapBasedPartitioner) partition(length int, rng func(int) (uint64, uint64)) (parts []part) {
	for k := 0; k < length; {
		j := k
		k++

		p := part{}
		p.start, p.end = rng(j)

		// Keep growing the range until the end or we encounter a large gap.
		for ; k < length; k++ {
			s, e := rng(k)

			if e-p.start > g.maxRangeSize {
				break
			}

			if g.maxGapSize != math.MaxUint64 && p.end+g.maxGapSize < s {
				break
			}

			if p.end < e {
				p.end = e
			}
		}
		p.elemRng = [2]int{j, k}
		parts = append(parts, p)
	}
	return parts
}
