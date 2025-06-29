// Copyright (c) 2025 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

package convert

import (
	"encoding/binary"
	"fmt"
	"sort"
	"time"

	"github.com/prometheus/prometheus/tsdb/chunks"

	"github.com/cloudflare/parquet-tsdb-poc/internal/encoding"
	"github.com/cloudflare/parquet-tsdb-poc/schema"
)

func collectChunks(it chunks.Iterator) ([schema.ChunkColumnsPerDay][]byte, error) {
	var (
		res [schema.ChunkColumnsPerDay][]byte
	)
	// NOTE: 'it' should hold chunks for one day. Chunks are usually length 2h so we should get 12 of them.
	chunks := make([]chunks.Meta, 0, 12)
	for it.Next() {
		chunks = append(chunks, it.At())
	}
	if err := it.Err(); err != nil {
		return res, fmt.Errorf("unable to iterate chunks: %w", err)
	}
	// NOTE: we need to sort chunks here as they come from different blocks that we merged.
	// Prometheus does not guarantee that they are sorted. We have to sort them either here or
	// before submitting them to the query engine.
	sort.Slice(chunks, func(i, j int) bool {
		return chunks[i].MinTime < chunks[j].MinTime
	})
	for _, chk := range chunks {
		if chk.Chunk.NumSamples() == 0 {
			continue
		}
		enc, bs := chk.Chunk.Encoding(), chk.Chunk.Bytes()
		hour := time.UnixMilli(chk.MinTime).UTC().Hour()
		chkIdx := (hour / int(schema.ChunkColumnLength.Hours())) % schema.ChunkColumnsPerDay
		chkBytes := res[chkIdx]
		chkBytes = binary.BigEndian.AppendUint32(chkBytes, uint32(enc))
		chkBytes = binary.BigEndian.AppendUint64(chkBytes, encoding.ZigZagEncode(chk.MinTime))
		chkBytes = binary.BigEndian.AppendUint64(chkBytes, encoding.ZigZagEncode(chk.MaxTime))
		chkBytes = binary.BigEndian.AppendUint32(chkBytes, uint32(len(bs)))
		chkBytes = append(chkBytes, bs...)
		res[chkIdx] = chkBytes
	}
	return res, nil
}
