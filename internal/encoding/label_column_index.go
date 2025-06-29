// Copyright (c) 2025 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

package encoding

import (
	"bytes"
	"encoding/binary"
	"slices"
)

func EncodeLabelColumnIndex(s []int) []byte {
	l := make([]byte, binary.MaxVarintLen32)
	r := make([]byte, 0, len(s)*binary.MaxVarintLen32)

	slices.Sort(s)

	n := binary.PutVarint(l[:], int64(len(s)))
	r = append(r, l[:n]...)

	for i := range len(s) {
		n := binary.PutVarint(l[:], int64(s[i]))
		r = append(r, l[:n]...)
	}

	return r
}

func DecodeLabelColumnIndex(b []byte) ([]int, error) {
	buffer := bytes.NewBuffer(b)

	s, err := binary.ReadVarint(buffer)
	if err != nil {
		return nil, err
	}

	r := make([]int, 0, s)

	for range s {
		v, err := binary.ReadVarint(buffer)
		if err != nil {
			return nil, err
		}
		r = append(r, int(v))
	}

	return r, nil
}
