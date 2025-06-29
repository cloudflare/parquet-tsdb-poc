// Copyright (c) 2025 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

package schema

import (
	"testing"
	"time"
)

func TestDataColumnIndex(t *testing.T) {
	m := Meta{Mint: 0}
	for _, tt := range []struct {
		d      time.Duration
		expect int
	}{
		{d: time.Hour, expect: 0},
		{d: 8 * time.Hour, expect: 1},
		{d: 11 * time.Hour, expect: 1},
		{d: 18 * time.Hour, expect: 2},
		{d: 24 * time.Hour, expect: 2},
	} {
		t.Run("", func(ttt *testing.T) {
			if got, _ := ChunkColumnIndex(m, time.UnixMilli(m.Mint).Add(tt.d)); got != tt.expect {
				ttt.Fatalf("unexpected chunk column index %d, expected %d", got, tt.expect)
			}
		})
	}
}
