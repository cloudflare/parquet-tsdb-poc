// Copyright (c) 2025 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

package util

import "slices"

func SortUnique(ss []string) []string {
	slices.Sort(ss)

	return slices.Compact(ss)
}
