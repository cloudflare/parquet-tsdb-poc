// Copyright (c) 2025 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

package warnings

import "errors"

var (
	ErrorTruncatedResponse                            = errors.New("results truncated due to limit")
	ErrorDroppedSeriesAfterExternalLabelMangling      = errors.New("dropped series after external label mangling")
	ErrorDroppedLabelValuesAfterExternalLabelMangling = errors.New("dropped label values after external label mangling")
)
