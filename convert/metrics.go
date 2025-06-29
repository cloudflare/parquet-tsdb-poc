// Copyright (c) 2025 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

package convert

import (
	"errors"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	lastSuccessfulConvertTime = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "last_successful_convert_time_unix_seconds",
		Help: "The timestamp the last conversion ran successfully.",
	})
)

func RegisterMetrics(reg prometheus.Registerer) error {
	lastSuccessfulConvertTime.Set(0)

	return errors.Join(
		reg.Register(lastSuccessfulConvertTime),
	)
}
