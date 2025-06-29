// Copyright (c) 2025 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

package http

import (
	"errors"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	httpRequestsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "requests_total",
		Help: "The total amount of http requests we answered",
	}, []string{"code", "method", "path"})
	httpRequestsDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "requests_seconds",
		Help:    "The histogram of time spent answering http requests",
		Buckets: prometheus.ExponentialBucketsRange(0.1, 30, 20),
	}, []string{"code", "method", "path"})
)

func RegisterMetrics(reg prometheus.Registerer) error {
	return errors.Join(
		reg.Register(httpRequestsTotal),
		reg.Register(httpRequestsDuration),
	)
}
