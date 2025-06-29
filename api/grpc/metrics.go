// Copyright (c) 2025 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

package grpc

import (
	"errors"

	grpc_prom "github.com/grpc-ecosystem/go-grpc-middleware/providers/prometheus"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	ServerMetrics = grpc_prom.NewServerMetrics(
		grpc_prom.WithServerHandlingTimeHistogram(
			grpc_prom.WithHistogramOpts(&prometheus.HistogramOpts{
				Buckets: prometheus.ExponentialBucketsRange(0.1, 30, 20),
			}),
		),
	)
)

func RegisterMetrics(reg prometheus.Registerer) error {
	return errors.Join(
		reg.Register(ServerMetrics),
	)
}
