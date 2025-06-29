// Copyright (c) 2025 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

package db

import (
	"errors"

	"github.com/prometheus/client_golang/prometheus"
)

const (
	typeSelect      = "select"
	typeLabelValues = "label_values"
	typeLabelNames  = "label_names"

	// to avoid too high cardinality, we only measure on shard level
	whereShard = "shard"
)

var (
	queryableOperationsTotal = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "queryable_operations_total",
		Help: "The total amount of query operations we evaluated",
	}, []string{"type", "where"})
	queryableOperationsDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "queryable_operations_seconds",
		Help:    "Histogram of durations for queryable operations",
		Buckets: prometheus.ExponentialBucketsRange(0.1, 30, 20),
	}, []string{"type", "where"})
)

func RegisterMetrics(reg prometheus.Registerer) error {
	for _, t := range []string{typeSelect, typeLabelNames, typeLabelValues} {
		for _, w := range []string{whereShard} {
			queryableOperationsTotal.WithLabelValues(t, w).Set(0)
			queryableOperationsDuration.WithLabelValues(t, w).Observe(0)
		}
	}
	return errors.Join(
		reg.Register(queryableOperationsTotal),
		reg.Register(queryableOperationsDuration),
	)
}
