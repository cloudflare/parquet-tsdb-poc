// Copyright (c) 2025 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

package search

import (
	"context"
	"errors"

	"github.com/prometheus/client_golang/prometheus"
)

const (
	scanRegex = "regex"
	scanEqual = "equal"

	methodSelect      = "select"
	methodLabelNames  = "label_names"
	methodLabelValues = "label_values"
)

type ctxMethodKey struct{}

var ctxMethodKeyVal = ctxMethodKey{}

func contextWithMethod(ctx context.Context, method string) context.Context {
	return context.WithValue(ctx, ctxMethodKeyVal, method)
}

func methodFromContext(ctx context.Context) string {
	return ctx.Value(ctxMethodKeyVal).(string)
}

var (
	pagesScanned = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "pages_scanned_total",
		Help: "Pages read during scans",
	}, []string{"column", "scan", "method"},
	)
	pagesRead = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "pages_read_total",
		Help: "Pages read during parquet operations",
	}, []string{"column", "method"},
	)
	pagesReadSize = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "pages_read_size_bytes_total",
		Help: "Cummulative size of pages in bytes that were read during parquet operations",
	}, []string{"column", "method"},
	)
	columnMaterialized = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "column_materialized_total",
		Help: "How often we had to materialize a column during queries",
	}, []string{"column", "method"},
	)
	rowsMaterialized = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "rows_materialized_total",
		Help: "How many rows we had to materialize for queries",
	}, []string{"column", "method"},
	)
)

func RegisterMetrics(reg prometheus.Registerer) error {
	return errors.Join(
		reg.Register(pagesScanned),
		reg.Register(pagesRead),
		reg.Register(pagesReadSize),
		reg.Register(columnMaterialized),
		reg.Register(rowsMaterialized),
	)
}
