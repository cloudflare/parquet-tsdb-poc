// Copyright (c) 2025 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

package locate

import (
	"errors"

	"github.com/prometheus/client_golang/prometheus"
)

const (
	whatSyncer         = "syncer"
	whatDiscoverer     = "discoverer"
	whatTSDBDiscoverer = "tsdb_discoverer"
)

var (
	bucketRequests = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "bucket_requests_total",
		Help: "Total amount of requests to object storage",
	})
	syncMinTime = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "sync_min_time_unix_seconds",
		Help: "The minimum timestamp that syncer knows",
	}, []string{"what"})
	syncMaxTime = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "sync_max_time_unix_seconds",
		Help: "The minimum timestamp that syncer knows",
	}, []string{"what"})
	syncLastSuccessfulTime = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "sync_last_successful_update_time_unix_seconds",
		Help: "The timestamp we last synced successfully",
	}, []string{"what"})
	syncCorruptedLabelFile = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "sync_corrupted_label_parquet_files_total",
		Help: "The amount of corrupted label parquet files we encountered",
	})
)

func RegisterMetrics(reg prometheus.Registerer) error {
	bucketRequests.Add(0)
	for _, w := range []string{whatSyncer, whatDiscoverer, whatTSDBDiscoverer} {
		syncMinTime.WithLabelValues(w).Set(0)
		syncMaxTime.WithLabelValues(w).Set(0)
		syncLastSuccessfulTime.WithLabelValues(w).Set(0)
	}

	return errors.Join(
		reg.Register(bucketRequests),
		reg.Register(syncMinTime),
		reg.Register(syncMaxTime),
		reg.Register(syncLastSuccessfulTime),
		reg.Register(syncCorruptedLabelFile),
	)
}
