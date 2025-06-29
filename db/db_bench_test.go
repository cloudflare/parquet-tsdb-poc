// Copyright (c) 2025 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

package db_test

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"sync/atomic"
	"testing"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/util/teststorage"
	"github.com/thanos-io/objstore"
	"github.com/thanos-io/objstore/providers/filesystem"
)

type countingBucket struct {
	objstore.Bucket

	nGet       atomic.Int32
	nGetRange  atomic.Int32
	bsGetRange atomic.Int64
}

func (b *countingBucket) ResetCounters() {
	b.nGet.Store(0)
	b.nGetRange.Store(0)
	b.bsGetRange.Store(0)
}

func (b *countingBucket) Get(ctx context.Context, name string) (io.ReadCloser, error) {
	b.nGet.Add(1)
	return b.Bucket.Get(ctx, name)
}

func (b *countingBucket) GetRange(ctx context.Context, name string, off int64, length int64) (io.ReadCloser, error) {
	b.nGetRange.Add(1)
	b.bsGetRange.Add(length)
	return b.Bucket.GetRange(ctx, name, off, length)
}

func BenchmarkSelect(b *testing.B) {
	ctx := b.Context()

	type queryableCreate func(tb testing.TB, bkt objstore.Bucket, st *teststorage.TestStorage) storage.Queryable

	for k, qc := range map[string]queryableCreate{
		"parquet": func(tb testing.TB, bkt objstore.Bucket, st *teststorage.TestStorage) storage.Queryable {
			return storageToDBWithBkt(tb, st, bkt).Queryable()
		},
		"prometheus": func(_ testing.TB, _ objstore.Bucket, st *teststorage.TestStorage) storage.Queryable {
			return st
		},
	} {
		b.Run(k, func(b *testing.B) {
			b.Run("80k cardinality series", func(b *testing.B) {
				st := teststorage.New(b)
				b.Cleanup(func() { _ = st.Close() })
				bkt, err := filesystem.NewBucket(b.TempDir())
				if err != nil {
					b.Fatal("error creating bucket: ", err)
				}
				b.Cleanup(func() { _ = bkt.Close() })
				cbkt := &countingBucket{Bucket: bkt}

				app := st.Appender(b.Context())
				for i := range 10_000 {
					for _, sc := range []string{"200", "202", "300", "404", "400", "429", "500", "503"} {
						app.Append(0, labels.FromStrings("__name__", "foo", "idx", fmt.Sprintf("%d", i), "status_code", sc), 0, rand.Float64())
					}
				}
				if err := app.Commit(); err != nil {
					b.Fatal("error committing samples: ", err)
				}

				q, err := qc(b, cbkt, st).Querier(0, 120)
				if err != nil {
					b.Fatal("error building querier: ", err)
				}

				for _, bc := range []struct {
					name     string
					matchers []*labels.Matcher
				}{
					{
						name:     "ShreddedByStatusCode",
						matchers: []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "__name__", "foo"), labels.MustNewMatcher(labels.MatchRegexp, "status_code", "4..")},
					},
					{
						name:     "ShreddedByIdx",
						matchers: []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "__name__", "foo"), labels.MustNewMatcher(labels.MatchRegexp, "idx", ".*2")},
					},
					{
						name:     "AllSeries",
						matchers: []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "__name__", "foo")},
					},
					{
						name:     "SingleSeries",
						matchers: []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "idx", "500")},
					},
					{
						name:     "FirstAndLastSeries",
						matchers: []*labels.Matcher{labels.MustNewMatcher(labels.MatchRegexp, "idx", "0|9999")},
					},
				} {
					b.Run(bc.name, func(b *testing.B) {
						cbkt.ResetCounters()
						b.ReportAllocs()

						for b.Loop() {
							ss := q.Select(ctx, true, &storage.SelectHints{}, bc.matchers...)
							for ss.Next() {
								s := ss.At()
								it := s.Iterator(nil)
								for it.Next() != chunkenc.ValNone { //revive:disable-line:empty-block
								}
							}
							if err := ss.Err(); err != nil {
								b.Error(err)
							}
						}

						b.ReportMetric(float64(cbkt.nGet.Load())/float64(b.N), "get/op")
						b.ReportMetric(float64(cbkt.nGetRange.Load())/float64(b.N), "get_range/op")
						b.ReportMetric(float64(cbkt.bsGetRange.Load())/float64(b.N), "bytes_get_range/op")
					})
				}
			})
			b.Run("Realistic Series", func(b *testing.B) {
				b.Run(k, func(b *testing.B) {
					st := teststorage.New(b)
					b.Cleanup(func() { _ = st.Close() })

					bkt, err := filesystem.NewBucket(b.TempDir())
					if err != nil {
						b.Fatal("error creating bucket: ", err)
					}
					b.Cleanup(func() { _ = bkt.Close() })
					cbkt := &countingBucket{Bucket: bkt}

					app := st.Appender(b.Context())

					// 5 metrics × 100 instances × 5 regions × 10 zones × 20 services × 3 environments = 1,500,000 series
					metrics := 5
					instances := 100
					regions := 5
					zones := 10
					services := 20
					environments := 3

					seriesCount := 0
					for m := range metrics {
						for i := range instances {
							for r := range regions {
								for z := range zones {
									for s := range services {
										for e := range environments {
											lbls := labels.FromStrings(
												"__name__", fmt.Sprintf("test_metric_%d", m),
												"instance", fmt.Sprintf("instance-%d", i),
												"region", fmt.Sprintf("region-%d", r),
												"zone", fmt.Sprintf("zone-%d", z),
												"service", fmt.Sprintf("service-%d", s),
												"environment", fmt.Sprintf("environment-%d", e),
											)
											app.Append(0, lbls, 0, rand.Float64())
											seriesCount++
										}
									}
								}
							}
						}
					}
					if err := app.Commit(); err != nil {
						b.Fatal("error committing samples: ", err)
					}

					q, err := qc(b, cbkt, st).Querier(0, 120)
					if err != nil {
						b.Fatal("error building querier: ", err)
					}

					for _, bc := range []struct {
						name     string
						matchers []*labels.Matcher
					}{
						{
							name: "SingleMetricAllSeries",
							matchers: []*labels.Matcher{
								labels.MustNewMatcher(labels.MatchEqual, "__name__", "test_metric_1"),
							},
						},
						{
							name: "SingleMetricReducedSeries",
							matchers: []*labels.Matcher{
								labels.MustNewMatcher(labels.MatchEqual, "__name__", "test_metric_1"),
								labels.MustNewMatcher(labels.MatchEqual, "instance", "instance-1"),
							},
						},
						{
							name: "SingleMetricOneSeries",
							matchers: []*labels.Matcher{
								labels.MustNewMatcher(labels.MatchEqual, "__name__", "test_metric_1"),
								labels.MustNewMatcher(labels.MatchEqual, "instance", "instance-2"),
								labels.MustNewMatcher(labels.MatchEqual, "region", "region-1"),
								labels.MustNewMatcher(labels.MatchEqual, "zone", "zone-3"),
								labels.MustNewMatcher(labels.MatchEqual, "service", "service-10"),
								labels.MustNewMatcher(labels.MatchEqual, "environment", "environment-1"),
							},
						},
						{
							name: "SingleMetricSparseSeries",
							matchers: []*labels.Matcher{
								labels.MustNewMatcher(labels.MatchEqual, "__name__", "test_metric_1"),
								labels.MustNewMatcher(labels.MatchEqual, "service", "service-1"),
								labels.MustNewMatcher(labels.MatchEqual, "environment", "environment-0"),
							},
						},
						{
							name: "NonExistentSeries",
							matchers: []*labels.Matcher{
								labels.MustNewMatcher(labels.MatchEqual, "__name__", "test_metric_1"),
								labels.MustNewMatcher(labels.MatchEqual, "environment", "non-existent-environment"),
							},
						},
						{
							name: "MultipleMetricsRange",
							matchers: []*labels.Matcher{
								labels.MustNewMatcher(labels.MatchRegexp, "__name__", "test_metric_[1-5]"),
							},
						},
						{
							name: "MultipleMetricsSparse",
							matchers: []*labels.Matcher{
								labels.MustNewMatcher(labels.MatchRegexp, "__name__", "test_metric_(1|5|10|15|20)"),
							},
						},
						{
							name: "NegativeRegexSingleMetric",
							matchers: []*labels.Matcher{
								labels.MustNewMatcher(labels.MatchEqual, "__name__", "test_metric_1"),
								labels.MustNewMatcher(labels.MatchNotRegexp, "instance", "(instance-1.*|instance-2.*)"),
							},
						},
						{
							name: "NegativeRegexMultipleMetrics",
							matchers: []*labels.Matcher{
								labels.MustNewMatcher(labels.MatchRegexp, "__name__", "test_metric_[1-3]"),
								labels.MustNewMatcher(labels.MatchNotRegexp, "instance", "(instance-1.*|instance-2.*)"),
							},
						},
						{
							name: "ExpensiveRegexSingleMetric",
							matchers: []*labels.Matcher{
								labels.MustNewMatcher(labels.MatchEqual, "__name__", "test_metric_1"),
								labels.MustNewMatcher(labels.MatchRegexp, "instance", "(container-1|instance-2|container-3|instance-4|container-5)"),
							},
						},
						{
							name: "ExpensiveRegexMultipleMetrics",
							matchers: []*labels.Matcher{
								labels.MustNewMatcher(labels.MatchRegexp, "__name__", "test_metric_[1-3]"),
								labels.MustNewMatcher(labels.MatchRegexp, "instance", "(container-1|container-2|container-3|container-4|container-5)"),
							},
						},
					} {
						b.Run(bc.name, func(b *testing.B) {
							cbkt.ResetCounters()
							b.ResetTimer()

							for b.Loop() {
								ss := q.Select(ctx, true, &storage.SelectHints{}, bc.matchers...)
								for ss.Next() {
									s := ss.At()
									it := s.Iterator(nil)
									for it.Next() != chunkenc.ValNone { //revive:disable-line:empty-block
									}
								}
								if err := ss.Err(); err != nil {
									b.Error(err)
								}
							}

							b.ReportMetric(float64(cbkt.nGet.Load())/float64(b.N), "get/op")
							b.ReportMetric(float64(cbkt.nGetRange.Load())/float64(b.N), "get_range/op")
							b.ReportMetric(float64(cbkt.bsGetRange.Load())/float64(b.N), "bytes_get_range/op")
						})
					}

				})
			})
		})
	}
}
