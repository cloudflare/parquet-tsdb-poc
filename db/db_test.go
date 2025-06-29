// Copyright (c) 2025 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

package db_test

import (
	"context"
	"errors"
	"fmt"
	"math"
	"slices"
	"sort"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/promqltest"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/teststorage"
	"github.com/prometheus/prometheus/util/testutil"
	"github.com/thanos-io/objstore"
	"github.com/thanos-io/objstore/providers/filesystem"

	"github.com/cloudflare/parquet-tsdb-poc/convert"
	"github.com/cloudflare/parquet-tsdb-poc/db"
	"github.com/cloudflare/parquet-tsdb-poc/internal/warnings"
	"github.com/cloudflare/parquet-tsdb-poc/locate"
)

var (
	opts = promql.EngineOpts{
		Timeout:                  1 * time.Hour,
		MaxSamples:               1e10,
		EnableNegativeOffset:     true,
		EnableAtModifier:         true,
		NoStepSubqueryIntervalFn: func(_ int64) int64 { return 30 * time.Second.Milliseconds() },
		LookbackDelta:            5 * time.Minute,
	}
)

func TestPromQLAcceptance(t *testing.T) {
	if testing.Short() {
		// There are still some tests failing because we dont support regex matcher on
		// the name label, or matching without a name label in the first place.
		t.Skip("Skipping, because 'short' flag was set")
	}

	engine := promql.NewEngine(opts)
	t.Cleanup(func() { engine.Close() })

	st := &testHelper{
		skipTests: []string{
			"testdata/name_label_dropping.test", // feature unsupported in promql-engine
		},
		TBRun: t,
	}

	promqltest.RunBuiltinTestsWithStorage(st, engine, func(tt testutil.T) storage.Storage {
		return &acceptanceTestStorage{tb: t, st: teststorage.New(tt)}
	})
}

type testHelper struct {
	skipTests []string
	promqltest.TBRun
}

func (s *testHelper) Run(name string, t func(*testing.T)) bool {
	if slices.Contains(s.skipTests, name) {
		return true
	}

	return s.TBRun.Run(name+"-concurrent", func(tt *testing.T) {
		tt.Parallel()
		s.TBRun.Run(name, t)
	})
}

type acceptanceTestStorage struct {
	tb testing.TB
	st *teststorage.TestStorage
}

func (st *acceptanceTestStorage) Appender(ctx context.Context) storage.Appender {
	return st.st.Appender(ctx)
}

func (st *acceptanceTestStorage) ChunkQuerier(int64, int64) (storage.ChunkQuerier, error) {
	return nil, errors.New("unimplemented")
}

func (st *acceptanceTestStorage) Querier(from, to int64) (storage.Querier, error) {
	if st.st.Head().NumSeries() == 0 {
		// parquet-go panics when writing an empty parquet file
		return st.st.Querier(from, to)
	}
	return storageToDB(st.tb, st.st).Queryable().Querier(from, to)
}

func (st *acceptanceTestStorage) Close() error {
	return st.st.Close()
}

func (st *acceptanceTestStorage) StartTime() (int64, error) {
	return st.st.StartTime()
}

func TestSelect(t *testing.T) {
	ts := promqltest.LoadedStorage(t, `load 30s
    foo{bar="baz"} 1
    abc{def="ghi"} 1
    jkl{ext="doesntmatter1"} 1
    jkl{ext="doesntmatter2"} 1
    `)
	t.Cleanup(func() { ts.Close() })

	database := storageToDB(t, ts, db.ExternalLabels(labels.FromStrings("ext", "test", "rep", "1")))
	qry, err := database.Queryable(db.DropReplicaLabels("rep")).Querier(math.MinInt64, math.MaxInt64)
	if err != nil {
		t.Fatalf("unable to construct querier: %s", err)
	}
	for _, tc := range []struct {
		matchers []*labels.Matcher
		want     []labels.Labels
		warns    []error
	}{
		{
			want: []labels.Labels{
				labels.FromStrings("__name__", "abc", "def", "ghi", "ext", "test"),
				labels.FromStrings("__name__", "foo", "bar", "baz", "ext", "test"),
				labels.FromStrings("__name__", "jkl", "ext", "test"),
			},
			warns: []error{
				warnings.ErrorDroppedSeriesAfterExternalLabelMangling,
			},
		},
		{
			matchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchNotEqual, "ext", "test"),
			},
		},
		{
			matchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchRegexp, "ext", "doesntmatter.*"),
			},
		},
		{
			matchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, "__name__", "foo"),
			},
			want: []labels.Labels{
				labels.FromStrings("__name__", "foo", "bar", "baz", "ext", "test"),
			},
		},
	} {
		t.Run("", func(tt *testing.T) {
			ss := qry.Select(tt.Context(), false, &storage.SelectHints{}, tc.matchers...)

			series := make([]labels.Labels, 0)
			for ss.Next() {
				series = append(series, ss.At().Labels())
			}
			if err := ss.Err(); err != nil {
				tt.Fatalf("unable to query label names: %s", err)
			}
			if !slices.EqualFunc(tc.want, series, func(l, r labels.Labels) bool { return l.Hash() == r.Hash() }) {
				tt.Fatalf("expected %q, got %q", tc.want, series)
			}
			if errs := ss.Warnings().AsErrors(); !slices.Equal(tc.warns, errs) {
				tt.Errorf("expected %q, got %q", tc.warns, errs)
			}
		})
	}
}

func TestLabelNames(t *testing.T) {
	ts := promqltest.LoadedStorage(t, `load 30s
    foo{bar="baz"} 1
    abc{def="ghi"} 1
    `)
	t.Cleanup(func() { ts.Close() })

	database := storageToDB(t, ts, db.ExternalLabels(labels.FromStrings("ext", "test", "rep", "1")))
	qry, err := database.Queryable(db.DropReplicaLabels("rep")).Querier(math.MinInt64, math.MaxInt64)
	if err != nil {
		t.Fatalf("unable to construct querier: %s", err)
	}

	for _, tc := range []struct {
		matchers []*labels.Matcher
		want     []string
	}{
		{
			want: []string{"__name__", "bar", "def", "ext"},
		},
		{
			matchers: []*labels.Matcher{labels.MustNewMatcher(labels.MatchNotEqual, "ext", "test")},
		},
		{
			matchers: []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "ext", "test")},
			want:     []string{"__name__", "bar", "def", "ext"},
		},
		{
			matchers: []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "fizz", "buzz")},
		},
		{
			matchers: []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "__name__", "abc")},
			want:     []string{"__name__", "def", "ext"},
		},
	} {
		t.Run("", func(tt *testing.T) {
			res, _, err := qry.LabelNames(tt.Context(), &storage.LabelHints{}, tc.matchers...)
			if err != nil {
				tt.Fatalf("unable to query label names: %s", err)
			}
			if !slices.Equal(tc.want, res) {
				tt.Fatalf("expected %q, got %q", tc.want, res)
			}
		})
	}
}

func TestLabelValues(t *testing.T) {
	ts := promqltest.LoadedStorage(t, `load 30s
    foo{bar="baz"} 1
    abc{ext="internal"} 1
    `)
	t.Cleanup(func() { ts.Close() })

	database := storageToDB(t, ts, db.ExternalLabels(labels.FromStrings("ext", "test", "rep", "1")))
	qry, err := database.Queryable(db.DropReplicaLabels("rep")).Querier(math.MinInt64, math.MaxInt64)
	if err != nil {
		t.Fatalf("unable to construct querier: %s", err)
	}

	for _, tc := range []struct {
		name     string
		matchers []*labels.Matcher
		want     []string
		warns    []error
	}{
		{
			name: "bar",
			want: []string{"baz"},
		},
		{
			name:     "ext",
			matchers: []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "bar", "baz")},
			want:     []string{"test"},
		},
		{
			name:     "ext",
			matchers: []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "fizz", "buzz")},
		},
		{
			name:     "ext",
			matchers: []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "__name__", "abc")},
			want:     []string{"test"},
			warns:    []error{warnings.ErrorDroppedLabelValuesAfterExternalLabelMangling},
		},
		{
			name:     "bar",
			matchers: []*labels.Matcher{labels.MustNewMatcher(labels.MatchNotEqual, "ext", "test")},
		},
		{
			name:     "bar",
			matchers: []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "ext", "test")},
			want:     []string{"baz"},
		},
		{
			name:  "ext",
			want:  []string{"test"},
			warns: []error{warnings.ErrorDroppedLabelValuesAfterExternalLabelMangling},
		},
		{
			name: "rep",
		},
	} {
		t.Run("", func(tt *testing.T) {
			res, warns, err := qry.LabelValues(tt.Context(), tc.name, &storage.LabelHints{}, tc.matchers...)
			if err != nil {
				tt.Fatalf("unable to query label names: %s", err)
			}
			if !slices.Equal(tc.want, res) {
				tt.Fatalf("expected %q, got %q", tc.want, res)
			}
			if errs := warns.AsErrors(); !slices.Equal(tc.warns, errs) {
				tt.Errorf("expected %q, got %q", tc.warns, errs)
			}
		})
	}

	t.Run("No matchers, replica label", func(tt *testing.T) {
		res, _, err := qry.LabelValues(tt.Context(), "rep", &storage.LabelHints{})
		if err != nil {
			tt.Fatalf("unable to query label values: %s", err)
		}
		if want := []string{}; !slices.Equal(want, res) {
			tt.Fatalf("expected %q, got %q", want, res)
		}
	})
}

func TestInstantQuery(t *testing.T) {
	defaultQueryTime := time.Unix(50, 0)
	engine := promql.NewEngine(opts)
	t.Cleanup(func() { engine.Close() })

	cases := []struct {
		load      string
		name      string
		queries   []string
		queryTime time.Time
	}{
		{
			name: "vector selector with empty matcher on nonexistent column",
			load: `load 10s
			    metric{pod="nginx-1", a=""} 1+2x40`,
			queries: []string{`metric{foo=""}`},
		},
		{
			name: "vector selector with empty labels",
			load: `load 10s
			    metric{pod="nginx-1", a=""} 1+2x40`,
			queries: []string{`metric`},
		},
		{
			name: "vector selector with differnt labelnames",
			load: `load 10s
			    metric{pod="nginx-1", a="foo"} 1+1x40
			    metric{pod="nginx-1", b="bar"} 1+1x40`,
			queries: []string{`metric`},
		},
		{
			name: "float histogram",
			load: `load 5m
				http_requests_histogram{job="api-server", instance="3", group="canary"} {{schema:2 count:4 sum:10 buckets:[1 0 0 0 1 0 0 1 1]}}`,
			queries: []string{
				`max(http_requests_histogram)`,
				`min(http_requests_histogram)`,
			},
		},
		{
			name: "vector selector with many labels",
			load: `load 10s
			    metric{pod="nginx-1", a="b", c="d", e="f"} 1+1x40`,
			queries: []string{`metric`},
		},
		{
			name: "vector selector with many not equal comparison",
			load: `load 10s
			    metric{pod="nginx-1", a="b", c="d", e="f"} 1+1x40`,
			queries: []string{`metric{a!="a",c!="c"}`},
		},
		{
			name: "vector selector with regex selector",
			load: `load 10s
			    metric{pod="nginx-1", a="foo"} 1+1x40
			    metric{pod="nginx-1", a="bar"} 1+1x40`,
			queries: []string{`metric{a=~"f.*"}`},
		},
		{
			name: "vector selector with not regex selector",
			load: `load 10s
                metric{pod="nginx-1", a="foo"} 1+1x40
                metric{pod="nginx-1", a="bar"} 1+1x40`,
			queries: []string{`metric{a!~"f.*"}`},
		},
		{
			name: "vector selector with not equal selector on unknown column",
			load: `load 10s
                metric{pod="nginx-1"} 1+1x40
                metric{pod="nginx-1"} 1+1x40`,
			queries: []string{`metric{a!~"f.*"}`},
		},
		{
			name: "sum with by grouping",
			load: `load 10s
                metric{pod="nginx-1", a="foo"} 1+1x40
                metric{pod="nginx-2", a="bar"} 1+1x40
                metric{pod="nginx-1", a="bar"} 1+1x40`,
			queries: []string{`sum by (a) (metric)`},
		},
		{
			name: "sum with without grouping",
			load: `load 10s
                metric{pod="nginx-1", a="foo"} 1+1x40
                metric{pod="nginx-2", a="bar"} 1+1x40
                metric{pod="nginx-1", a="bar"} 1+1x40`,
			queries: []string{`sum without (a) (metric)`},
		},
		{
			name: "sum_over_time with subquery",
			load: `load 10s
			    metric{pod="nginx-1", series="1"} 1+1x40
			    metric{pod="nginx-2", series="2"} 2+2x50
			    metric{pod="nginx-4", series="3"} 5+2x50
			    metric{pod="nginx-5", series="1"} 8+4x50
			    metric{pod="nginx-6", series="2"} 2+3x50`,
			queryTime: time.Unix(600, 0),
			queries:   []string{`sum_over_time(sum by (series) (x)[5m:1m])`},
		},
		{
			name: "",
			load: `load 10s
			    data{test="ten",point="a"} 2
				data{test="ten",point="b"} 8
				data{test="ten",point="c"} 1e+100
				data{test="ten",point="d"} -1e100
				data{test="pos_inf",group="1",point="a"} Inf
				data{test="pos_inf",group="1",point="b"} 2
				data{test="pos_inf",group="2",point="a"} 2
				data{test="pos_inf",group="2",point="b"} Inf
				data{test="neg_inf",group="1",point="a"} -Inf
				data{test="neg_inf",group="1",point="b"} 2
				data{test="neg_inf",group="2",point="a"} 2
				data{test="neg_inf",group="2",point="b"} -Inf
				data{test="inf_inf",point="a"} Inf
				data{test="inf_inf",point="b"} -Inf
				data{test="nan",group="1",point="a"} NaN
				data{test="nan",group="1",point="b"} 2
				data{test="nan",group="2",point="a"} 2
				data{test="nan",group="2",point="b"} NaN`,
			queryTime: time.Unix(60, 0),
			queries:   []string{`sum(data{test="ten"})`},
		},
		{
			name: "",
			load: `load 5m
				http_requests{job="api-server", instance="0", group="production"} 0+10x10
				http_requests{job="api-server", instance="1", group="production"} 0+20x10
				http_requests{job="api-server", instance="0", group="canary"}   0+30x10
				http_requests{job="api-server", instance="1", group="canary"}   0+40x10
				http_requests{job="app-server", instance="0", group="production"} 0+50x10
				http_requests{job="app-server", instance="1", group="production"} 0+60x10
				http_requests{job="app-server", instance="0", group="canary"}   0+70x10
				http_requests{job="app-server", instance="1", group="canary"}   0+80x10`,
			queryTime: time.Unix(3000, 0),
			queries: []string{
				`SUM BY (group) (http_requests{job="api-server"})`,
				`SUM BY (group) (((http_requests{job="api-server"})))`,
				`sum by (group) (http_requests{job="api-server"})`,
				`avg by (group) (http_requests{job="api-server"})`,
				`count by (group) (http_requests{job="api-server"})`,
				`sum without (instance) (http_requests{job="api-server"})`,
				`sum by () (http_requests{job="api-server"})`,
				`sum(http_requests{job="api-server"})`,
				`sum without () (http_requests{job="api-server",group="production"})`,
				`sum without (instance) (http_requests{job="api-server"} or foo)`,
				`sum(http_requests) by (job) + min(http_requests) by (job) + max(http_requests) by (job) + avg(http_requests) by (job)`,
				`sum by (group) (http_requests{job="api-server"})`,
				`sum(sum by (group) (http_requests{job="api-server"})) by (job)`,
				`SUM(http_requests)`,
				`SUM(http_requests{instance="0"}) BY(job)`,
				`SUM(http_requests) BY (job)`,
				`SUM(http_requests) BY (job, nonexistent)`,
				`COUNT(http_requests) BY (job)`,
				`SUM(http_requests) BY (job, group)`,
				`AVG(http_requests) BY (job)`,
				`MIN(http_requests) BY (job)`,
				`MAX(http_requests) BY (job)`,
				`abs(-1 * http_requests{group="production",job="api-server"})`,
				`floor(0.004 * http_requests{group="production",job="api-server"})`,
				`ceil(0.004 * http_requests{group="production",job="api-server"})`,
				`round(0.004 * http_requests{group="production",job="api-server"})`,
				`round(-1 * (0.004 * http_requests{group="production",job="api-server"}))`,
				`round(0.005 * http_requests{group="production",job="api-server"})`,
				`round(-1 * (0.005 * http_requests{group="production",job="api-server"}))`,
				`round(1 + 0.005 * http_requests{group="production",job="api-server"})`,
				`round(-1 * (1 + 0.005 * http_requests{group="production",job="api-server"}))`,
				`round(0.0005 * http_requests{group="production",job="api-server"}, 0.1)`,
				`round(2.1 + 0.0005 * http_requests{group="production",job="api-server"}, 0.1)`,
				`round(5.2 + 0.0005 * http_requests{group="production",job="api-server"}, 0.1)`,
				`round(-1 * (5.2 + 0.0005 * http_requests{group="production",job="api-server"}), 0.1)`,
				`round(0.025 * http_requests{group="production",job="api-server"}, 5)`,
				`round(0.045 * http_requests{group="production",job="api-server"}, 5)`,
				`stddev(http_requests)`,
				`stddev by (instance)(http_requests)`,
				`stdvar(http_requests)`,
				`stdvar by (instance)(http_requests)`,
			},
		},
		{
			name: "",
			load: `load 5m
				http_requests{job="api-server", instance="0", group="production"} 0+1.33x10
				http_requests{job="api-server", instance="1", group="production"} 0+1.33x10
				http_requests{job="api-server", instance="0", group="canary"} 0+1.33x10`,
			queryTime: time.Unix(3000, 0),
			queries: []string{
				`stddev(http_requests)`,
				`stdvar(http_requests)`,
			},
		},
		{
			name: "",
			load: `load 5m
				label_grouping_test{a="aa", b="bb"} 0+10x10
				label_grouping_test{a="a", b="abb"} 0+20x10`,
			queryTime: time.Unix(3000, 0),
			queries: []string{
				`sum(label_grouping_test) by (a, b)`,
			},
		},
		{
			name: "",
			load: `load 5m
				label_grouping_test{a="aa", b="bb"} 0+10x10
				label_grouping_test{a="a", b="abb"} 0+20x10`,
			queryTime: time.Unix(3000, 0),
			queries: []string{
				`sum(label_grouping_test) by (a, b)`,
			},
		},
		{
			name: "",
			load: `load 5m
				http_requests{job="api-server", instance="0", group="production"}	1
				http_requests{job="api-server", instance="1", group="production"}	2
				http_requests{job="api-server", instance="0", group="canary"}		NaN
				http_requests{job="api-server", instance="1", group="canary"}		3
				http_requests{job="api-server", instance="2", group="canary"}		4
				http_requests_histogram{job="api-server", instance="3", group="canary"} {{schema:2 count:4 sum:10 buckets:[1 0 0 0 1 0 0 1 1]}}`,
			queryTime: time.Unix(0, 0),
			queries: []string{
				`max(http_requests)`,
				`min(http_requests)`,
				`max by (group) (http_requests)`,
				`min by (group) (http_requests)`,
			},
		},
		{
			name: "",
			load: `load 5m
				http_requests{job="api-server", instance="0", group="production"}	0+10x10
				http_requests{job="api-server", instance="1", group="production"}	0+20x10
				http_requests{job="api-server", instance="2", group="production"}	NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN
				http_requests{job="api-server", instance="0", group="canary"}		0+30x10
				http_requests{job="api-server", instance="1", group="canary"}		0+40x10
				http_requests{job="app-server", instance="0", group="production"}	0+50x10
				http_requests{job="app-server", instance="1", group="production"}	0+60x10
				http_requests{job="app-server", instance="0", group="canary"}		0+70x10
				http_requests{job="app-server", instance="1", group="canary"}		0+80x10
				foo 3+0x10`,
			queryTime: time.Unix(3000, 0),
			queries: []string{
				`topk(3, http_requests)`,
				`topk((3), (http_requests))`,
				`topk(5, http_requests{group="canary",job="app-server"})`,
				`bottomk(3, http_requests)`,
				`bottomk(5, http_requests{group="canary",job="app-server"})`,
				`topk by (group) (1, http_requests)`,
				`bottomk by (group) (2, http_requests)`,
				`bottomk by (group) (2, http_requests{group="production"})`,
				`topk(3, http_requests{job="api-server",group="production"})`,
				`bottomk(3, http_requests{job="api-server",group="production"})`,
				`bottomk(9999999999, http_requests{job="app-server",group="canary"})`,
				`topk(9999999999, http_requests{job="api-server",group="production"})`,
				`topk(scalar(foo), http_requests)`,
			},
		},
		{
			name: "",
			load: `load 5m
				version{job="api-server", instance="0", group="production"}	6
				version{job="api-server", instance="1", group="production"}	6
				version{job="api-server", instance="2", group="production"}	6
				version{job="api-server", instance="0", group="canary"}		8
				version{job="api-server", instance="1", group="canary"}		8
				version{job="app-server", instance="0", group="production"}	6
				version{job="app-server", instance="1", group="production"}	6
				version{job="app-server", instance="0", group="canary"}		7
				version{job="app-server", instance="1", group="canary"}		7
				version{job="app-server", instance="2", group="canary"}		{{schema:0 sum:10 count:20 z_bucket_w:0.001 z_bucket:2 buckets:[1 2] n_buckets:[1 2]}}
				version{job="app-server", instance="3", group="canary"}		{{schema:0 sum:10 count:20 z_bucket_w:0.001 z_bucket:2 buckets:[1 2] n_buckets:[1 2]}}`,
			queryTime: time.Unix(60, 0),
			queries: []string{
				`count_values("version", version)`,
				`count_values(((("version"))), version)`,
				`count_values without (instance)("version", version)`,
				`count_values without (instance)("job", version)`,
				`count_values by (job, group)("job", version)`,
			},
		},
		{
			name: "",
			load: `load 10s
				data{test="two samples",point="a"} 0
				data{test="two samples",point="b"} 1
				data{test="three samples",point="a"} 0
				data{test="three samples",point="b"} 1
				data{test="three samples",point="c"} 2
				data{test="uneven samples",point="a"} 0
				data{test="uneven samples",point="b"} 1
				data{test="uneven samples",point="c"} 4
				data_histogram{test="histogram sample", point="c"} {{schema:2 count:4 sum:10 buckets:[1 0 0 0 1 0 0 1 1]}}
				foo .8`,
			queryTime: time.Unix(60, 0),
			queries: []string{
				`quantile without(point)(0.8, data)`,
				`quantile(0.8, data_histogram)`,
				`quantile without(point)(scalar(foo), data)`,
				`quantile without(point)((scalar(foo)), data)`,
				`quantile without(point)(NaN, data)`,
			},
		},
		{
			name: "",
			load: `load 10s
				data{test="two samples",point="a"} 0
				data{test="two samples",point="b"} 1
				data{test="three samples",point="a"} 0
				data{test="three samples",point="b"} 1
				data{test="three samples",point="c"} 2
				data{test="uneven samples",point="a"} 0
				data{test="uneven samples",point="b"} 1
				data{test="uneven samples",point="c"} 4
				data{test="histogram sample",point="c"} {{schema:0 sum:0 count:0}}
				foo .8`,
			queryTime: time.Unix(60, 0),
			queries: []string{
				`group without(point)(data)`,
				`group(foo)`,
			},
		},
		{
			name: "",
			load: `load 10s
				data{test="ten",point="a"} 8
				data{test="ten",point="b"} 10
				data{test="ten",point="c"} 12
				data{test="inf",point="a"} 0
				data{test="inf",point="b"} Inf
				data{test="inf",point="d"} Inf
				data{test="inf",point="c"} 0
				data{test="-inf",point="a"} -Inf
				data{test="-inf",point="b"} -Inf
				data{test="-inf",point="c"} 0
				data{test="inf2",point="a"} Inf
				data{test="inf2",point="b"} 0
				data{test="inf2",point="c"} Inf
				data{test="-inf2",point="a"} -Inf
				data{test="-inf2",point="b"} 0
				data{test="-inf2",point="c"} -Inf
				data{test="inf3",point="b"} Inf
				data{test="inf3",point="d"} Inf
				data{test="inf3",point="c"} Inf
				data{test="inf3",point="d"} -Inf
				data{test="-inf3",point="b"} -Inf
				data{test="-inf3",point="d"} -Inf
				data{test="-inf3",point="c"} -Inf
				data{test="-inf3",point="c"} Inf
				data{test="nan",point="a"} -Inf
				data{test="nan",point="b"} 0
				data{test="nan",point="c"} Inf
				data{test="big",point="a"} 9.988465674311579e+307
				data{test="big",point="b"} 9.988465674311579e+307
				data{test="big",point="c"} 9.988465674311579e+307
				data{test="big",point="d"} 9.988465674311579e+307
				data{test="-big",point="a"} -9.988465674311579e+307
				data{test="-big",point="b"} -9.988465674311579e+307
				data{test="-big",point="c"} -9.988465674311579e+307
				data{test="-big",point="d"} -9.988465674311579e+307
				data{test="bigzero",point="a"} -9.988465674311579e+307
				data{test="bigzero",point="b"} -9.988465674311579e+307
				data{test="bigzero",point="c"} 9.988465674311579e+307
				data{test="bigzero",point="d"} 9.988465674311579e+307`,
			queryTime: time.Unix(60, 0),
			queries: []string{
				`avg(data{test="ten"})`,
				`avg(data{test="inf"})`,
				`avg(data{test="inf2"})`,
				`avg(data{test="inf3"})`,
				`avg(data{test="-inf"})`,
				`avg(data{test="-inf2"})`,
				`avg(data{test="-inf3"})`,
				`avg(data{test="nan"})`,
				`avg(data{test="big"})`,
				`avg(data{test="-big"})`,
				`avg(data{test="bigzero"})`,
			},
		},
		{
			name: "",
			load: `load 10s
				data{test="ten",point="a"} 2
				data{test="ten",point="b"} 8
				data{test="ten",point="c"} 1e+100
				data{test="ten",point="d"} -1e100
				data{test="pos_inf",group="1",point="a"} Inf
				data{test="pos_inf",group="1",point="b"} 2
				data{test="pos_inf",group="2",point="a"} 2
				data{test="pos_inf",group="2",point="b"} Inf
				data{test="neg_inf",group="1",point="a"} -Inf
				data{test="neg_inf",group="1",point="b"} 2
				data{test="neg_inf",group="2",point="a"} 2
				data{test="neg_inf",group="2",point="b"} -Inf
				data{test="inf_inf",point="a"} Inf
				data{test="inf_inf",point="b"} -Inf
				data{test="nan",group="1",point="a"} NaN
				data{test="nan",group="1",point="b"} 2
				data{test="nan",group="2",point="a"} 2
				data{test="nan",group="2",point="b"} NaN`,
			queryTime: time.Unix(60, 0),
			queries: []string{
				`sum(data{test="ten"})`,
				`avg(data{test="ten"})`,
				`sum by (group) (data{test="pos_inf"})`,
				`avg by (group) (data{test="pos_inf"})`,
				`sum by (group) (data{test="neg_inf"})`,
				`avg by (group) (data{test="neg_inf"})`,
				`sum(data{test="inf_inf"})`,
				`avg(data{test="inf_inf"})`,
				`sum by (group) (data{test="nan"})`,
				`avg by (group) (data{test="nan"})`,
			},
		},
		{
			name: "",
			load: `load 5m
				series{label="a"} 1
				series{label="b"} 2
				series{label="c"} NaN`,
			queryTime: time.Unix(0, 0),
			queries: []string{
				`stddev(series)`,
				`stdvar(series)`,
				`stddev by (label) (series)`,
				`stdvar by (label) (series)`,
			},
		},
		{
			name: "",
			load: `load 5m
				series{label="a"} NaN
				series{label="b"} 1
				series{label="c"} 2`,
			queryTime: time.Unix(0, 0),
			queries: []string{
				`stddev(series)`,
				`stdvar(series)`,
				`stddev by (label) (series)`,
				`stdvar by (label) (series)`,
			},
		},
		{
			name: "",
			load: `load 5m
				series NaN`,
			queryTime: time.Unix(0, 0),
			queries: []string{
				`stddev(series)`,
				`stdvar(series)`,
			},
		},
		{
			name: "",
			load: `load 5m
				series{label="a"} 1
				series{label="b"} 2
				series{label="c"} inf`,
			queryTime: time.Unix(0, 0),
			queries: []string{
				`stddev(series)`,
				`stdvar(series)`,
				`stddev by (label) (series)`,
				`stdvar by (label) (series)`,
			},
		},
		{
			name: "",
			load: `load 5m
				series{label="a"} inf
				series{label="b"} 1
				series{label="c"} 2`,
			queryTime: time.Unix(0, 0),
			queries: []string{
				`stddev(series)`,
				`stdvar(series)`,
				`stddev by (label) (series)`,
				`stdvar by (label) (series)`,
			},
		},
		{
			name: "",
			load: `load 5m
				series inf`,
			queryTime: time.Unix(0, 0),
			queries: []string{
				`stddev(series)`,
				`stdvar(series)`,
			},
		},
		{
			name: "",
			load: `load 1s
				node_namespace_pod:kube_pod_info:{namespace="observability",node="gke-search-infra-custom-96-253440-fli-d135b119-jx00",pod="node-exporter-l454v"} 1
				node_cpu_seconds_total{cpu="10",endpoint="https",instance="10.253.57.87:9100",job="node-exporter",mode="idle",namespace="observability",pod="node-exporter-l454v",service="node-exporter"} 449
				node_cpu_seconds_total{cpu="35",endpoint="https",instance="10.253.57.87:9100",job="node-exporter",mode="idle",namespace="observability",pod="node-exporter-l454v",service="node-exporter"} 449
				node_cpu_seconds_total{cpu="89",endpoint="https",instance="10.253.57.87:9100",job="node-exporter",mode="idle",namespace="observability",pod="node-exporter-l454v",service="node-exporter"} 449`,
			queryTime: time.Unix(4, 0),
			queries: []string{
				`count by(namespace, pod, cpu) (node_cpu_seconds_total{cpu=~".*",job="node-exporter",mode="idle",namespace="observability",pod="node-exporter-l454v"}) * on(namespace, pod) group_left(node) node_namespace_pod:kube_pod_info:{namespace="observability",pod="node-exporter-l454v"}`,
			},
		},
		{
			name: "duplicate labelset in promql output",
			load: `load 5m
				testmetric1{src="a",dst="b"} 0
				testmetric2{src="a",dst="b"} 1`,
			queryTime: time.Unix(0, 0),
			queries: []string{
				`ceil({__name__=~'testmetric1|testmetric2'})`,
			},
		},
		{
			name: "operators",
			load: `load 5m
				http_requests_total{job="api-server", instance="0", group="production"}	0+10x10
				http_requests_total{job="api-server", instance="1", group="production"}	0+20x10
				http_requests_total{job="api-server", instance="0", group="canary"}	0+30x10
				http_requests_total{job="api-server", instance="1", group="canary"}	0+40x10
				http_requests_total{job="app-server", instance="0", group="production"}	0+50x10
				http_requests_total{job="app-server", instance="1", group="production"}	0+60x10
				http_requests_total{job="app-server", instance="0", group="canary"}	0+70x10
				http_requests_total{job="app-server", instance="1", group="canary"}	0+80x10
				http_requests_histogram{job="app-server", instance="1", group="production"} {{schema:1 sum:15 count:10 buckets:[3 2 5 7 9]}}x11

				load 5m
					vector_matching_a{l="x"} 0+1x100
					vector_matching_a{l="y"} 0+2x50
					vector_matching_b{l="x"} 0+4x25`,
			queryTime: time.Unix(3000, 0),
			queries: []string{
				`SUM(http_requests_total) BY (job) - COUNT(http_requests_total) BY (job)`,
				`2 - SUM(http_requests_total) BY (job)`,
				`-http_requests_total{job="api-server",instance="0",group="production"}`,
				`+http_requests_total{job="api-server",instance="0",group="production"}`,
				`- - - SUM(http_requests_total) BY (job)`,
				`- - - 1`,
				`-2^---1*3`,
				`2/-2^---1*3+2`,
				`-10^3 * - SUM(http_requests_total) BY (job) ^ -1`,
				`1000 / SUM(http_requests_total) BY (job)`,
				`SUM(http_requests_total) BY (job) - 2`,
				`SUM(http_requests_total) BY (job) % 3`,
				`SUM(http_requests_total) BY (job) % 0.3`,
				`SUM(http_requests_total) BY (job) ^ 2`,
				`SUM(http_requests_total) BY (job) % 3 ^ 2`,
				`SUM(http_requests_total) BY (job) % 2 ^ (3 ^ 2)`,
				`SUM(http_requests_total) BY (job) % 2 ^ 3 ^ 2`,
				`SUM(http_requests_total) BY (job) % 2 ^ 3 ^ 2 ^ 2`,
				`COUNT(http_requests_total) BY (job) ^ COUNT(http_requests_total) BY (job)`,
				`SUM(http_requests_total) BY (job) / 0`,
				`http_requests_total{group="canary", instance="0", job="api-server"} / 0`,
				`-1 * http_requests_total{group="canary", instance="0", job="api-server"} / 0`,
				`0 * http_requests_total{group="canary", instance="0", job="api-server"} / 0`,
				`0 * http_requests_total{group="canary", instance="0", job="api-server"} % 0`,
				`SUM(http_requests_total) BY (job) + SUM(http_requests_total) BY (job)`,
				`(SUM((http_requests_total)) BY (job)) + SUM(http_requests_total) BY (job)`,
				`http_requests_total{job="api-server", group="canary"}`,
				`rate(http_requests_total[25m]) * 25 * 60`,
				`(rate((http_requests_total[25m])) * 25) * 60`,
				`http_requests_total{group="canary"} and http_requests_total{instance="0"}`,
				`(http_requests_total{group="canary"} + 1) and http_requests_total{instance="0"}`,
				`(http_requests_total{group="canary"} + 1) and on(instance, job) http_requests_total{instance="0", group="production"}`,
				`(http_requests_total{group="canary"} + 1) and on(instance) http_requests_total{instance="0", group="production"}`,
				`(http_requests_total{group="canary"} + 1) and ignoring(group) http_requests_total{instance="0", group="production"}`,
				`(http_requests_total{group="canary"} + 1) and ignoring(group, job) http_requests_total{instance="0", group="production"}`,
				`http_requests_total{group="canary"} or http_requests_total{group="production"}`,
				`(http_requests_total{group="canary"} + 1) or http_requests_total{instance="1"}`,
				`(http_requests_total{group="canary"} + 1) or on(instance) (http_requests_total or cpu_count or vector_matching_a)`,
				`(http_requests_total{group="canary"} + 1) or ignoring(l, group, job) (http_requests_total or cpu_count or vector_matching_a)`,
				`http_requests_total{group="canary"} unless http_requests_total{instance="0"}`,
				`http_requests_total{group="canary"} unless on(job) http_requests_total{instance="0"}`,
				`http_requests_total{group="canary"} unless on(job, instance) http_requests_total{instance="0"}`,
				`http_requests_total{group="canary"} / on(instance,job) http_requests_total{group="production"}`,
				`http_requests_total{group="canary"} unless ignoring(group, instance) http_requests_total{instance="0"}`,
				`http_requests_total{group="canary"} unless ignoring(group) http_requests_total{instance="0"}`,
				`http_requests_total{group="canary"} / ignoring(group) http_requests_total{group="production"}`,
				`http_requests_total AND ON (dummy) vector(1)`,
				`http_requests_total AND IGNORING (group, instance, job) vector(1)`,
				`SUM(http_requests_total) BY (job) > 1000`,
				`1000 < SUM(http_requests_total) BY (job)`,
				`SUM(http_requests_total) BY (job) <= 1000`,
				`SUM(http_requests_total) BY (job) != 1000`,
				`SUM(http_requests_total) BY (job) == 1000`,
				`SUM(http_requests_total) BY (job) == bool 1000`,
				`SUM(http_requests_total) BY (job) == bool SUM(http_requests_total) BY (job)`,
				`SUM(http_requests_total) BY (job) != bool SUM(http_requests_total) BY (job)`,
				`0 == bool 1`,
				`1 == bool 1`,
				`http_requests_total{job="api-server", instance="0", group="production"} == bool 100`,
			},
		},
		{
			name: "wrong query time",
			load: `load 5m
				  node_var{instance="abc",job="node"} 2
				  node_role{instance="abc",job="node",role="prometheus"} 1`,
			queryTime: time.Unix(3000, 0),
			queries: []string{
				`node_role * on (instance) group_right (role) node_var`,
			},
		},
		{
			name: "operators",
			load: `load 5m
				  node_var{instance="abc",job="node"} 2
				  node_role{instance="abc",job="node",role="prometheus"} 1

				load 5m
				  node_cpu{instance="abc",job="node",mode="idle"} 3
				  node_cpu{instance="abc",job="node",mode="user"} 1
				  node_cpu{instance="def",job="node",mode="idle"} 8
				  node_cpu{instance="def",job="node",mode="user"} 2

				load 5m
				  random{foo="bar"} 1

				load 5m
				  threshold{instance="abc",job="node",target="a@b.com"} 0`,
			queryTime: time.Unix(60, 0),
			queries: []string{
				`node_role * on (instance) group_right (role) node_var`,
				`node_var * on (instance) group_left (role) node_role`,
				`node_var * ignoring (role) group_left (role) node_role`,
				`node_role * ignoring (role) group_right (role) node_var`,
				`node_cpu * ignoring (role, mode) group_left (role) node_role`,
				`node_cpu * on (instance) group_left (role) node_role`,
				`node_cpu / on (instance) group_left sum by (instance,job)(node_cpu)`,
				`sum by (mode, job)(node_cpu) / on (job) group_left sum by (job)(node_cpu)`,
				`sum(sum by (mode, job)(node_cpu) / on (job) group_left sum by (job)(node_cpu))`,
				`node_cpu / ignoring (mode) group_left sum without (mode)(node_cpu)`,
				`node_cpu / ignoring (mode) group_left(dummy) sum without (mode)(node_cpu)`,
				`sum without (instance)(node_cpu) / ignoring (mode) group_left sum without (instance, mode)(node_cpu)`,
				`sum(sum without (instance)(node_cpu) / ignoring (mode) group_left sum without (instance, mode)(node_cpu))`,
				`node_cpu + on(dummy) group_left(foo) random*0`,
				`node_cpu > on(job, instance) group_left(target) threshold`,
				`node_cpu > on(job, instance) group_left(target) (threshold or on (job, instance) (sum by (job, instance)(node_cpu) * 0 + 1))`,
				`node_cpu + 2`,
				`node_cpu - 2`,
				`node_cpu / 2`,
				`node_cpu * 2`,
				`node_cpu ^ 2`,
				`node_cpu % 2`,
			},
		},
		{
			name: "at_modifiers",
			load: `load 10s
				  metric{job="1"} 0+1x1000
				  metric{job="2"} 0+2x1000

				load 1ms
				  metric_ms 0+1x10000`,
			queryTime: time.Unix(10, 0),
			queries: []string{
				`metric @ 100`,
				`metric @ 100s`,
				`metric @ 1m40s`,
				`metric @ 100 offset 50s`,
				`metric @ 100 offset 50`,
				`metric offset 50s @ 100`,
				`metric offset 50 @ 100`,
				`metric @ 0 offset -50s`,
				`metric @ 0 offset -50`,
				`metric offset -50s @ 0`,
				`metric offset -50 @ 0`,
				`metric @ 0 offset -50s`,
				`metric @ 0 offset -50`,
				`-metric @ 100`,
				`---metric @ 100`,
				`minute(metric @ 1500)`,
			},
		},
		{
			name: "at_modifiers",
			load: `load 10s
				  metric{job="1"} 0+1x1000
				  metric{job="2"} 0+2x1000

				load 1ms
				  metric_ms 0+1x10000`,
			queryTime: time.Unix(25, 0),
			queries: []string{
				`sum_over_time(metric{job="1"}[100s] @ 100)`,
				`sum_over_time(metric{job="1"}[100s] @ 100 offset 50s)`,
				`sum_over_time(metric{job="1"}[100s] offset 50s @ 100)`,
				`sum_over_time(metric{job="1"}[100] @ 100 offset 50)`,
				`sum_over_time(metric{job="1"}[100] offset 50s @ 100)`,
				`metric{job="1"} @ 50 + metric{job="1"} @ 100`,
				`sum_over_time(metric{job="1"}[100s] @ 100) + label_replace(sum_over_time(metric{job="2"}[100s] @ 100), "job", "1", "", "")`,
				`sum_over_time(metric{job="1"}[100] @ 100) + label_replace(sum_over_time(metric{job="2"}[100] @ 100), "job", "1", "", "")`,
				`sum_over_time(metric{job="1"}[100s:1s] @ 100)`,
				`sum_over_time(metric{job="1"}[100s:1s] @ 100 offset 20s)`,
				`sum_over_time(metric{job="1"}[100s:1s] offset 20s @ 100)`,
				`sum_over_time(metric{job="1"}[100:1] offset 20 @ 100)`,
			},
		},
		{
			name: "at_modifiers",
			load: `load 10s
				  metric{job="1"} 0+1x1000
				  metric{job="2"} 0+2x1000

				load 1ms
				  metric_ms 0+1x10000`,
			queryTime: time.Unix(0, 0),
			queries: []string{
				`sum_over_time(sum_over_time(sum_over_time(metric{job="1"}[100s] @ 100)[100s:25s] @ 50)[3s:1s] @ 3000)`,
				`sum_over_time(sum_over_time(sum_over_time(metric{job="1"}[10s])[100s:25s] @ 50)[3s:1s] @ 200)`,
				`sum_over_time(sum_over_time(sum_over_time(metric{job="1"}[10s])[100s:25s] @ 200)[3s:1s] @ 50)`,
				`sum_over_time(sum_over_time(sum_over_time(metric{job="1"}[20s])[20s:10s] offset 10s)[100s:25s] @ 1000)`,
				`sum_over_time(minute(metric @ 1500)[100s:10s])`,
				`sum_over_time(minute()[50m:1m] @ 6000)`,
				`sum_over_time(minute()[50m:1m] @ 6000 offset 5m)`,
				`sum_over_time(vector(time())[100s:1s] @ 3000)`,
				`sum_over_time(vector(time())[100s:1s] @ 3000 offset 600s)`,
				`sum_over_time(timestamp(metric{job="1"} @ 10)[100s:10s] @ 3000)`,
				`sum_over_time(timestamp(timestamp(metric{job="1"} @ 999))[10s:1s] @ 10)`,
			},
		},
		{
			name: "literals",
			load: `load 5m
					metric	60 120 180`,
			queryTime: time.Unix(3000, 0),
			queries: []string{
				`12.34e6`,
				`12.34e+6`,
				`12.34e-6`,
				`1+1`,
				`1-1`,
				`1 - -1`,
				`.2`,
				`+0.2`,
				`-0.2e-6`,
				`+Inf`,
				`inF`,
				`-inf`,
				`NaN`,
				`nan`,
				`2.`,
				`1 / 0`,
				`((1) / (0))`,
				`-1 / 0`,
				`0 / 0`,
				`1 % 0`,
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			queryTime := defaultQueryTime
			if !tc.queryTime.IsZero() {
				queryTime = tc.queryTime
			}

			t.Parallel()
			testStorage := promqltest.LoadedStorage(t, tc.load)
			t.Cleanup(func() { testStorage.Close() })

			database := storageToDB(t, testStorage)
			ctx := context.Background()

			for _, query := range tc.queries {
				t.Run("", func(t *testing.T) {
					t.Parallel()

					q1, err := engine.NewInstantQuery(ctx, database.Queryable(), nil, query, queryTime)
					if err != nil {
						t.Fatalf("error for query `%s`: %v", query, err)
					}
					defer q1.Close()

					newResult := q1.Exec(ctx)

					q2, err := engine.NewInstantQuery(ctx, testStorage, nil, query, queryTime)
					if err != nil {
						t.Fatalf("error for query `%s`: %v", query, err)
					}
					defer q2.Close()

					oldResult := q2.Exec(ctx)

					if !cmp.Equal(oldResult, newResult, comparer) {
						t.Fatalf("query `%s`: expected results to be equal, \nOLD:\n%s\nNEW:\n%s", query, oldResult, newResult)
					}
				})
			}
		})
	}
}

func TestRangeQuery(t *testing.T) {
	engine := promql.NewEngine(opts)
	t.Cleanup(func() { engine.Close() })

	type rangeQuery struct {
		query      string
		start, end time.Time
		step       time.Duration
	}

	cases := []struct {
		load    string
		name    string
		queries []rangeQuery
	}{
		{
			name: "sum_over_time with all values",
			load: `load 15s
				bar 0 1 10 100 1000`,
			queries: []rangeQuery{
				{
					query: `sum_over_time(bar[30s])`,
					start: time.Unix(0, 0),
					end:   time.Unix(60, 0),
					step:  time.Second * 30,
				},
			},
		},
		{
			name: "sum_over_time with trailing values",
			load: `load 15s
				bar 0 1 10 100 1000 0 0 0 0`,
			queries: []rangeQuery{
				{
					query: `sum_over_time(bar[30s])`,
					start: time.Unix(0, 0),
					end:   time.Unix(120, 0),
					step:  time.Minute,
				},
			},
		},
		{
			name: "sum_over_time with all values long",
			load: `load 30s
				bar 0 1 10 100 1000 10000 100000 1000000 10000000`,
			queries: []rangeQuery{
				{
					query: `sum_over_time(bar[30s])`,
					start: time.Unix(0, 0),
					end:   time.Unix(480, 0),
					step:  time.Minute,
				},
			},
		},
		{
			name: "sum_over_time with all values random",
			load: `load 15s
				bar 5 17 42 2 7 905 51`,
			queries: []rangeQuery{
				{
					query: `sum_over_time(bar[30s])`,
					start: time.Unix(0, 0),
					end:   time.Unix(90, 0),
					step:  time.Second * 30,
				},
			},
		},
		{
			name: "metric query",
			load: `load 30s
				metric 1+1x4`,
			queries: []rangeQuery{
				{
					query: `metric`,
					start: time.Unix(0, 0),
					end:   time.Unix(120, 0),
					step:  time.Minute,
				},
			},
		},
		{
			name: "metric query with trailing values",
			load: `load 30s
				metric 1+1x8`,
			queries: []rangeQuery{
				{
					query: `metric`,
					start: time.Unix(0, 0),
					end:   time.Unix(120, 0),
					step:  time.Minute,
				},
			},
		},
		{
			name: "short-circuit",
			load: `load 30s
				foo{job="1"} 1+1x4
				bar{job="2"} 1+1x4`,
			queries: []rangeQuery{
				{
					query: `foo > 2 or bar`,
					start: time.Unix(0, 0),
					end:   time.Unix(120, 0),
					step:  time.Minute,
				},
			},
		},
		{
			name: "drop metric name",
			load: `load 30s
				requests{job="1", __address__="bar"} 100`,
			queries: []rangeQuery{
				{
					query: `requests * 2`,
					start: time.Unix(0, 0),
					end:   time.Unix(120, 0),
					step:  time.Minute,
				},
			},
		},
		{
			name: "empty and nonempty matchers",
			load: `
        load 30s
			    http_requests_total{pod="nginx-1", route="/"} 0+1x5
			    http_requests_total{pod="nginx-2"} 0+2x5
			    http_requests_total{pod="nginx-3", route="/"} 0+3x5
			    http_requests_total{pod="nginx-4"} 0+4x5`,
			queries: []rangeQuery{
				{
					query: `http_requests_total{route=""}`,
					start: time.Unix(0, 0),
					end:   time.Unix(120, 0),
					step:  time.Minute,
				},
				{
					query: `http_requests_total{route!=""}`,
					start: time.Unix(0, 0),
					end:   time.Unix(120, 0),
					step:  time.Minute,
				},
				{
					query: `http_requests_total{route=~""}`,
					start: time.Unix(0, 0),
					end:   time.Unix(120, 0),
					step:  time.Minute,
				},
				{
					query: `http_requests_total{route!~""}`,
					start: time.Unix(0, 0),
					end:   time.Unix(120, 0),
					step:  time.Minute,
				},
				{
					query: `http_requests_total{route=~".+"}`,
					start: time.Unix(0, 0),
					end:   time.Unix(120, 0),
					step:  time.Minute,
				},
				{
					query: `http_requests_total{route!~".+"}`,
					start: time.Unix(0, 0),
					end:   time.Unix(120, 0),
					step:  time.Minute,
				},
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			testStorage := promqltest.LoadedStorage(t, tc.load)
			t.Cleanup(func() { testStorage.Close() })

			db := storageToDB(t, testStorage)
			ctx := t.Context()

			for _, query := range tc.queries {
				q1, err := engine.NewRangeQuery(ctx, db.Queryable(), nil, query.query, query.start, query.end, query.step)
				if err != nil {
					t.Fatalf("error for query `%s`: %v", query.query, err)
				}
				defer q1.Close()

				newResult := q1.Exec(ctx)

				q2, err := engine.NewRangeQuery(ctx, testStorage, nil, query.query, query.start, query.end, query.step)
				if err != nil {
					t.Fatalf("error for query `%s`: %v", query.query, err)
				}
				defer q2.Close()

				oldResult := q2.Exec(ctx)

				if !cmp.Equal(oldResult, newResult, comparer) {
					t.Fatalf("query `%s`: expected results to be equal, \nOLD:\n%s\nNEW:\n%s", query.query, oldResult, newResult)
				}
			}
		})
	}
}

func TestExternalAndReplicaLabels(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st := teststorage.New(t)
	t.Cleanup(func() { st.Close() })

	bkt, err := filesystem.NewBucket(t.TempDir())
	if err != nil {
		t.Fatalf("unable to create bucket: %s", err)
	}
	t.Cleanup(func() { bkt.Close() })

	engine := promql.NewEngine(opts)
	t.Cleanup(func() { engine.Close() })

	app := st.Appender(ctx)
	for i := range 100 {
		app.Append(0, labels.FromStrings("__name__", "foo", "replica", "1", "bar", fmt.Sprintf("%d", 2*i)), 0, float64(i))
		app.Append(0, labels.FromStrings("__name__", "foo", "replica", "2", "bar", fmt.Sprintf("%d", 2*i)), 0, float64(i))
	}
	if err := app.Commit(); err != nil {
		t.Fatalf("unable to commit: %s", err)
	}
	database := storageToDB(t, st)

	expr := `foo{bar=~"0"}`

	q1, err := engine.NewInstantQuery(ctx, database.Queryable(db.DropReplicaLabels("replica")), nil, expr, time.Unix(5, 0))
	if err != nil {
		t.Fatalf("error for query: %v", err)
	}
	defer q1.Close()

	res := q1.Exec(ctx)

	if err := res.Err; err != nil {
		t.Fatalf("error for query execution: %v", err)
	}
	v, err := res.Vector()
	if err != nil {
		t.Fatalf("result was no vector: %v", err)
	}
	for i := range v {
		if v[i].Metric.Get("replica") != "" {
			t.Fatalf("unexpected series in result for query %s: %s", expr, v[i].Metric)
		}
	}
}

func storageToDBWithBkt(tb testing.TB, st *teststorage.TestStorage, bkt objstore.Bucket, opts ...db.DBOption) *db.DB {
	ctx := context.Background()

	h := st.DB.Head()
	day := time.UnixMilli(h.MinTime()).UTC()
	if err := convert.ConvertTSDBBlock(ctx, bkt, day, []convert.Convertable{h}); err != nil {
		tb.Fatal(err)
	}

	discoverer := locate.NewDiscoverer(bkt)
	if err := discoverer.Discover(ctx); err != nil {
		tb.Fatal(err)
	}
	syncer := locate.NewSyncer(bkt, locate.BlockOptions(locate.LabelFilesDir(tb.TempDir())))
	if err := syncer.Sync(ctx, discoverer.Metas()); err != nil {
		tb.Fatal(err)
	}
	return db.NewDB(syncer, opts...)
}

func storageToDB(tb testing.TB, st *teststorage.TestStorage, opts ...db.DBOption) *db.DB {
	bkt, err := filesystem.NewBucket(tb.TempDir())
	if err != nil {
		tb.Fatalf("unable to create bucket: %s", err)
	}
	return storageToDBWithBkt(tb, st, bkt, opts...)

}

type seriesByLabels []promql.Series

func (b seriesByLabels) Len() int           { return len(b) }
func (b seriesByLabels) Swap(i, j int)      { b[i], b[j] = b[j], b[i] }
func (b seriesByLabels) Less(i, j int) bool { return labels.Compare(b[i].Metric, b[j].Metric) < 0 }

type samplesByLabels []promql.Sample

func (b samplesByLabels) Len() int           { return len(b) }
func (b samplesByLabels) Swap(i, j int)      { b[i], b[j] = b[j], b[i] }
func (b samplesByLabels) Less(i, j int) bool { return labels.Compare(b[i].Metric, b[j].Metric) < 0 }

var (
	// comparer should be used to compare promql results between engines.
	comparer = cmp.Comparer(func(x, y *promql.Result) bool {
		compareFloats := func(l, r float64) bool {
			const epsilon = 1e-6
			return cmp.Equal(l, r, cmpopts.EquateNaNs(), cmpopts.EquateApprox(0, epsilon))
		}
		compareHistograms := func(l, r *histogram.FloatHistogram) bool {
			if l == nil && r == nil {
				return true
			}
			return l.Equals(r)
		}
		compareMetrics := func(l, r labels.Labels) bool {
			return l.Hash() == r.Hash()
		}

		if x.Err != nil && y.Err != nil {
			return cmp.Equal(x.Err.Error(), y.Err.Error())
		} else if x.Err != nil || y.Err != nil {
			return false
		}

		vx, xvec := x.Value.(promql.Vector)
		vy, yvec := y.Value.(promql.Vector)

		if xvec && yvec {
			if len(vx) != len(vy) {
				return false
			}

			// Sort vector before comparing.
			sort.Sort(samplesByLabels(vx))
			sort.Sort(samplesByLabels(vy))

			for i := range len(vx) {
				if !compareMetrics(vx[i].Metric, vy[i].Metric) {
					return false
				}
				if vx[i].T != vy[i].T {
					return false
				}
				if !compareFloats(vx[i].F, vy[i].F) {
					return false
				}
				if !compareHistograms(vx[i].H, vy[i].H) {
					return false
				}
			}
			return true
		}

		mx, xmat := x.Value.(promql.Matrix)
		my, ymat := y.Value.(promql.Matrix)

		if xmat && ymat {
			if len(mx) != len(my) {
				return false
			}
			// Sort matrix before comparing.
			sort.Sort(seriesByLabels(mx))
			sort.Sort(seriesByLabels(my))
			for i := range len(mx) {
				mxs := mx[i]
				mys := my[i]

				if !compareMetrics(mxs.Metric, mys.Metric) {
					return false
				}

				xps := mxs.Floats
				yps := mys.Floats

				if len(xps) != len(yps) {
					return false
				}
				for j := range len(xps) {
					if xps[j].T != yps[j].T {
						return false
					}
					if !compareFloats(xps[j].F, yps[j].F) {
						return false
					}
				}
				xph := mxs.Histograms
				yph := mys.Histograms

				if len(xph) != len(yph) {
					return false
				}
				for j := range len(xph) {
					if xph[j].T != yph[j].T {
						return false
					}
					if !compareHistograms(xph[j].H, yph[j].H) {
						return false
					}
				}
			}
			return true
		}

		sx, xscalar := x.Value.(promql.Scalar)
		sy, yscalar := y.Value.(promql.Scalar)
		if xscalar && yscalar {
			if sx.T != sy.T {
				return false
			}
			return compareFloats(sx.V, sy.V)
		}
		return false
	})
)
