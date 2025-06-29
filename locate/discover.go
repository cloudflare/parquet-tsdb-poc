// Copyright (c) 2025 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

package locate

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"math"
	"slices"
	"strings"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/thanos-io/objstore"
	"github.com/thanos-io/thanos/pkg/block/metadata"

	"github.com/cloudflare/parquet-tsdb-poc/proto/metapb"
	"github.com/cloudflare/parquet-tsdb-poc/schema"
)

type discoveryConfig struct {
	concurrency int
}

type DiscoveryOption func(*discoveryConfig)

func MetaConcurrency(c int) DiscoveryOption {
	return func(cfg *discoveryConfig) {
		cfg.concurrency = c
	}
}

type Discoverer struct {
	bkt objstore.Bucket

	mu    sync.Mutex
	metas map[string]schema.Meta

	concurrency int
}

func NewDiscoverer(bkt objstore.Bucket, opts ...DiscoveryOption) *Discoverer {
	cfg := discoveryConfig{
		concurrency: 1,
	}
	for _, o := range opts {
		o(&cfg)
	}
	return &Discoverer{
		bkt:         bkt,
		metas:       make(map[string]schema.Meta),
		concurrency: cfg.concurrency,
	}
}

func (s *Discoverer) Metas() map[string]schema.Meta {
	s.mu.Lock()
	defer s.mu.Unlock()

	res := make(map[string]schema.Meta, len(s.metas))
	maps.Copy(res, s.metas)

	return res
}

func (s *Discoverer) Discover(ctx context.Context) error {
	m := make(map[string][]string)
	err := s.bkt.Iter(ctx, "", func(n string) error {
		id, file, ok := schema.SplitBlockPath(n)
		if !ok {
			return nil
		}
		m[id] = append(m[id], file)
		return nil
	}, objstore.WithRecursiveIter())
	if err != nil {
		return err
	}

	type metaOrError struct {
		m   schema.Meta
		err error
	}

	metaC := make(chan metaOrError)
	go func() {
		defer close(metaC)

		workerC := make(chan string, s.concurrency)
		go func() {
			defer close(workerC)

			for k, v := range m {
				if !slices.Contains(v, schema.MetaFile) {
					// skip incomplete block
					continue
				}
				if _, ok := s.metas[k]; ok {
					// we already got the block
					continue
				}
				workerC <- k
			}
		}()

		var wg sync.WaitGroup
		defer wg.Wait()

		for range s.concurrency {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for k := range workerC {
					meta, err := readMetaFile(ctx, s.bkt, k)
					if err != nil {
						metaC <- metaOrError{err: fmt.Errorf("unable to read meta file for %q: %w", k, err)}
					} else {
						metaC <- metaOrError{m: meta}
					}
				}
			}()
		}
	}()

	am := make(map[string]struct{})
	for k, v := range m {
		if !slices.Contains(v, schema.MetaFile) {
			// skip incomplete block
			continue
		}
		am[k] = struct{}{}
	}

	nm := make(map[string]schema.Meta)
	for m := range metaC {
		if m.err != nil {
			return fmt.Errorf("unable to read meta: %w", m.err)
		}
		nm[m.m.Name] = m.m
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	maps.Copy(s.metas, nm)

	// delete metas that are no longer in the bucket
	maps.DeleteFunc(s.metas, func(k string, _ schema.Meta) bool {
		_, ok := am[k]
		return !ok
	})

	if len(s.metas) != 0 {
		mint, maxt := int64(math.MaxInt64), int64(math.MinInt64)
		for _, v := range s.metas {
			mint = min(mint, v.Mint)
			maxt = max(maxt, v.Mint)
		}
		syncMinTime.WithLabelValues(whatDiscoverer).Set(float64(mint))
		syncMaxTime.WithLabelValues(whatDiscoverer).Set(float64(maxt))
	}
	syncLastSuccessfulTime.WithLabelValues(whatDiscoverer).SetToCurrentTime()

	return nil
}

func readMetaFile(ctx context.Context, bkt objstore.Bucket, name string) (schema.Meta, error) {
	mfile := schema.MetaFileNameForBlock(name)
	if _, err := bkt.Attributes(ctx, mfile); err != nil {
		return schema.Meta{}, fmt.Errorf("unable to attr %s: %w", mfile, err)
	}
	rdr, err := bkt.Get(ctx, mfile)
	if err != nil {
		return schema.Meta{}, fmt.Errorf("unable to get %s: %w", mfile, err)
	}
	defer rdr.Close()

	metaBytes, err := io.ReadAll(rdr)
	if err != nil {
		return schema.Meta{}, fmt.Errorf("unable to read %s: %w", mfile, err)
	}

	metapb := &metapb.Metadata{}
	if err := proto.Unmarshal(metaBytes, metapb); err != nil {
		return schema.Meta{}, fmt.Errorf("unable to read %s: %w", mfile, err)
	}

	// for version == 0
	m := make(map[string][]string, len(metapb.GetColumnsForName()))
	for k, v := range metapb.GetColumnsForName() {
		m[k] = v.GetColumns()
	}
	return schema.Meta{
		Version:        int(metapb.GetVersion()),
		Name:           name,
		Mint:           metapb.GetMint(),
		Maxt:           metapb.GetMaxt(),
		Shards:         metapb.GetShards(),
		ColumnsForName: m,
	}, nil
}

type tsdbDiscoveryConfig struct {
	concurrency int

	externalLabelMatchers []*labels.Matcher
	minBlockAge           time.Duration
}

type TSDBDiscoveryOption func(*tsdbDiscoveryConfig)

func TSDBMetaConcurrency(c int) TSDBDiscoveryOption {
	return func(cfg *tsdbDiscoveryConfig) {
		cfg.concurrency = c
	}
}

func TSDBMatchExternalLabels(ms ...*labels.Matcher) TSDBDiscoveryOption {
	return func(cfg *tsdbDiscoveryConfig) {
		cfg.externalLabelMatchers = ms
	}
}

func TSDBMinBlockAge(d time.Duration) TSDBDiscoveryOption {
	return func(cfg *tsdbDiscoveryConfig) {
		cfg.minBlockAge = d
	}
}

type TSDBDiscoverer struct {
	bkt objstore.Bucket

	mu    sync.Mutex
	metas map[string]metadata.Meta

	externalLabelMatchers []*labels.Matcher
	minBlockAge           time.Duration

	concurrency int
}

func NewTSDBDiscoverer(bkt objstore.Bucket, opts ...TSDBDiscoveryOption) *TSDBDiscoverer {
	cfg := tsdbDiscoveryConfig{
		concurrency: 1,
	}
	for _, o := range opts {
		o(&cfg)
	}
	return &TSDBDiscoverer{
		bkt:                   bkt,
		metas:                 make(map[string]metadata.Meta),
		concurrency:           cfg.concurrency,
		externalLabelMatchers: cfg.externalLabelMatchers,
		minBlockAge:           cfg.minBlockAge,
	}
}

func (s *TSDBDiscoverer) Metas() map[string]metadata.Meta {
	s.mu.Lock()
	defer s.mu.Unlock()

	res := make(map[string]metadata.Meta, len(s.metas))
	maps.Copy(res, s.metas)

	return res
}

func (s *TSDBDiscoverer) Discover(ctx context.Context) error {
	m := make(map[string][]string)
	err := s.bkt.Iter(ctx, "", func(n string) error {
		split := strings.Split(n, "/")
		if len(split) != 2 {
			return nil
		}
		id, f := split[0], split[1]

		m[id] = append(m[id], f)
		return nil
	}, objstore.WithRecursiveIter())
	if err != nil {
		return err
	}

	type metaOrError struct {
		m   metadata.Meta
		err error
	}

	metaC := make(chan metaOrError)
	go func() {
		defer close(metaC)

		workerC := make(chan string, s.concurrency)
		go func() {
			defer close(workerC)

			for k, v := range m {
				if !slices.Contains(v, metadata.MetaFilename) {
					// skip incomplete block
					continue
				}
				if slices.Contains(v, metadata.DeletionMarkFilename) {
					// skip block that is about to be deleted
					continue
				}
				if _, ok := s.metas[k]; ok {
					// we already got the block
					continue
				}
				workerC <- k
			}
		}()

		var wg sync.WaitGroup
		defer wg.Wait()

		for range s.concurrency {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for k := range workerC {
					meta, err := s.readMetaFile(ctx, k)
					if err != nil {
						metaC <- metaOrError{err: fmt.Errorf("unable to read meta file for %q: %w", k, err)}
					} else {
						metaC <- metaOrError{m: meta}
					}
				}
			}()
		}
	}()

	am := make(map[string]struct{})
	for k, v := range m {
		if !slices.Contains(v, metadata.MetaFilename) {
			// skip incomplete block
			continue
		}
		if slices.Contains(v, metadata.DeletionMarkFilename) {
			// skip block that is about to be deleted
			continue
		}
		am[k] = struct{}{}
	}

	nm := make(map[string]metadata.Meta)
	for m := range metaC {
		if m.err != nil {
			return fmt.Errorf("unable to read meta: %w", m.err)
		}
		nm[m.m.ULID.String()] = m.m
	}

	// filter for metas that match our external labels
	maps.DeleteFunc(nm, func(_ string, v metadata.Meta) bool {
		series := labels.FromMap(v.Thanos.Labels)

		for _, m := range s.externalLabelMatchers {
			if !m.Matches(series.Get(m.Name)) {
				return true
			}
		}
		return false
	})

	s.mu.Lock()
	defer s.mu.Unlock()

	maps.Copy(s.metas, nm)

	// filter for metas that dont contain data after "now-minAge"
	maps.DeleteFunc(s.metas, func(_ string, v metadata.Meta) bool {
		return time.UnixMilli(v.MaxTime).After(time.Now().Add(-s.minBlockAge))
	})
	// delete metas that are no longer in the bucket
	maps.DeleteFunc(s.metas, func(k string, _ metadata.Meta) bool {
		_, ok := am[k]
		return !ok
	})

	if len(s.metas) != 0 {
		mint, maxt := int64(math.MaxInt64), int64(math.MinInt64)
		for _, v := range s.metas {
			mint = min(mint, v.MinTime)
			maxt = max(maxt, v.MaxTime)
		}
		syncMinTime.WithLabelValues(whatDiscoverer).Set(float64(mint))
		syncMaxTime.WithLabelValues(whatDiscoverer).Set(float64(maxt))
	}
	syncLastSuccessfulTime.WithLabelValues(whatDiscoverer).SetToCurrentTime()

	return nil
}

func (s *TSDBDiscoverer) readMetaFile(ctx context.Context, name string) (metadata.Meta, error) {
	mfile := fmt.Sprintf("%s/%s", name, metadata.MetaFilename)
	if _, err := s.bkt.Attributes(ctx, mfile); err != nil {
		return metadata.Meta{}, fmt.Errorf("unable to attr %s: %w", mfile, err)
	}
	rdr, err := s.bkt.Get(ctx, mfile)
	if err != nil {
		return metadata.Meta{}, fmt.Errorf("unable to get %s: %w", mfile, err)
	}
	defer rdr.Close()

	var m metadata.Meta
	if err := json.NewDecoder(rdr).Decode(&m); err != nil {
		return metadata.Meta{}, fmt.Errorf("unable to decode %s: %w", mfile, err)
	}
	return m, nil
}
