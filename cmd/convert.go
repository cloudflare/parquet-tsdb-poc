// Copyright (c) 2025 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/oklog/run"
	"github.com/parquet-go/parquet-go"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/thanos-io/objstore"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/runutil"
	"golang.org/x/sync/errgroup"
	"gopkg.in/alecthomas/kingpin.v2"

	"github.com/cloudflare/parquet-tsdb-poc/convert"
	"github.com/cloudflare/parquet-tsdb-poc/locate"
)

type convertOpts struct {
	parquetBucket   bucketOpts
	tsdbBucket      bucketOpts
	parquetDiscover discoveryOpts
	tsdbDiscover    tsdbDiscoveryOpts
	conversion      conversionOpts
	internalAPI     apiOpts
}

type conversionOpts struct {
	runInterval   time.Duration
	runTimeout    time.Duration
	retryInterval time.Duration

	gracePeriod         time.Duration
	recompress          bool
	sortLabels          []string
	rowGroupSize        int
	rowGroupCount       int
	downloadConcurrency int
	encodingConcurrency int

	tempDir string
}

func (opts *convertOpts) registerFlags(cmd *kingpin.CmdClause) {
	opts.conversion.registerFlags(cmd)
	opts.parquetBucket.registerConvertParquetFlags(cmd)
	opts.tsdbBucket.registerConvertTSDBFlags(cmd)
	opts.parquetDiscover.registerConvertParquetFlags(cmd)
	opts.tsdbDiscover.registerConvertTSDBFlags(cmd)
	opts.internalAPI.registerConvertFlags(cmd)
}

func (opts *conversionOpts) registerFlags(cmd *kingpin.CmdClause) {
	cmd.Flag("convert.run-interval", "interval to run conversion on").Default("1h").DurationVar(&opts.runInterval)
	cmd.Flag("convert.run-timeout", "timeout for a single conversion step").Default("24h").DurationVar(&opts.runTimeout)
	cmd.Flag("convert.retry-interval", "interval to retry a single conversion after an error").Default("1m").DurationVar(&opts.retryInterval)
	cmd.Flag("convert.tempdir", "directory for temporary state").Default(os.TempDir()).StringVar(&opts.tempDir)
	cmd.Flag("convert.recompress", "recompress chunks").Default("true").BoolVar(&opts.recompress)
	cmd.Flag("convert.grace-period", "dont convert for dates younger than this").Default("48h").DurationVar(&opts.gracePeriod)
	cmd.Flag("convert.rowgroup.size", "size of rowgroups").Default("1_000_000").IntVar(&opts.rowGroupSize)
	cmd.Flag("convert.rowgroup.count", "rowgroups per shard").Default("6").IntVar(&opts.rowGroupCount)
	cmd.Flag("convert.sorting.label", "label to sort by").Default("__name__").StringsVar(&opts.sortLabels)
	cmd.Flag("convert.download.concurrency", "concurrency for downloading tsdb blocks").Default("4").IntVar(&opts.downloadConcurrency)
	cmd.Flag("convert.encoding.concurrency", "concurrency for encoding chunks").Default("4").IntVar(&opts.encodingConcurrency)
}

func (opts *bucketOpts) registerConvertParquetFlags(cmd *kingpin.CmdClause) {
	cmd.Flag("parquet.storage.type", "type of storage").Default("filesystem").EnumVar(&opts.storage, "filesystem", "s3")
	cmd.Flag("parquet.storage.prefix", "prefix for the storage").Default("").StringVar(&opts.prefix)
	cmd.Flag("parquet.storage.filesystem.directory", "directory for filesystem").Default(".data").StringVar(&opts.filesystemDirectory)
	cmd.Flag("parquet.storage.s3.bucket", "bucket for s3").Default("").StringVar(&opts.s3Bucket)
	cmd.Flag("parquet.storage.s3.endpoint", "endpoint for s3").Default("").StringVar(&opts.s3Endpoint)
	cmd.Flag("parquet.storage.s3.access_key", "access key for s3").Default("").Envar("PARQUET_STORAGE_S3_ACCESS_KEY").StringVar(&opts.s3AccessKey)
	cmd.Flag("parquet.storage.s3.secret_key", "secret key for s3").Default("").Envar("PARQUET_STORAGE_S3_SECRET_KEY").StringVar(&opts.s3SecretKey)
	cmd.Flag("parquet.storage.s3.insecure", "use http").Default("false").BoolVar(&opts.s3Insecure)
}

func (opts *bucketOpts) registerConvertTSDBFlags(cmd *kingpin.CmdClause) {
	cmd.Flag("tsdb.storage.type", "type of storage").Default("filesystem").EnumVar(&opts.storage, "filesystem", "s3")
	cmd.Flag("tsdb.storage.prefix", "prefix for the storage").Default("").StringVar(&opts.prefix)
	cmd.Flag("tsdb.storage.filesystem.directory", "directory for filesystem").Default(".data").StringVar(&opts.filesystemDirectory)
	cmd.Flag("tsdb.storage.s3.bucket", "bucket for s3").Default("").StringVar(&opts.s3Bucket)
	cmd.Flag("tsdb.storage.s3.endpoint", "endpoint for s3").Default("").StringVar(&opts.s3Endpoint)
	cmd.Flag("tsdb.storage.s3.access_key", "access key for s3").Default("").Envar("TSDB_STORAGE_S3_ACCESS_KEY").StringVar(&opts.s3AccessKey)
	cmd.Flag("tsdb.storage.s3.secret_key", "secret key for s3").Default("").Envar("TSDB_STORAGE_S3_SECRET_KEY").StringVar(&opts.s3SecretKey)
	cmd.Flag("tsdb.storage.s3.insecure", "use http").Default("false").BoolVar(&opts.s3Insecure)
}

func (opts *discoveryOpts) registerConvertParquetFlags(cmd *kingpin.CmdClause) {
	cmd.Flag("parquet.discovery.interval", "interval to discover blocks").Default("30m").DurationVar(&opts.discoveryInterval)
	cmd.Flag("parquet.discovery.concurrency", "concurrency for loading metadata").Default("1").IntVar(&opts.discoveryConcurrency)
}

func (opts *tsdbDiscoveryOpts) registerConvertTSDBFlags(cmd *kingpin.CmdClause) {
	cmd.Flag("tsdb.discovery.interval", "interval to discover blocks").Default("30m").DurationVar(&opts.discoveryInterval)
	cmd.Flag("tsdb.discovery.concurrency", "concurrency for loading metadata").Default("1").IntVar(&opts.discoveryConcurrency)
	cmd.Flag("tsdb.discovery.min-block-age", "blocks that have metrics that are youner then this wont be loaded").Default("0s").DurationVar(&opts.discoveryMinBlockAge)
	MatchersVar(cmd.Flag("tsdb.discovery.select-external-labels", "only external labels matching this selector will be discovered").PlaceHolder("SELECTOR"), &opts.externalLabelMatchers)
}

func (opts *apiOpts) registerConvertFlags(cmd *kingpin.CmdClause) {
	cmd.Flag("http.internal.port", "port to host query api").Default("6060").IntVar(&opts.port)
	cmd.Flag("http.internal.shutdown-timeout", "timeout on shutdown").Default("10s").DurationVar(&opts.shutdownTimeout)
}

func registerConvertApp(app *kingpin.Application) (*kingpin.CmdClause, func(context.Context, *slog.Logger, *prometheus.Registry) error) {
	cmd := app.Command("convert", "convert TSDB Block to parquet file")

	var opts convertOpts
	opts.registerFlags(cmd)

	return cmd, func(ctx context.Context, log *slog.Logger, reg *prometheus.Registry) error {
		var g run.Group

		setupInterrupt(ctx, &g, log)
		setupInternalAPI(&g, log, reg, opts.internalAPI)

		tsdbBkt, err := setupBucket(log, opts.tsdbBucket)
		if err != nil {
			return fmt.Errorf("unable to setup tsdb bucket: %s", err)
		}
		parquetBkt, err := setupBucket(log, opts.parquetBucket)
		if err != nil {
			return fmt.Errorf("unable to setup parquet bucket: %s", err)
		}

		tsdbDiscoverer, err := setupTSDBDiscovery(ctx, &g, log, tsdbBkt, opts.tsdbDiscover)
		if err != nil {
			return fmt.Errorf("unable to setup tsdb discovery: %s", err)
		}
		parquetDiscoverer, err := setupDiscovery(ctx, &g, log, parquetBkt, opts.parquetDiscover)
		if err != nil {
			return fmt.Errorf("unable to setup parquet discovery: %s", err)
		}

		ctx, cancel := context.WithCancel(context.Background())
		g.Add(func() error {
			log.Info("Starting conversion", "sort_by", opts.conversion.sortLabels)
			return runutil.Repeat(opts.conversion.runInterval, ctx.Done(), func() error {
				iterCtx, iterCancel := context.WithTimeout(ctx, opts.conversion.runTimeout)
				defer iterCancel()

				if err := runutil.Retry(opts.conversion.retryInterval, iterCtx.Done(), func() error {
					// Sync parquet files once here so we have the latest view
					log.Info("Discovering parquet blocks before conversion")
					if err := parquetDiscoverer.Discover(iterCtx); err != nil {
						log.Error("Unable to discover parquet blocks", "error", err)
						return err
					}
					log.Info("Converting next blocks", "sort_by", opts.conversion.sortLabels)
					if err := advanceConversion(iterCtx, log, tsdbBkt, parquetBkt, tsdbDiscoverer, parquetDiscoverer, opts.conversion); err != nil {
						log.Error("Unable to convert blocks", "error", err)
						return err
					}
					return nil
				}); err != nil {
					log.Warn("Error during conversion", slog.Any("err", err))
					return nil
				}
				return nil
			})
		}, func(error) {
			log.Info("Stopping conversion")
			cancel()
		})
		return g.Run()
	}
}

func advanceConversion(
	ctx context.Context,
	log *slog.Logger,
	tsdbBkt objstore.Bucket,
	parquetBkt objstore.Bucket,
	tsdbDiscoverer *locate.TSDBDiscoverer,
	parquetDiscoverer *locate.Discoverer,
	opts conversionOpts,
) error {
	blkDir := filepath.Join(opts.tempDir, ".blocks")
	bufferDir := filepath.Join(opts.tempDir, ".buffers")

	log.Info("Cleaning up previous state", "block_directory", blkDir, "buffer_directory", bufferDir)
	if err := cleanupDirectory(blkDir); err != nil {
		return fmt.Errorf("unable to clean up block directory: %w", err)
	}
	if err := cleanupDirectory(bufferDir); err != nil {
		return fmt.Errorf("unable to clean up buffer directory: %w", err)
	}

	parquetMetas := parquetDiscoverer.Metas()
	tsdbMetas := tsdbDiscoverer.Metas()

	plan, ok := convert.NewPlanner(time.Now().Add(-opts.gracePeriod)).Plan(tsdbMetas, parquetMetas)
	if !ok {
		log.Info("Nothing to do")
		return nil
	}
	log.Info("Converting dates", "dates", plan.ConvertForDates)

	log.Info("Opening blocks", "ulids", ulidsFromMetas(plan.Download))
	tsdbBlocks, err := downloadedBlocks(ctx, tsdbBkt, plan.Download, blkDir, opts)
	defer func() {
		for _, blk := range tsdbBlocks {
			if cErr := blk.(io.Closer).Close(); cErr != nil {
				log.Warn("Unable to close block", "block", blk.Meta().ULID, "err", cErr)
			}
		}
	}()
	if err != nil {
		return fmt.Errorf("unable to download tsdb blocks: %w", err)
	}

	for _, next := range plan.ConvertForDates {
		log.Info("Converting next parquet file", "day", next)

		candidates := overlappingBlocks(tsdbBlocks, next)
		if len(candidates) == 0 {
			continue
		}
		convOpts := []convert.ConvertOption{
			convert.SortBy(opts.sortLabels...),
			convert.RowGroupSize(opts.rowGroupSize),
			convert.RowGroupCount(opts.rowGroupCount),
			convert.EncodingConcurrency(opts.encodingConcurrency),
			convert.ChunkBufferPool(parquet.NewFileBufferPool(bufferDir, "chunkbuf-*")),
		}
		if err := convert.ConvertTSDBBlock(ctx, parquetBkt, next, candidates, convOpts...); err != nil {
			return fmt.Errorf("unable to convert blocks for date %q: %s", next, err)
		}
	}
	return nil
}

func cleanupDirectory(dir string) error {
	if err := os.RemoveAll(dir); err != nil {
		return fmt.Errorf("unable to delete directory: %w", err)
	}
	if err := os.MkdirAll(dir, os.ModePerm); err != nil {
		return fmt.Errorf("unable to recreate block directory: %w", err)
	}
	if _, err := os.Stat(dir); err != nil {
		return fmt.Errorf("unable to stat block directory: %w", err)
	}
	return nil
}

func overlappingBlocks(blocks []convert.Convertable, date time.Time) []convert.Convertable {
	res := make([]convert.Convertable, 0)
	for _, m := range blocks {
		if date.AddDate(0, 0, 1).UnixMilli() >= m.Meta().MinTime && date.UnixMilli() <= m.Meta().MaxTime {
			res = append(res, m)
		}
	}
	sort.Slice(res, func(i, j int) bool {
		return res[i].Meta().MaxTime <= res[j].Meta().MaxTime
	})
	return res
}

func ulidsFromMetas(metas []metadata.Meta) []string {
	res := make([]string, len(metas))
	for i := range metas {
		res[i] = metas[i].ULID.String()
	}
	return res
}

func downloadedBlocks(ctx context.Context, bkt objstore.BucketReader, metas []metadata.Meta, blkDir string, opts conversionOpts) ([]convert.Convertable, error) {
	slogger := slog.New(slog.NewJSONHandler(io.Discard, nil))

	res := make([]convert.Convertable, 0)
	for _, m := range metas {
		src := m.ULID.String()
		dst := filepath.Join(blkDir, src)

		if err := runutil.Retry(5*time.Second, ctx.Done(), func() error {
			return downloadBlock(ctx, bkt, m, blkDir, opts)
		}); err != nil {
			return res, fmt.Errorf("unable to download block %q: %w", src, err)
		}
		blk, err := tsdb.OpenBlock(slogger, dst, chunkenc.NewPool(), tsdb.DefaultPostingsDecoderFactory)
		if err != nil {
			return res, fmt.Errorf("unable to open block %q: %w", m.ULID, err)
		}
		res = append(res, blk)
	}
	return res, nil
}

func downloadBlock(ctx context.Context, bkt objstore.BucketReader, meta metadata.Meta, blkDir string, opts conversionOpts) error {
	src := meta.ULID.String()
	dst := filepath.Join(blkDir, src)

	fmap := make(map[string]metadata.File, len(meta.Thanos.Files))
	for _, fl := range meta.Thanos.Files {
		if fl.SizeBytes == 0 || fl.RelPath == "" {
			continue
		}
		fmap[fl.RelPath] = fl
	}

	// order is not guaranteed in "Iter" so we need to create directory structure beforehand
	if err := os.MkdirAll(dst, 0750); err != nil {
		return fmt.Errorf("unable to create block directory: %w", err)
	}
	if err := os.MkdirAll(filepath.Join(dst, "chunks"), 0750); err != nil {
		return fmt.Errorf("unable to create chunks directory: %w", err)
	}

	// we reimplement download dir from objstore to skip the cleanup part on partial downloads
	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(opts.downloadConcurrency)

	err := bkt.Iter(ctx, src, func(name string) error {
		g.Go(func() error {
			dst := filepath.Join(dst, strings.TrimPrefix(name, src))
			if strings.HasSuffix(name, objstore.DirDelim) {
				return nil
			}
			// In case the previous upload failed we dont download the files that have the correct size.
			// Size is not the best indicator, but its good enough. ideally we would want a hash but we
			// dont write those currently.
			// If the file was corrupted, then opening the block will very likely fail anyway.
			if stat, err := os.Stat(dst); err == nil {
				if known, ok := fmap[strings.TrimPrefix(name, src+objstore.DirDelim)]; ok {
					if stat.Size() == known.SizeBytes && stat.Size() != 0 {
						return nil
					}
				}
			}

			rc, err := bkt.Get(ctx, name)
			if err != nil {
				return fmt.Errorf("unable to get file %q: %w", name, err)
			}
			defer rc.Close()

			f, err := os.Create(dst)
			if err != nil {
				return fmt.Errorf("unable to create file %q: %w", dst, err)
			}
			if _, err := io.Copy(f, rc); err != nil {
				return fmt.Errorf("unable to copy file %q: %w", dst, err)
			}
			return nil
		})
		return nil
	}, objstore.WithRecursiveIter())
	if err != nil {
		return fmt.Errorf("unable to iter bucket: %w", err)
	}
	if err := g.Wait(); err != nil {
		return fmt.Errorf("unable to download directory: %w", err)
	}
	return nil
}
