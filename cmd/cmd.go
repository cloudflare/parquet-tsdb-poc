// Copyright (c) 2025 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/KimMachineGun/automemlimit/memlimit"
	"github.com/prometheus/client_golang/prometheus"
	"gopkg.in/alecthomas/kingpin.v2"

	"github.com/cloudflare/parquet-tsdb-poc/api/grpc"
	"github.com/cloudflare/parquet-tsdb-poc/api/http"
	"github.com/cloudflare/parquet-tsdb-poc/convert"
	"github.com/cloudflare/parquet-tsdb-poc/db"
	"github.com/cloudflare/parquet-tsdb-poc/locate"
	"github.com/cloudflare/parquet-tsdb-poc/search"
)

var logLevelMap = map[string]slog.Level{
	"DEBUG": slog.LevelDebug,
	"INFO":  slog.LevelInfo,
	"WARN":  slog.LevelWarn,
	"ERROR": slog.LevelError,
}

func main() {
	app := kingpin.New("parquet-gateway", "parquet metrics experiments")
	memratio := app.Flag("memlimit.ratio", "gomemlimit ratio").Default("0.9").Float()
	logLevel := app.Flag("logger.level", "log level").Default("INFO").Enum("DEBUG", "INFO", "WARN", "ERROR")

	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: logLevelMap[*logLevel],
	}))

	tsdbConvert, tsdbConvertF := registerConvertApp(app)
	serve, serveF := registerServeApp(app)
	parsed := kingpin.MustParse(app.Parse(os.Args[1:]))

	memlimit.SetGoMemLimitWithOpts(
		memlimit.WithRatio(*memratio),
		memlimit.WithProvider(
			memlimit.ApplyFallback(
				memlimit.FromCgroup,
				memlimit.FromSystem,
			),
		),
	)

	reg, err := setupPrometheusRegistry()
	if err != nil {
		log.Error("Could not setup prometheus", slog.Any("err", err))
		return
	}

	sigC := make(chan os.Signal, 1)
	signal.Notify(sigC, syscall.SIGTERM, syscall.SIGINT)

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		s := <-sigC
		log.Warn("Caught signal, canceling context", slog.String("signal", s.String()))
		cancel()
	}()

	switch parsed {
	case tsdbConvert.FullCommand():
		log.Info("Running convert")
		if err := tsdbConvertF(ctx, log, reg); err != nil {
			log.Error("Error converting tsdb block", slog.Any("err", err))
			os.Exit(1)
		}
	case serve.FullCommand():
		log.Info("Running serve")
		if err := serveF(ctx, log, reg); err != nil {
			log.Error("Error running serve", slog.Any("err", err))
			os.Exit(1)
		}
	}
	log.Info("Done")
}

func setupPrometheusRegistry() (*prometheus.Registry, error) {
	reg := prometheus.NewRegistry()
	registerer := prometheus.WrapRegistererWithPrefix("cf_metrics_", reg)

	if err := errors.Join(
		locate.RegisterMetrics(prometheus.WrapRegistererWithPrefix("locate_", registerer)),
		search.RegisterMetrics(prometheus.WrapRegistererWithPrefix("search_", registerer)),
		convert.RegisterMetrics(prometheus.WrapRegistererWithPrefix("convert_", registerer)),
		db.RegisterMetrics(prometheus.WrapRegistererWithPrefix("db_", registerer)),
		http.RegisterMetrics(prometheus.WrapRegistererWithPrefix("http_", registerer)),
		grpc.RegisterMetrics(prometheus.WrapRegistererWithPrefix("grpc_", registerer)),
	); err != nil {
		return nil, fmt.Errorf("unable to register metrics: %w", err)
	}
	return reg, nil
}
