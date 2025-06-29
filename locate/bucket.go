// Copyright (c) 2025 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

package locate

import (
	"context"
	"fmt"
	"io"

	"github.com/alecthomas/units"
	"github.com/thanos-io/objstore"
	"go.opentelemetry.io/otel/attribute"

	"github.com/cloudflare/parquet-tsdb-poc/internal/tracing"
)

type bucketReaderAt struct {
	ctx  context.Context
	bkt  objstore.Bucket
	name string
}

func newBucketReaderAt(ctx context.Context, bkt objstore.Bucket, name string) *bucketReaderAt {
	return &bucketReaderAt{
		ctx:  ctx,
		bkt:  bkt,
		name: name,
	}
}

func (br *bucketReaderAt) ReadAt(p []byte, off int64) (int, error) {
	ctx, span := tracing.Tracer().Start(br.ctx, "Bucket Get Range")
	defer span.End()

	span.SetAttributes(attribute.String("object", br.name))
	span.SetAttributes(attribute.Stringer("bytes", units.Base2Bytes(len(p)).Round(1)))
	span.SetAttributes(attribute.Int64("offset", off))

	bucketRequests.Inc()

	rdc, err := br.bkt.GetRange(ctx, br.name, off, int64(len(p)))
	if err != nil {
		return 0, fmt.Errorf("unable to read range for %s: %w", br.name, err)
	}
	defer rdc.Close()

	n, err := io.ReadFull(rdc, p)
	if n == len(p) {
		return n, nil
	}
	return n, err
}
