package compaction

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/manifestv2"
	"github.com/lychee-technology/forma/internal/telemetry"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

// metricRecorder is the per-test forma.MetricEmitter: it keeps every metric
// the compactor under test emits through its Metrics sink (#423).
type metricRecorder struct{ events []forma.Metric }

func (r *metricRecorder) EmitMetric(_ context.Context, m forma.Metric) {
	r.events = append(r.events, m)
}

// recordingSink returns a sink for a Compactor literal and the recorder
// behind it.
func recordingSink() (*telemetry.Sink, *metricRecorder) {
	rec := &metricRecorder{}
	return telemetry.NewSink(rec), rec
}

// objectReader is a cdc.S3GetClient over a map of keys to bytes, recording
// the keys it was asked for. read answers a key's bytes, from a store when
// it is set.
type objectReader struct {
	objects map[string][]byte
	read    func(key string) ([]byte, error)
	gets    []string
}

func (r *objectReader) GetObject(_ context.Context, in *s3.GetObjectInput, _ ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	key := *in.Key
	r.gets = append(r.gets, key)
	if r.read != nil {
		raw, err := r.read(key)
		if err != nil {
			return nil, err
		}
		return &s3.GetObjectOutput{Body: io.NopCloser(bytes.NewReader(raw))}, nil
	}
	raw, ok := r.objects[key]
	if !ok {
		return nil, errors.New("NoSuchKey")
	}
	return &s3.GetObjectOutput{Body: io.NopCloser(bytes.NewReader(raw))}, nil
}

var (
	sourcePayload  = []byte("source parquet bytes")
	corruptPayload = []byte("source parquet bytes, tampered")
)

const sourceKey = "data/1/e1/base/0190a000-0000-7000-8000-000000000001.parquet"

func listed(payload []byte) manifestv2.File {
	return manifestv2.File{Tier: manifestv2.TierBase, Key: sourceKey, SHA256: manifestv2.Digest(payload)}
}

// A source whose bytes still hash to the digest its entry lists passes the
// gate, and the gate read the bytes rather than trusting the manifest.
func TestVerifySourceChecksumsPassesAMatchingSource(t *testing.T) {
	r := &objectReader{objects: map[string][]byte{sourceKey: sourcePayload}}
	c := &Compactor{Bucket: "bkt", ObjectReader: r}
	require.NoError(t, c.verifySourceChecksums(context.Background(), 1, []manifestv2.File{listed(sourcePayload)}, false))
	require.Equal(t, []string{sourceKey}, r.gets)
}

// The #347 gate: bytes that no longer hash to the listed digest refuse the
// merge with an attributable error, one metric and one ERROR naming both
// digests.
func TestVerifySourceChecksumsRefusesMutatedBytes(t *testing.T) {
	sink, rec := recordingSink()
	core, logs := observer.New(zap.ErrorLevel)
	c := &Compactor{Bucket: "bkt", Logger: zap.New(core), Metrics: sink,
		ObjectReader: &objectReader{objects: map[string][]byte{sourceKey: corruptPayload}}}
	err := c.verifySourceChecksums(context.Background(), 1, []manifestv2.File{listed(sourcePayload)}, false)
	require.ErrorIs(t, err, ErrSourceChecksumMismatch)
	require.ErrorContains(t, err, sourceKey)
	require.ErrorContains(t, err, manifestv2.Digest(sourcePayload))
	require.ErrorContains(t, err, manifestv2.Digest(corruptPayload))
	require.Len(t, rec.events, 1)
	require.Equal(t, "parquet_checksum_mismatch_total", rec.events[0].Name)
	require.Len(t, logs.All(), 1)
}

// A failed read is infrastructure, not a corruption verdict.
func TestVerifySourceChecksumsProbeFailureIsNotAVerdict(t *testing.T) {
	c := &Compactor{Bucket: "bkt", ObjectReader: &objectReader{}}
	err := c.verifySourceChecksums(context.Background(), 1, []manifestv2.File{listed(sourcePayload)}, false)
	require.Error(t, err)
	require.NotErrorIs(t, err, ErrSourceChecksumMismatch)
}

// The opt-out and a compactor without a reader read nothing.
func TestVerifySourceChecksumsDisabledReadsNothing(t *testing.T) {
	r := &objectReader{}
	c := &Compactor{Bucket: "bkt", ObjectReader: r}
	require.NoError(t, c.verifySourceChecksums(context.Background(), 1, []manifestv2.File{listed(sourcePayload)}, true))
	require.Empty(t, r.gets)
	c.ObjectReader = nil
	require.NoError(t, c.verifySourceChecksums(context.Background(), 1, []manifestv2.File{listed(sourcePayload)}, false))
}
