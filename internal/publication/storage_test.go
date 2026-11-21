package publication

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestGraceMillisRoundsUpWithStampPrecision(t *testing.T) {
	require.Equal(t, int64(0), GraceMillis(0))
	require.Equal(t, int64(0), GraceMillis(-time.Second))
	require.Equal(t, int64(2), GraceMillis(time.Nanosecond))
	require.Equal(t, int64(2), GraceMillis(time.Millisecond))
	require.Equal(t, int64(60_001), GraceMillis(60*time.Second))
	require.Equal(t, int64(60_002), GraceMillis(60*time.Second+time.Nanosecond))
	require.Equal(t, int64(math.MaxInt64/int64(time.Millisecond)+2), GraceMillis(time.Duration(math.MaxInt64)))
}

func TestStorageTargetNormalizeAndSame(t *testing.T) {
	valid := []struct {
		name string
		in   StorageTarget
		want StorageTarget
	}{
		{
			name: "lowercases scheme and host and trims layout prefix slashes",
			in:   StorageTarget{Endpoint: "HTTPS://Lake.S3.US-East-1.AmazonAWS.com/", Bucket: "lake", Prefix: "data///"},
			want: StorageTarget{Endpoint: "https://lake.s3.us-east-1.amazonaws.com", Bucket: "lake", Prefix: "data"},
		},
		{
			name: "preserves custom tenant path",
			in:   StorageTarget{Endpoint: "https://lake.s3.storage.example/tenant", Bucket: "lake", Prefix: "data"},
			want: StorageTarget{Endpoint: "https://lake.s3.storage.example/tenant", Bucket: "lake", Prefix: "data"},
		},
		{
			name: "preserves path-style bucket path",
			in:   StorageTarget{Endpoint: "https://s3.storage.example/tenant/lake", Bucket: "lake", Prefix: "data"},
			want: StorageTarget{Endpoint: "https://s3.storage.example/tenant/lake", Bucket: "lake", Prefix: "data"},
		},
		{
			name: "allows empty endpoint for non-S3 test targets",
			in:   StorageTarget{Endpoint: "", Bucket: "lake", Prefix: "data/"},
			want: StorageTarget{Endpoint: "", Bucket: "lake", Prefix: "data"},
		},
	}
	for _, tc := range valid {
		t.Run(tc.name, func(t *testing.T) {
			got, err := tc.in.Normalize()
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
			require.True(t, tc.in.Same(tc.want))
		})
	}

	// Distinct custom-endpoint paths must never compare equal (R15-F1).
	t1 := StorageTarget{Endpoint: "https://lake.s3.storage.example/tenant", Bucket: "lake", Prefix: "data"}
	t2 := StorageTarget{Endpoint: "https://lake.s3.storage.example/tenant/", Bucket: "lake", Prefix: "data"}
	t3 := StorageTarget{Endpoint: "https://lake.s3.storage.example/tenant//", Bucket: "lake", Prefix: "data"}
	require.False(t, t1.Same(t2))
	require.False(t, t1.Same(t3))
	require.False(t, t2.Same(t3))

	// Virtual-hosted vs path-style at the same endpoint never compare equal.
	vh := StorageTarget{Endpoint: "https://lake.s3.storage.example", Bucket: "lake", Prefix: "data"}
	ps := StorageTarget{Endpoint: "https://s3.storage.example/lake", Bucket: "lake", Prefix: "data"}
	require.False(t, vh.Same(ps))

	invalid := []struct {
		name string
		in   StorageTarget
	}{
		{"empty bucket", StorageTarget{Endpoint: "https://s3.us-east-1.amazonaws.com", Bucket: "", Prefix: "data"}},
		{"missing scheme", StorageTarget{Endpoint: "s3.storage.example/lake", Bucket: "lake", Prefix: "data"}},
		{"userinfo", StorageTarget{Endpoint: "https://user:pass@s3.storage.example/lake", Bucket: "lake", Prefix: "data"}},
		{"query", StorageTarget{Endpoint: "https://s3.storage.example/lake?x=1", Bucket: "lake", Prefix: "data"}},
		{"empty query", StorageTarget{Endpoint: "https://s3.storage.example/lake?", Bucket: "lake", Prefix: "data"}},
		{"fragment", StorageTarget{Endpoint: "https://s3.storage.example/lake#frag", Bucket: "lake", Prefix: "data"}},
		{"trailing slash on non-root path", StorageTarget{Endpoint: "https://lake.s3.storage.example/tenant/", Bucket: "lake", Prefix: "data"}},
		{"consecutive slashes in path", StorageTarget{Endpoint: "https://lake.s3.storage.example/tenant//", Bucket: "lake", Prefix: "data"}},
		{"consecutive slashes mid-path", StorageTarget{Endpoint: "https://s3.storage.example/a//lake", Bucket: "lake", Prefix: "data"}},
		{"dot segment", StorageTarget{Endpoint: "https://s3.storage.example/./lake", Bucket: "lake", Prefix: "data"}},
		{"parent segment", StorageTarget{Endpoint: "https://s3.storage.example/a/../lake", Bucket: "lake", Prefix: "data"}},
		{"escaped path", StorageTarget{Endpoint: "https://s3.storage.example/a%2Fb/lake", Bucket: "lake", Prefix: "data"}},
	}
	for _, tc := range invalid {
		t.Run("rejects "+tc.name, func(t *testing.T) {
			_, err := tc.in.Normalize()
			require.ErrorIs(t, err, ErrStorageTargetInvalid)
			require.False(t, tc.in.Same(tc.in), "an invalid target never compares equal even to itself")
		})
	}
}
