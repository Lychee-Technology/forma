package federated

import (
	"testing"
	"time"
)

func TestCorruptParquetCacheSplitExcludesUnexpired(t *testing.T) {
	base := time.Unix(1000, 0)
	c := newCorruptParquetCache(5 * time.Minute)
	c.now = func() time.Time { return base }
	c.Add([]string{"s3://b/bad.parquet"})

	kept, excluded := c.Split([]string{"s3://b/good.parquet", "s3://b/bad.parquet"})
	if len(kept) != 1 || kept[0] != "s3://b/good.parquet" {
		t.Fatalf("kept = %v", kept)
	}
	if len(excluded) != 1 || excluded[0] != "s3://b/bad.parquet" {
		t.Fatalf("excluded = %v", excluded)
	}
}

func TestCorruptParquetCacheEntryExpires(t *testing.T) {
	base := time.Unix(1000, 0)
	c := newCorruptParquetCache(5 * time.Minute)
	c.now = func() time.Time { return base }
	c.Add([]string{"s3://b/bad.parquet"})

	c.now = func() time.Time { return base.Add(5*time.Minute + time.Second) }
	kept, excluded := c.Split([]string{"s3://b/bad.parquet"})
	if len(kept) != 1 || len(excluded) != 0 {
		t.Fatalf("expired entry must be re-admitted: kept=%v excluded=%v", kept, excluded)
	}
	// The expired entry is dropped, not retained.
	if len(c.expires) != 0 {
		t.Fatalf("expired entry not evicted: %v", c.expires)
	}
}

func TestCorruptParquetCacheNilSafe(t *testing.T) {
	var c *corruptParquetCache
	c.Add([]string{"s3://b/x.parquet"}) // must not panic
	kept, excluded := c.Split([]string{"s3://b/x.parquet"})
	if len(kept) != 1 || excluded != nil {
		t.Fatalf("nil cache must pass paths through: kept=%v excluded=%v", kept, excluded)
	}
}
