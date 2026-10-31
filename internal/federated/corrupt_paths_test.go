package federated

import (
	"testing"
	"time"
)

func TestCorruptParquetCacheCachedNamesUnexpired(t *testing.T) {
	base := time.Unix(1000, 0)
	c := newCorruptParquetCache(5 * time.Minute)
	c.now = func() time.Time { return base }
	c.Add([]string{"s3://b/bad.parquet"})

	cached := c.Cached([]string{"s3://b/good.parquet", "s3://b/bad.parquet"})
	if len(cached) != 1 || cached[0] != "s3://b/bad.parquet" {
		t.Fatalf("cached = %v", cached)
	}
}

func TestCorruptParquetCacheEntryExpires(t *testing.T) {
	base := time.Unix(1000, 0)
	c := newCorruptParquetCache(5 * time.Minute)
	c.now = func() time.Time { return base }
	c.Add([]string{"s3://b/bad.parquet"})

	c.now = func() time.Time { return base.Add(5*time.Minute + time.Second) }
	if cached := c.Cached([]string{"s3://b/bad.parquet"}); len(cached) != 0 {
		t.Fatalf("expired entry must be re-admitted: cached=%v", cached)
	}
	// The expired entry is dropped, not retained.
	if len(c.expires) != 0 {
		t.Fatalf("expired entry not evicted: %v", c.expires)
	}
}

func TestCorruptParquetCacheAddSweepsExpiredEntries(t *testing.T) {
	base := time.Unix(1000, 0)
	c := newCorruptParquetCache(5 * time.Minute)
	c.now = func() time.Time { return base }
	c.Add([]string{"s3://b/retired.parquet"})

	// The retired path never reappears in any Cached input (compaction removed
	// it from the manifest); a later Add of an unrelated path must still evict
	// its expired entry.
	c.now = func() time.Time { return base.Add(5*time.Minute + time.Second) }
	c.Add([]string{"s3://b/new.parquet"})
	if _, ok := c.expires["s3://b/retired.parquet"]; ok {
		t.Fatalf("expired never-rescanned entry not swept on Add: %v", c.expires)
	}
	if _, ok := c.expires["s3://b/new.parquet"]; !ok {
		t.Fatal("new entry missing after Add")
	}
}

func TestCorruptParquetCacheNilSafe(t *testing.T) {
	var c *corruptParquetCache
	c.Add([]string{"s3://b/x.parquet"}) // must not panic
	if cached := c.Cached([]string{"s3://b/x.parquet"}); cached != nil {
		t.Fatalf("nil cache must report nothing cached: %v", cached)
	}
}
