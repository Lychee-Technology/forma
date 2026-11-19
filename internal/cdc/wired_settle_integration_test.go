package cdc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/lychee-technology/forma/internal/cdcv2"
	"github.com/lychee-technology/forma/internal/manifestv2"
	"github.com/lychee-technology/forma/internal/publication"
)

// TestWiredFlushFoundCommittedBySettlingKeepsItsMarkCounts: every COMMIT of
// the flush goes without an answer, and the writer's own fence fails, so it
// stops with ErrUnresolved. Meanwhile the lost COMMIT lands. The tools'
// in-process settling finds it committed: the run reports the generation and
// the two entries its marks flushed, not zero, and leaves none dirty.
func TestWiredFlushFoundCommittedBySettlingKeepsItsMarkCounts(t *testing.T) {
	f := newWiredFixture(t)
	f.register(t, publication.Unpublished)
	f.seed(t, 2, time.Now().UnixMilli()-60_000)

	var fences atomic.Int32
	landed := make(chan error, 1)
	lateCommit := func(c *CDCConfig) {
		c.PublishFailpoint = func(_ context.Context, p string) error {
			switch {
			case p == string(cdcv2.PointCommit):
				return cdcv2.ErrCommitAnswerLost
			case p == string(cdcv2.PointFence) && fences.Add(1) == 1:
				// The writer's own fence fails, and the lost COMMIT lands
				// while it does: only the settling can tell.
				landed <- f.landStagedFlush(context.Background())
				return errWiredFault
			}
			return nil
		}
	}

	rep, err := f.flush(t, nil, false, lateCommit)
	require.NoError(t, err)
	require.NoError(t, <-landed)
	rec := mustRecord(t, f)
	require.Equal(t, publication.Published, rec.State)
	require.Len(t, rep.Flushed, 1)
	got := rep.Flushed[0]
	require.Equal(t, []int64{rec.Generation}, got.Generations)
	require.Equal(t, 2, got.Selected)
	require.Equal(t, 2, got.Marked, "the marks committed with the generation the settling found")
	require.Zero(t, got.Unconfirmed, "every unanswered COMMIT counted the same two marks")
	require.Empty(t, f.dirty(t))
	require.Equal(t, int32(2), fences.Load(), "the writer's fence, then the settling's")
}

// landStagedFlush commits the one staged flush manifest of the generation
// after the record that no publication committed, and marks every unflushed
// entry of the schema with it, as the writer's lost COMMIT would have.
func (f *wiredFixture) landStagedFlush(ctx context.Context) error {
	snap, err := publication.Read(ctx, publication.DB(f.db), f.tables, wiredSchema)
	if err != nil {
		return fmt.Errorf("read the record: %w", err)
	}
	cur := snap.Record
	key, raw, m, err := f.stagedFlush(cur)
	if err != nil {
		return err
	}
	tx, err := f.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin the late commit: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	out, err := publication.Publish(ctx, publication.PgxTx(tx), f.tables, cur, publication.Next{
		Kind: m.Kind, Token: m.Token, ManifestKey: key, ManifestSHA256: manifestv2.Digest(raw),
		Lineage: m.Lineage, StagedAt: snap.ReadAt, StagingBudget: time.Minute,
	})
	if err != nil {
		return fmt.Errorf("publish the staged generation: %w", err)
	}
	if _, err := tx.Exec(ctx, "UPDATE "+sanitizeIdentifier(f.cfg.ChangeLogTable)+
		" SET flushed_at = $2 WHERE schema_id = $1 AND flushed_at = 0", wiredSchema, out.UpdatedAt); err != nil {
		return fmt.Errorf("mark the staged generation's entries: %w", err)
	}
	return tx.Commit(ctx)
}

// stagedFlush answers the key, bytes and document of the one flush manifest
// staged for the generation after cur.
func (f *wiredFixture) stagedFlush(cur publication.Record) (string, []byte, manifestv2.Manifest, error) {
	var found []string
	err := filepath.WalkDir(f.store.Dir, func(path string, d fs.DirEntry, err error) error {
		if err == nil && !d.IsDir() && strings.Contains(path, "_manifests") && strings.HasSuffix(path, ".json") {
			found = append(found, path)
		}
		return err
	})
	if err != nil {
		return "", nil, manifestv2.Manifest{}, fmt.Errorf("list the staged manifests: %w", err)
	}
	for _, path := range found {
		raw, err := os.ReadFile(path)
		if err != nil {
			return "", nil, manifestv2.Manifest{}, fmt.Errorf("read %s: %w", path, err)
		}
		var m manifestv2.Manifest
		if err := json.Unmarshal(raw, &m); err != nil {
			return "", nil, manifestv2.Manifest{}, fmt.Errorf("decode %s: %w", path, err)
		}
		if m.Kind == publication.KindFlush && m.Generation == cur.Generation+1 {
			key, err := filepath.Rel(f.store.Dir, path)
			return filepath.ToSlash(key), raw, m, err
		}
	}
	return "", nil, manifestv2.Manifest{}, errors.New("no flush manifest is staged for the next generation")
}
