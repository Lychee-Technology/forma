package cdcv2

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/manifestv2"
	"github.com/lychee-technology/forma/internal/publication"
)

// TestClassify pins what each failure of a publication transaction calls
// for. Only a COMMIT the server did not answer goes to Resolve: every other
// failure rolled the transaction back, or was the server's refusal.
func TestClassify(t *testing.T) {
	w := &Writer{}
	conflict := &pgconn.PgError{Code: "40001"}
	refused := &pgconn.PgError{Code: "23505"}
	live, cancelled := context.Background(), canceled()
	for _, tc := range []struct {
		name string
		ctx  context.Context
		err  error
		want verdict
	}{
		{"a commit the server refused with 40001", live, &commitError{conflict}, resend},
		{"a commit the server refused otherwise", live, &commitError{fmt.Errorf("commit: %w", refused)}, fail},
		{"a commit without an answer", live, &commitError{errors.New("connection reset")}, resolve},
		{"a commit past T_pub", live, &commitError{context.DeadlineExceeded}, resolve},
		{"a failpoint's lost commit", live, fmt.Errorf("failpoint commit: %w", ErrCommitAnswerLost), resolve},
		{"an expired staging", live, fmt.Errorf("publish: %w", publication.ErrStagingExpired), abandon},
		{"a moved record", live, fmt.Errorf("publish: %w", publication.ErrRecordMoved), rebase},
		{"a 40001 before commit", live, fmt.Errorf("mark: %w", conflict), resend},
		{"T_pub before commit", live, fmt.Errorf("publish: %w", context.DeadlineExceeded), resend},
		{"the caller's own deadline", cancelled, fmt.Errorf("publish: %w", context.DeadlineExceeded), fail},
		{"any other fault", live, errors.New("disk full"), fail},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, w.classify(tc.ctx, tc.err))
		})
	}
}

func canceled() context.Context {
	ctx, cancel := context.WithDeadline(context.Background(), time.Unix(0, 0))
	cancel()
	return ctx
}

// TestNewChecksAndDefaults: a writer needs its database, store, cache and
// tables, and a zero field takes its default.
func TestNewChecksAndDefaults(t *testing.T) {
	st := &manifestv2.FSStore{Dir: t.TempDir()}
	cache, err := manifestv2.NewCache(st, 1)
	require.NoError(t, err)
	db := &sql.DB{}
	full := Config{DB: db, Store: st, Cache: cache, EntityMain: "entity_main", ChangeLog: "change_log"}
	for _, broken := range []func(*Config){
		func(c *Config) { c.DB = nil },
		func(c *Config) { c.Store = nil },
		func(c *Config) { c.Cache = nil },
		func(c *Config) { c.EntityMain = "" },
		func(c *Config) { c.ChangeLog = "" },
	} {
		cfg := full
		broken(&cfg)
		_, err := New(cfg)
		require.Error(t, err)
	}
	w, err := New(full)
	require.NoError(t, err)
	require.Equal(t, DefaultPublishTimeout, w.cfg.PublishTimeout)
	require.Equal(t, DefaultStagingBudget, w.cfg.StagingBudget)
	require.Equal(t, DefaultAttempts, w.cfg.Attempts)
	require.Equal(t, DefaultBatchSize, w.cfg.BatchSize)
	require.Equal(t, DefaultBatchSize, w.generationLimit(), "a PostgreSQL generation is the whole batch")
	require.Equal(t, publication.TablesFor("entity_main"), w.tables)

	full.Engine = forma.EngineAuroraDSQL
	w, err = New(full)
	require.NoError(t, err)
	require.Equal(t, DSQLGenerationLimit, w.generationLimit())
	full.BatchSize = 300
	w, err = New(full)
	require.NoError(t, err)
	require.Equal(t, 300, w.generationLimit(), "a smaller batch is one generation on DSQL too")
}

// TestCommittedIsWhatPublishWouldAnswer: the record a Committed outcome
// stands for carries the expectation's epoch and the staged generation.
func TestCommittedIsWhatPublishWouldAnswer(t *testing.T) {
	next := publication.Next{Kind: publication.KindFlush, Token: uuid.New(), ManifestKey: "k", ManifestSHA256: "d", Lineage: uuid.New()}
	cur := publication.Record{SchemaID: 7, Epoch: 2, State: publication.Published, Generation: 4, Lineage: next.Lineage, Token: uuid.New(), UpdatedAt: 9}
	require.Equal(t, publication.Record{
		SchemaID: 7, Epoch: 2, State: publication.Published, Generation: 5, ParentGeneration: 4,
		ManifestKey: "k", ManifestSHA256: "d", Lineage: next.Lineage, Token: next.Token,
	}, committed(cur, next))
	cur.State, cur.Lineage = publication.Unpublished, uuid.Nil
	require.Zero(t, committed(cur, next).ParentGeneration, "an epoch's first generation has parent 0")
}
