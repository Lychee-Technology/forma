//go:build e2e

package production

import (
	"context"
	"strings"
	"testing"

	"github.com/lychee-technology/forma/internal"
)

// TestLegacyTextColumnsNeedADuplicateCIndex is the side of #816 that #815
// keeps: a table created before the contract, whose text columns follow the
// database default, cannot seek a text range on its ordinary indexes, and
// needs a second, COLLATE "C" key per index for that. Equality and in still
// seek the ordinary index, so a legacy table carries both, which is the
// storage and write cost a column under C avoids. The test measures both on
// the same rows: the size of each ordinary index and its C twin, and the WAL
// an insert of 5,000 rows writes before and after the twins exist.
func TestLegacyTextColumnsNeedADuplicateCIndex(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	env := NewEnv(t, SharedCluster(t))
	requireNonCDatabaseDefault(ctx, t, env)
	seedCollationIndexes(ctx, t, env)
	compliantWAL := insertMainRows(ctx, t, env, SchemaIDSecond, 1, 5_000) + insertEAVRows(ctx, t, env, SchemaIDSecond, 1, 5_000, "note")
	// The measured rows would skew value_text's statistics for the probes
	// (see loadEAVAttribute).
	env.ExecSQL(ctx, `DELETE FROM eav_data WHERE schema_id = $1`, SchemaIDSecond)

	// A table from before #816: the ALTER moves the columns to the database
	// default and rebuilds every index on them under it.
	env.ExecSQL(ctx, `ALTER TABLE entity_main ALTER COLUMN text_01 TYPE text COLLATE "default"`)
	env.ExecSQL(ctx, `ALTER TABLE eav_data ALTER COLUMN value_text TYPE text COLLATE "default"`)
	analyzeByteOrderTables(ctx, env)

	recorder := &recordingPool{Pool: env.Pool}
	repo := internal.NewDBPersistentRecordRepository(recorder, env.Metadata)
	probes := collationProbes(t, env)
	loaded := ""
	for _, p := range probes {
		loaded = loadEAVAttribute(ctx, t, env, loaded, p.eav)
		_, seeks := runCollationProbe(ctx, t, env, repo, recorder, p, false)
		if wantSeek := !isTextRangeProbe(p); seeks != wantSeek {
			t.Errorf("%s under the database default: seeks %s on %v = %v, want %v", p.name, p.index, p.columns, seeks, wantSeek)
		}
	}

	twins := map[string]string{
		declaredIndexFor(t, env, "e2e_wide.by_title").Name:      `CREATE INDEX by_title_c ON entity_main (ltbase_schema_id, text_01 COLLATE "C", ltbase_row_id)`,
		declaredIndexFor(t, env, "e2e_wide.by_rank_title").Name: `CREATE INDEX by_rank_title_c ON entity_main (ltbase_schema_id, smallint_01, text_01 COLLATE "C", ltbase_row_id)`,
		"eav_data_text_idx": `CREATE INDEX eav_data_text_c_idx ON eav_data (schema_id, attr_id, value_text COLLATE "C", row_id) WHERE value_text IS NOT NULL`,
	}
	twinName := map[string]string{}
	for plain, ddl := range twins {
		env.ExecSQL(ctx, ddl)
		twinName[plain] = strings.Fields(ddl)[2]
	}
	// Rebuilt together, each pair indexes the same rows from scratch, so the
	// sizes compare what each key costs, not what reloading eav_data left.
	env.ExecSQL(ctx, `REINDEX TABLE entity_main`)
	env.ExecSQL(ctx, `REINDEX TABLE eav_data`)
	for plain, twin := range twinName {
		t.Logf("index %s: %d bytes; its C twin %s: %d bytes", plain, indexBytes(ctx, t, env, plain), twin, indexBytes(ctx, t, env, twin))
	}
	analyzeByteOrderTables(ctx, env)
	for _, p := range probes {
		if !isTextRangeProbe(p) {
			continue
		}
		loaded = loadEAVAttribute(ctx, t, env, loaded, p.eav)
		p.index = twinName[p.index]
		if plan, seeks := runCollationProbe(ctx, t, env, repo, recorder, p, false); !seeks {
			t.Errorf("%s does not seek the C twin %s; plan:\n%s", p.name, p.index, describePlan(plan, 0))
		}
	}

	legacyWAL := insertMainRows(ctx, t, env, SchemaIDSecond, 5_001, 10_000) + insertEAVRows(ctx, t, env, SchemaIDSecond, 5_001, 10_000, "note")
	t.Logf("WAL of inserting 5,000 rows: %d bytes with one index per text key (columns under C), %d bytes with a C twin beside each", compliantWAL, legacyWAL)
	if legacyWAL <= compliantWAL {
		t.Errorf("the C twins cost no write: %d WAL bytes with them, %d without", legacyWAL, compliantWAL)
	}
}

func isTextRangeProbe(p collationProbe) bool {
	return !strings.HasSuffix(p.name, "/equals") && !strings.HasSuffix(p.name, "/in")
}

// indexBytes answers the on-disk size of index.
func indexBytes(ctx context.Context, t *testing.T, env *Env, index string) int64 {
	t.Helper()
	var size int64
	if err := env.Pool.QueryRow(ctx, `SELECT pg_relation_size(to_regclass($1))`, index).Scan(&size); err != nil {
		t.Fatalf("read the size of %s: %v", index, err)
	}
	return size
}
