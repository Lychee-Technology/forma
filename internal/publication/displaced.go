package publication

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma/internal/sqlgen"
	"github.com/lychee-technology/forma/internal/textcollation"
)

// displacedDDL keeps the inventory a replaced generation's history row named
// when the transition that replaced the generation found the row disagreeing
// with the record, and wrote the row back from the record (supersede). One
// of the two was restored or written outside the protocol, and nothing tells
// which: a reader that chose the generation while the record still named
// what the row names scans the row's manifest, and one that chose it after
// scans the record's. The row keeps the record's inventory; this table keeps
// the other one, as evidence that garbage collection may delete nothing for
// the schema while the replaced generation's row is retained (Displacement).
// The primary key holds one row per manifest a generation's row named, so a
// second write of the same displacement changes nothing.
const displacedDDL = `CREATE TABLE IF NOT EXISTS %s (
		schema_id       SMALLINT NOT NULL,
		generation      BIGINT   NOT NULL,
		epoch           BIGINT   NOT NULL,
		manifest_key    ` + textcollation.TextType + ` NOT NULL,
		manifest_sha256 ` + textcollation.TextType + ` NOT NULL,
		cas_token       UUID     NOT NULL,
		displaced_at    BIGINT   NOT NULL,
		PRIMARY KEY (schema_id, generation, manifest_key)
	)`

// Displacement is an inventory a replaced generation's history row named
// before the replacing transition wrote it back from the record. A reader
// pinned to the generation may still scan it: the generation was chosen
// through the record, and the record may have named it before it was
// changed. It is retired with the replaced generation's row: while that row
// is retained, which it is until a collector retired it after seeing the
// replacement committed and the retention grace passed, garbage collection
// deletes nothing for the schema (internal/reconcile). After that no reader
// that chose the generation, under either inventory, is still inside its
// pass deadline: a collector then deletes what only the displaced inventory
// lists and its manifest (Collection.Displaced), each only once it is below
// the orphan cutoff, since the inventory proves which keys the row named
// and not that no publication can still commit one (Plan.Deferred), and
// forgets the displacement only after that, so that a pass that stops
// before then finds the inventory named again.
type Displacement struct {
	Generation     int64
	Epoch          int64
	ManifestKey    string
	ManifestSHA256 string
	Token          uuid.UUID
	// DisplacedAt is the start time of the replacing transaction.
	DisplacedAt int64
}

const (
	// historyRowSQL reads the replaced generation's row and locks it, so
	// that what displace compares is what supersede then overwrites.
	historyRowSQL = `SELECT epoch, manifest_key, manifest_sha256, cas_token FROM %s
	WHERE schema_id = $1 AND generation = $2
	FOR UPDATE`
	displaceSQL = `INSERT INTO %s (schema_id, generation, epoch, manifest_key, manifest_sha256, cas_token, displaced_at)
	VALUES ($1, $2, $3, $4, $5, $6, ` + sqlgen.ServerTimeMillisSQL + `)
	ON CONFLICT (schema_id, generation, manifest_key) DO NOTHING`
	displacedSQL = `SELECT generation, epoch, manifest_key, manifest_sha256, cas_token, displaced_at FROM %s
	WHERE schema_id = $1
	ORDER BY generation, manifest_key`
	// forgetDisplacedSQL deletes a generation's displacements only once
	// its history row is no longer retained: expired, or forgotten after
	// it expired.
	forgetDisplacedSQL = `DELETE FROM %s
	WHERE schema_id = $1 AND generation = $2
	  AND NOT EXISTS (SELECT 1 FROM %s g WHERE g.schema_id = $1 AND g.generation = $2
	    AND (g.retired_at = 0 OR g.retired_at > ` + sqlgen.ServerTimeMillisSQL + ` - $3))`
)

// Inventory answers the displaced inventory as the generation its history
// row named: what a collector verifies its manifest against
// (ManifestFiles) before it trusts a key the manifest lists.
func (d Displacement) Inventory() Generation {
	return Generation{Generation: d.Generation, Epoch: d.Epoch, ManifestKey: d.ManifestKey,
		ManifestSHA256: d.ManifestSHA256, Token: d.Token}
}

// displace records, on r, the inventory cur's generation's history row
// names when it is not the one cur names: another epoch, manifest key,
// digest or token. It runs in the transaction that replaces cur, before
// supersede writes the row back from cur, so the row's inventory and the
// record's both stay named. A row that agrees, or no row (a lost history
// row, which the next collection reports), displaces nothing.
func displace(ctx context.Context, r runner, t Tables, cur Record) error {
	var row Displacement
	err := r.queryRow(ctx, fmt.Sprintf(historyRowSQL, t.Generation), cur.SchemaID, cur.Generation).
		Scan(&row.Epoch, &row.ManifestKey, &row.ManifestSHA256, &row.Token)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("read schema %d generation %d's history row: %w", cur.SchemaID, cur.Generation, err)
	}
	if row.Epoch == cur.Epoch && row.ManifestKey == cur.ManifestKey && row.ManifestSHA256 == cur.ManifestSHA256 && row.Token == cur.Token {
		return nil
	}
	if _, err := r.exec(ctx, fmt.Sprintf(displaceSQL, t.Displaced), cur.SchemaID, cur.Generation,
		row.Epoch, row.ManifestKey, row.ManifestSHA256, row.Token); err != nil {
		return fmt.Errorf("keep the inventory %s that schema %d generation %d's history row names, which disagrees with the record's %s, in %s "+
			"(init-db creates the table): %w", row.ManifestKey, cur.SchemaID, cur.Generation, cur.ManifestKey, t.Displaced, err)
	}
	return nil
}

// Displaced answers the schema's displacements, by generation. A collector
// reads them in the snapshot it reads the history in.
func Displaced(ctx context.Context, q Querier, t Tables, schemaID int16) ([]Displacement, error) {
	rows, err := q.runner().query(ctx, fmt.Sprintf(displacedSQL, t.Displaced), schemaID)
	if err != nil {
		return nil, fmt.Errorf("read schema %d's displaced inventories in %s (init-db creates the table): %w", schemaID, t.Displaced, err)
	}
	defer rows.Close()
	var out []Displacement
	for rows.Next() {
		var d Displacement
		if err := rows.Scan(&d.Generation, &d.Epoch, &d.ManifestKey, &d.ManifestSHA256, &d.Token, &d.DisplacedAt); err != nil {
			return nil, fmt.Errorf("scan schema %d's displaced inventory: %w", schemaID, err)
		}
		out = append(out, d)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read schema %d's displaced inventories: %w", schemaID, err)
	}
	return out, nil
}

// ForgetDisplaced deletes generation's displacements once its history row
// is no longer retained under grace (Retained), and answers how many it
// deleted: none while the row is retained. A collector runs it only after
// it deleted the displaced manifest and the objects only it listed: the
// displacement is what names them.
func ForgetDisplaced(ctx context.Context, q Querier, t Tables, schemaID int16, generation int64, grace time.Duration) (int64, error) {
	n, err := q.runner().exec(ctx, fmt.Sprintf(forgetDisplacedSQL, t.Displaced, t.Generation), schemaID, generation, GraceMillis(grace))
	if err != nil {
		return 0, fmt.Errorf("forget schema %d generation %d's displaced inventories: %w", schemaID, generation, err)
	}
	return n, nil
}
