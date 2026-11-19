package publication

import (
	"context"
	"fmt"
)

// epochsSQL reads the epoch of every record.
const epochsSQL = `SELECT schema_id, epoch FROM %s`

// Epochs answers the epoch of every schema's record on q, by schema id; a
// schema without a record has no entry. Epochs only grow, and every drop and
// every registration of a dropped id moves a record to its next epoch, so
// two readings that agree on a schema's epoch prove that no drop or
// re-registration of it committed between them. A process reads them before
// it loads schema metadata, and requires the epoch its writes or reads pin
// afterwards to be the same: metadata loaded in between belongs to that
// epoch, and never to a schema the record no longer belongs to.
func Epochs(ctx context.Context, q Querier, t Tables) (map[int16]int64, error) {
	rows, err := q.runner().query(ctx, fmt.Sprintf(epochsSQL, t.Record))
	if err != nil {
		return nil, fmt.Errorf("read the publication epochs in %s: %w", t.Record, err)
	}
	defer rows.Close()
	out := map[int16]int64{}
	for rows.Next() {
		var id int16
		var epoch int64
		if err := rows.Scan(&id, &epoch); err != nil {
			return nil, fmt.Errorf("read the publication epochs in %s: %w", t.Record, err)
		}
		out[id] = epoch
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read the publication epochs in %s: %w", t.Record, err)
	}
	return out, nil
}
