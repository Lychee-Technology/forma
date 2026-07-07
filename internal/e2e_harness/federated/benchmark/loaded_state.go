package benchmark

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"time"

	"github.com/google/uuid"
	federated "github.com/lychee-technology/forma/internal/e2e_harness/federated"
)

func buildLoadedStateSnapshot(ctx context.Context, h *federated.FederatedTestHarness, dataset *TieredDataset) ([]GeneratedRecord, map[string]struct{}, error) {
	if h == nil || dataset == nil {
		return nil, nil, fmt.Errorf("harness and dataset are required")
	}
	hotRecords, hotKeys, err := loadHotStateRecords(ctx, h)
	if err != nil {
		return nil, nil, err
	}
	records := make([]GeneratedRecord, 0, len(dataset.Base)+len(dataset.Delta)+len(hotRecords))
	for _, bucket := range [][]GeneratedRecord{dataset.Base, dataset.Delta} {
		for _, record := range bucket {
			if _, ok := hotKeys[schemaRowKey(record.SchemaID, record.RowID)]; ok {
				continue
			}
			records = append(records, cloneGeneratedRecord(record))
		}
	}
	records = append(records, hotRecords...)
	return records, hotKeys, nil
}

func loadHotStateRecords(ctx context.Context, h *federated.FederatedTestHarness) ([]GeneratedRecord, map[string]struct{}, error) {
	registry, err := LoadFixtureRegistry()
	if err != nil {
		return nil, nil, fmt.Errorf("load fixture registry: %w", err)
	}
	_, tradeCache, err := registry.GetSchemaAttributeCacheByID(SchemaIDTrade)
	if err != nil {
		return nil, nil, fmt.Errorf("load trade schema cache: %w", err)
	}
	tradeAttrID := func(name string) (int16, error) {
		meta, ok := tradeCache[name]
		if !ok {
			return 0, fmt.Errorf("trade attribute %s not found in fixture cache", name)
		}
		return meta.AttributeID, nil
	}
	symbolAttrID, err := tradeAttrID("symbol")
	if err != nil {
		return nil, nil, err
	}
	exchangeAttrID, err := tradeAttrID("exchange")
	if err != nil {
		return nil, nil, err
	}
	regionAttrID, err := tradeAttrID("region")
	if err != nil {
		return nil, nil, err
	}
	tradeTypeAttrID, err := tradeAttrID("tradeType")
	if err != nil {
		return nil, nil, err
	}
	tradeTimeAttrID, err := tradeAttrID("tradeTime")
	if err != nil {
		return nil, nil, err
	}
	rows, err := h.PGDB.QueryContext(ctx, `
		SELECT cl.schema_id, cl.row_id, cl.changed_at, COALESCE(cl.deleted_at, 0),
			em.text_01, em.text_02, em.smallint_01, em.bigint_02,
			hot_vals.symbol, hot_vals.exchange, hot_vals.region, hot_vals.trade_type, hot_vals.trade_time, hot_vals.name
		FROM change_log cl
		LEFT JOIN entity_main em
			ON em.ltbase_schema_id = cl.schema_id AND em.ltbase_row_id = cl.row_id
		LEFT JOIN (
			SELECT schema_id, row_id,
				MAX(CASE WHEN attr_id = $1 THEN value_text END) AS symbol,
				MAX(CASE WHEN attr_id = $2 THEN value_text END) AS exchange,
				MAX(CASE WHEN attr_id = $3 THEN value_text END) AS region,
				MAX(CASE WHEN attr_id = $4 THEN value_numeric END) AS trade_type,
				MAX(CASE WHEN attr_id = $5 THEN COALESCE(value_text, CAST(value_numeric AS TEXT)) END) AS trade_time,
				MAX(CASE WHEN attr_id = $6 THEN value_text END) AS name
			FROM eav_data
			GROUP BY schema_id, row_id
		) hot_vals ON hot_vals.schema_id = cl.schema_id AND hot_vals.row_id = cl.row_id
		WHERE cl.flushed_at = 0
	`,
		symbolAttrID,
		exchangeAttrID,
		regionAttrID,
		tradeTypeAttrID,
		tradeTimeAttrID,
		0,
	)
	if err != nil {
		return nil, nil, fmt.Errorf("load hot state snapshot: %w", err)
	}
	defer rows.Close()
	records := make([]GeneratedRecord, 0)
	keys := make(map[string]struct{})
	for rows.Next() {
		record, err := scanLoadedHotRecord(rows)
		if err != nil {
			return nil, nil, err
		}
		records = append(records, record)
		keys[schemaRowKey(record.SchemaID, record.RowID)] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		return nil, nil, fmt.Errorf("iterate hot state snapshot: %w", err)
	}
	return records, keys, nil
}

func scanLoadedHotRecord(rows *sql.Rows) (GeneratedRecord, error) {
	var schemaID int16
	var rowID uuid.UUID
	var changedAt int64
	var deletedAt int64
	var text01 sql.NullString
	var text02 sql.NullString
	var smallint01 sql.NullInt16
	var bigint02 sql.NullInt64
	var symbol sql.NullString
	var exchange sql.NullString
	var region sql.NullString
	var tradeType sql.NullFloat64
	var tradeTime any
	var name sql.NullString
	if err := rows.Scan(&schemaID, &rowID, &changedAt, &deletedAt, &text01, &text02, &smallint01, &bigint02, &symbol, &exchange, &region, &tradeType, &tradeTime, &name); err != nil {
		return GeneratedRecord{}, fmt.Errorf("scan hot state row: %w", err)
	}
	attrs := make(map[string]any)
	schemaName, err := schemaNameForID(schemaID)
	if err != nil {
		return GeneratedRecord{}, err
	}
	switch schemaID {
	case SchemaIDTrade:
		if symbol.Valid {
			attrs["symbol"] = symbol.String
		} else if text01.Valid {
			attrs["symbol"] = text01.String
		}
		if exchange.Valid {
			attrs["exchange"] = exchange.String
		}
		if region.Valid {
			attrs["region"] = region.String
		} else if text02.Valid {
			attrs["region"] = text02.String
		}
		if tradeType.Valid {
			attrs["tradeType"] = int64(tradeType.Float64)
		} else if smallint01.Valid {
			attrs["tradeType"] = int64(smallint01.Int16)
		}
		if normalizedTradeTime := normalizeBenchmarkTradeTime(tradeTime); normalizedTradeTime != "" {
			attrs["tradeTime"] = normalizedTradeTime
		} else if bigint02.Valid {
			attrs["tradeTime"] = strconv.FormatInt(bigint02.Int64, 10)
		}
		if name.Valid {
			attrs["name"] = name.String
		} else if symbol.Valid {
			attrs["name"] = symbol.String
		}
	case SchemaIDCustomer:
		if text02.Valid {
			attrs["region"] = text02.String
		}
		if name.Valid {
			attrs["name"] = name.String
		} else if text01.Valid {
			attrs["name"] = text01.String
		}
	case SchemaIDSecurity:
		if symbol.Valid {
			attrs["symbol"] = symbol.String
		} else if text01.Valid {
			attrs["symbol"] = text01.String
		}
		if name.Valid {
			attrs["companyName"] = name.String
		}
	}
	return GeneratedRecord{SchemaID: schemaID, SchemaName: schemaName, RowID: rowID, Version: 0, ChangedAt: changedAt, DeletedAt: deletedAt, Attributes: attrs}, nil
}

func normalizeBenchmarkTradeTime(value any) string {
	switch v := value.(type) {
	case time.Time:
		return strconv.FormatInt(v.UnixMilli(), 10)
	case sql.NullTime:
		if v.Valid {
			return strconv.FormatInt(v.Time.UnixMilli(), 10)
		}
	case string:
		if unixMillis, err := strconv.ParseInt(v, 10, 64); err == nil {
			return strconv.FormatInt(unixMillis, 10)
		}
		if parsed, err := time.Parse(time.RFC3339, v); err == nil {
			return strconv.FormatInt(parsed.UnixMilli(), 10)
		}
	case []byte:
		return normalizeBenchmarkTradeTime(string(v))
	}
	return ""
}

func schemaNameForID(schemaID int16) (string, error) {
	for _, fixture := range DefaultSchemaFixtures() {
		if fixture.ID == schemaID {
			return fixture.Name, nil
		}
	}
	return "", fmt.Errorf("unknown benchmark schema id %d", schemaID)
}
