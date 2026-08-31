package federated

import "fmt"

// Hot-tier SQL assembly for benchmark projections and targeted EAV filters.
// Split from query.go to keep execution/scanning separate from postgres_scan
// query construction (#220).

// hotCreationStampItem renders the hot leg's creation-stamp SELECT item. The
// hot leg must report the same quantity the parquet legs report in that slot
// (#460), which means reading entity_main.ltbase_created_at rather than
// aliasing cl.changed_at. COALESCE keeps fixtures that seed change_log without
// an entity_main row behaving exactly as before.
func hotCreationStampItem(withCreationStamp bool) string {
	if withCreationStamp {
		return "COALESCE(em.ltbase_created_at, cl.changed_at) as ltbase_created_at,\n\t\t\t"
	}
	return ""
}

func (h *FederatedTestHarness) buildHotTierQuery(pgConnStr string, schemaID int16, rowIDFilter, attributeFilter, timeWindowFilter string, benchmarkProjection, tradeTimeOnlyProjection, withCreationStamp bool) string {
	if benchmarkProjection {
		if tradeTimeOnlyProjection && schemaID == benchmarkSchemaIDTrade {
			return h.buildHotTradeTimeOnlyQuery(pgConnStr, schemaID, rowIDFilter, withCreationStamp)
		}
		return h.buildHotTierQueryTargeted(pgConnStr, schemaID, rowIDFilter, attributeFilter, timeWindowFilter, withCreationStamp)
	}
	// The narrow path keeps its change_log-only scan: entity_main is joined
	// only when the creation stamp is actually projected, so count queries
	// (withCreationStamp = false) stay exactly as narrow as before (#460).
	mainJoin := ""
	if withCreationStamp {
		mainJoin = fmt.Sprintf(`
		LEFT JOIN postgres_scan('%s', 'public', 'entity_main') em
			ON em.ltbase_schema_id = cl.schema_id AND em.ltbase_row_id::VARCHAR = cl.row_id::VARCHAR`, pgConnStr)
	}
	return fmt.Sprintf(`
		SELECT
			cl.row_id::VARCHAR as row_id,
			cl.schema_id,
			%s'' as name,
			0 as version,
			'hot' as tier
		FROM postgres_scan('%s', 'public', 'change_log') cl%s
		WHERE cl.flushed_at = 0
			AND cl.schema_id = %d
			%s
			%s`, hotCreationStampItem(withCreationStamp)+"cl.changed_at,\n\t\t\tcl.deleted_at,\n\t\t\t", pgConnStr, mainJoin, schemaID, rowIDFilter, timeWindowFilter)
}

func buildHotTierQuery(pgConnStr string, schemaID int16, rowIDFilter, attributeFilter, timeWindowFilter string, benchmarkProjection, tradeTimeOnlyProjection, withCreationStamp bool) string {
	return (*FederatedTestHarness)(nil).buildHotTierQuery(pgConnStr, schemaID, rowIDFilter, attributeFilter, timeWindowFilter, benchmarkProjection, tradeTimeOnlyProjection, withCreationStamp)
}

func (h *FederatedTestHarness) buildHotTradeTimeOnlyQuery(pgConnStr string, schemaID int16, rowIDFilter string, withCreationStamp bool) string {
	tradeTimeAttrID := h.benchmarkAttributeID(schemaID, "tradeTime")
	return fmt.Sprintf(`
		SELECT
			cl.row_id::VARCHAR as row_id,
			cl.schema_id,
			%scl.changed_at,
			cl.deleted_at,
			'' as name,
			0 as version,
			'' as symbol,
			'' as exchange,
			'' as region,
			0 as tradeType,
			COALESCE(hot_vals.trade_time, em.bigint_02, 0) as tradeTime,
			'hot' as tier
		FROM postgres_scan('%s', 'public', 'change_log') cl
		LEFT JOIN postgres_scan('%s', 'public', 'entity_main') em
			ON em.ltbase_schema_id = cl.schema_id AND em.ltbase_row_id::VARCHAR = cl.row_id::VARCHAR
		LEFT JOIN (
			SELECT row_id::VARCHAR as row_id, schema_id,
				MAX(CASE WHEN attr_id = %d THEN value_numeric::BIGINT END) AS trade_time
			FROM postgres_scan('%s', 'public', 'eav_data')
			WHERE attr_id = %d
			GROUP BY schema_id, row_id
		) hot_vals ON hot_vals.schema_id = cl.schema_id AND hot_vals.row_id = cl.row_id::VARCHAR
		WHERE cl.flushed_at = 0
			AND cl.schema_id = %d
			%s`, hotCreationStampItem(withCreationStamp), pgConnStr, pgConnStr, tradeTimeAttrID, pgConnStr, tradeTimeAttrID, schemaID, rowIDFilter)
}

type hotTierEAVMapping struct {
	attrIDList   string
	pivotColumns string
	selectExprs  string
	nameExpr     string
}

func (h *FederatedTestHarness) benchmarkAttributeID(schemaID int16, name string) int {
	if h != nil && h.Registry != nil {
		if _, cache, err := h.Registry.GetSchemaAttributeCacheByID(schemaID); err == nil && cache != nil {
			if metadata, ok := cache[name]; ok {
				return int(metadata.AttributeID)
			}
		}
	}
	hash := uint32(2166136261)
	input := fmt.Sprintf("%d:%s", schemaID, name)
	for i := 0; i < len(input); i++ {
		hash ^= uint32(input[i])
		hash *= 16777619
	}
	return int(hash%30000) + 1
}

func (h *FederatedTestHarness) hotTierEAVMappingForSchema(schemaID int16) hotTierEAVMapping {
	switch schemaID {
	case benchmarkSchemaIDTrade:
		symbolID := h.benchmarkAttributeID(schemaID, "symbol")
		exchangeID := h.benchmarkAttributeID(schemaID, "exchange")
		regionID := h.benchmarkAttributeID(schemaID, "region")
		tradeTypeID := h.benchmarkAttributeID(schemaID, "tradeType")
		tradeTimeID := h.benchmarkAttributeID(schemaID, "tradeTime")
		return hotTierEAVMapping{
			attrIDList: fmt.Sprintf("%d, %d, %d, %d, %d", symbolID, exchangeID, regionID, tradeTypeID, tradeTimeID),
			pivotColumns: fmt.Sprintf(
				"MAX(CASE WHEN attr_id = %d THEN value_text END) AS symbol,\n\t\t\t"+
					"MAX(CASE WHEN attr_id = %d THEN value_text END) AS exchange,\n\t\t\t"+
					"MAX(CASE WHEN attr_id = %d THEN value_text END) AS region,\n\t\t\t"+
					"MAX(CASE WHEN attr_id = %d THEN value_numeric::BIGINT END) AS tradeType,\n\t\t\t"+
					"MAX(CASE WHEN attr_id = %d THEN value_numeric::BIGINT END) AS tradeTime",
				symbolID, exchangeID, regionID, tradeTypeID, tradeTimeID),
			selectExprs: "COALESCE(hot_vals.symbol, em.text_01) as symbol,\n\t\t\t" +
				"COALESCE(hot_vals.exchange, '') as exchange,\n\t\t\t" +
				"COALESCE(hot_vals.region, em.text_02) as region,\n\t\t\t" +
				"COALESCE(hot_vals.tradeType, em.smallint_01) as tradeType,\n\t\t\t" +
				"COALESCE(hot_vals.tradeTime, em.bigint_02) as tradeTime",
			nameExpr: "COALESCE(hot_vals.symbol, em.text_01, '')",
		}
	case benchmarkSchemaIDCustomer:
		regionID := h.benchmarkAttributeID(schemaID, "region")
		nameID := h.benchmarkAttributeID(schemaID, "name")
		return hotTierEAVMapping{
			attrIDList: fmt.Sprintf("%d, %d", regionID, nameID),
			pivotColumns: fmt.Sprintf(
				"MAX(CASE WHEN attr_id = %d THEN value_text END) AS region,\n\t\t\t"+
					"MAX(CASE WHEN attr_id = %d THEN value_text END) AS name",
				regionID, nameID),
			selectExprs: "'' as symbol,\n\t\t\t" +
				"'' as exchange,\n\t\t\t" +
				"COALESCE(hot_vals.region, em.text_02) as region,\n\t\t\t" +
				"0 as tradeType,\n\t\t\t" +
				"0 as tradeTime",
			nameExpr: "COALESCE(hot_vals.name, '')",
		}
	case benchmarkSchemaIDSecurity:
		symbolID := h.benchmarkAttributeID(schemaID, "symbol")
		nameID := h.benchmarkAttributeID(schemaID, "companyName")
		return hotTierEAVMapping{
			attrIDList: fmt.Sprintf("%d, %d", symbolID, nameID),
			pivotColumns: fmt.Sprintf(
				"MAX(CASE WHEN attr_id = %d THEN value_text END) AS symbol,\n\t\t\t"+
					"MAX(CASE WHEN attr_id = %d THEN value_text END) AS name",
				symbolID, nameID),
			selectExprs: "COALESCE(hot_vals.symbol, em.text_01) as symbol,\n\t\t\t" +
				"'' as exchange,\n\t\t\t" +
				"'' as region,\n\t\t\t" +
				"0 as tradeType,\n\t\t\t" +
				"0 as tradeTime",
			nameExpr: "COALESCE(hot_vals.name, hot_vals.symbol, '')",
		}
	default:
		return hotTierEAVMapping{}
	}
}

func hotTierEAVMappingForSchema(schemaID int16) hotTierEAVMapping {
	return (*FederatedTestHarness)(nil).hotTierEAVMappingForSchema(schemaID)
}

func (h *FederatedTestHarness) buildHotTierQueryTargeted(pgConnStr string, schemaID int16, rowIDFilter, attributeFilter, timeWindowFilter string, withCreationStamp bool) string {
	m := h.hotTierEAVMappingForSchema(schemaID)
	return fmt.Sprintf(`
		SELECT
			cl.row_id::VARCHAR as row_id,
			cl.schema_id,
			%scl.changed_at,
			cl.deleted_at,
			%s as name,
			0 as version,
			%s,
			'hot' as tier
		FROM postgres_scan('%s', 'public', 'change_log') cl
		LEFT JOIN postgres_scan('%s', 'public', 'entity_main') em
			ON em.ltbase_schema_id = cl.schema_id AND em.ltbase_row_id::VARCHAR = cl.row_id::VARCHAR
		LEFT JOIN (
			SELECT row_id::VARCHAR as row_id, schema_id,
				%s
			FROM postgres_scan('%s', 'public', 'eav_data')
			WHERE attr_id IN (%s)
			GROUP BY schema_id, row_id
		) hot_vals ON hot_vals.schema_id = cl.schema_id AND hot_vals.row_id = cl.row_id::VARCHAR
		WHERE cl.flushed_at = 0
			AND cl.schema_id = %d
			%s
			%s
			%s`,
		hotCreationStampItem(withCreationStamp),
		m.nameExpr, m.selectExprs,
		pgConnStr, pgConnStr,
		m.pivotColumns, pgConnStr, m.attrIDList,
		schemaID, rowIDFilter, attributeFilter, timeWindowFilter)
}

func buildHotTierQueryTargeted(pgConnStr string, schemaID int16, rowIDFilter, attributeFilter, timeWindowFilter string, withCreationStamp bool) string {
	return (*FederatedTestHarness)(nil).buildHotTierQueryTargeted(pgConnStr, schemaID, rowIDFilter, attributeFilter, timeWindowFilter, withCreationStamp)
}
