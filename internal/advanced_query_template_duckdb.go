package internal

import "text/template"

var optimizedQuerySQLTemplateDuckDB = template.Must(template.New("optimizedQueryDuckDB").Funcs(template.FuncMap{
	"add":   func(a, b int) int { return a + b },
	"param": func(v any) string { return "?" },
	"ident": func(s string) string { return `"` + s + `"` },
}).Parse(`
        WITH
            {{- if .HasDirtyIDs }}
            dirty_ids AS (
                -- Injected dirty row id set (VALUES list of quoted literals)
                SELECT CAST(value AS VARCHAR) AS row_id FROM (VALUES {{.DirtyIDsCSV}}) AS v(value)
            ),
            {{- end }}
            anchor AS (
            {{- if .UseMainTableAsAnchor }}
            SELECT m.ltbase_row_id AS row_id
            FROM {{ident .MainTable}} m
            WHERE m.ltbase_schema_id = {{param .SchemaID}} AND {{.Anchor.Condition}}
            {{- else }}
            SELECT DISTINCT t.row_id
            FROM {{ident .EAVTable}} t
            WHERE t.schema_id = {{param .SchemaID}} AND {{.Anchor.Condition}}
            {{- end }}
            {{- if .ChangeLogTable }}
            UNION
            -- Include real-time buffer rows from change_log (flushed_at = 0)
            SELECT cl.row_id
            FROM {{ident .ChangeLogTable}} cl
            WHERE cl.schema_id = {{param .SchemaID}} AND cl.flushed_at = 0
            {{- end }}
        ),
        keys AS (
            SELECT
                a.row_id
                {{- if gt (len .SortKeys) 0 }}
                {{- range $i, $k := .SortKeys }}
                ,
                {{- if $k.IsMainColumn }}
                (
                    SELECT m.{{ident $k.MainColumnName}}
                    FROM {{ident $.MainTable}} m
                    WHERE m.ltbase_schema_id = {{param $.SchemaID}}
                        AND m.ltbase_row_id = a.row_id
                    LIMIT 1
                ) AS k{{$i}}
                {{- else }}
                (
                    SELECT d.{{ident $k.ValueColumn}}
                    FROM {{ident $.EAVTable}} d
                    WHERE d.schema_id = {{param $.SchemaID}}
                        AND d.row_id = a.row_id
                        AND d.attr_id = {{ $k.AttrIDInt }}
                    ORDER BY d.array_indices NULLS FIRST
                    LIMIT 1
                ) AS k{{$i}}
                {{- end }}
                {{- end }}
                {{- end }},
                COUNT(*) OVER() AS total
            FROM anchor a
        ),
        ordered AS (
            SELECT
                row_id
                {{- if gt (len .SortKeys) 0 }}
                {{- range $i, $_ := .SortKeys }}
                , k{{$i}}
                {{- end }}
                {{- end }},
                total
            FROM keys
            ORDER BY
                {{- if gt (len .SortKeys) 0 }}
                {{- range $i, $k := .SortKeys }}
                k{{$i}} {{ if $k.Desc }}DESC{{ else }}ASC{{ end }}{{ if lt (add $i 1) (len $.SortKeys) }},{{ end }}
                {{- end }}
                {{- if gt (len .SortKeys) 0 }},{{ end }}
                {{- end }}
                row_id
            LIMIT {{param .Limit}} OFFSET {{param .Offset}}
        ),
        main_data AS (
            SELECT 
                {{.MainProjection}},
                o.total
                {{- if gt (len .SortKeys) 0 }}
                {{- range $i, $_ := .SortKeys }}
                , o.k{{$i}}
                {{- end }}
                {{- end }}
            FROM ordered o
            INNER JOIN {{ident .MainTable}} m 
                ON m.ltbase_schema_id = {{param .SchemaID}} 
                AND m.ltbase_row_id = o.row_id
        ),
        eav_aggregated AS (
            SELECT 
                e.row_id,
                JSON_AGG(
                    JSON_BUILD_OBJECT(
                        'schema_id', e.schema_id,
                        'row_id', e.row_id,
                        'attr_id', e.attr_id,
                        'array_indices', e.array_indices,
                        'value_text', e.value_text,
                        'value_numeric', e.value_numeric
                    ) ORDER BY e.attr_id, e.array_indices
                )::TEXT AS attributes_json
            FROM ordered o
            INNER JOIN {{ident .EAVTable}} e 
                ON e.schema_id = {{param .SchemaID}} 
                AND e.row_id = o.row_id
            GROUP BY e.row_id
        )
        SELECT 
            {{.MainProjection}},
            COALESCE(e.attributes_json, '[]') AS attributes_json,
            m.total AS total_records,
            CEIL(m.total::numeric / NULLIF({{param .PageSize}}::numeric, 0)) AS total_pages,
            (FLOOR({{param .Offset}}::numeric / NULLIF({{param .Limit}}::numeric, 0)) + 1)::int AS current_page
        FROM main_data m
        LEFT JOIN eav_aggregated e ON e.row_id = m.ltbase_row_id
        ORDER BY
            {{- if gt (len .SortKeys) 0 }}
            {{- range $i, $k := .SortKeys }}
            m.k{{$i}} {{ if $k.Desc }}DESC{{ else }}ASC{{ end }}{{ if lt (add $i 1) (len $.SortKeys) }},{{ end }}
            {{- end }}
            {{- if gt (len .SortKeys) 0 }},{{ end }}
            {{- end }}
            m.ltbase_row_id;`))
