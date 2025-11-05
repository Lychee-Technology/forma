package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math"
	"math/rand"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"lychee.technology/ltbase/forma/internal"
)

type options struct {
	host         string
	port         int
	database     string
	user         string
	password     string
	sslMode      string
	schemaDir    string
	schemaTable  string
	eavTable     string
	purge        bool
	leadCount    int
	listingCount int
	chunkSize    int
	seed         int64
	seedProvided bool
}

type AttributeMetadata struct {
	AttrID      int16  `json:"attributeID"`
	ValueType   string `json:"valueType"`
	InsideArray bool   `json:"insideArray"`
}

func main() {
	log.SetFlags(0)

	opts := parseFlags()
	ctx := context.Background()

	connString := buildConnString(opts)
	pool, err := pgxpool.New(ctx, connString)
	if err != nil {
		log.Fatalf("failed to create connection pool: %v", err)
	}
	defer pool.Close()

	registry, err := internal.NewFileSchemaRegistry(opts.schemaDir)
	if err != nil {
		log.Fatalf("failed to load schema registry from %s: %v", opts.schemaDir, err)
	}

	transformer := internal.NewTransformer(registry)

	conn, err := pool.Acquire(ctx)
	if err != nil {
		log.Fatalf("failed to acquire connection: %v", err)
	}
	defer conn.Release()

	var schemaIDs map[string]int16
	if err := withTx(ctx, conn, func(tx pgx.Tx) error {
		if err := ensureTables(ctx, tx, opts); err != nil {
			return err
		}

		ids, err := syncSchemaRegistry(ctx, tx, opts, registry)
		if err != nil {
			return err
		}

		schemaIDs = ids
		return nil
	}); err != nil {
		log.Fatalf("failed to initialize metadata: %v", err)
	}

	if !opts.seedProvided {
		log.Printf("[info] Using random seed %d", opts.seed)
	}

	random := rand.New(rand.NewSource(opts.seed))

	type insertedSummary struct {
		records    int
		attributes int
	}

	summary := make(map[string]insertedSummary)

	if opts.leadCount > 0 {
		schemaID, ok := schemaIDs["lead"]
		if !ok {
			log.Fatalf("schema 'lead' not found. Ensure lead.json exists in %s", opts.schemaDir)
		}

		if opts.purge {
			if err := withTx(ctx, conn, func(tx pgx.Tx) error {
				return purgeSchema(ctx, tx, opts.eavTable, schemaID)
			}); err != nil {
				log.Fatalf("failed to purge existing lead data: %v", err)
			}
			log.Printf("[info] Cleared existing lead records for schema_id=%d", schemaID)
		}

		// Load attribute definitions
		attrDefs, err := loadAttributeDefinitions(opts.schemaDir, "lead")
		if err != nil {
			log.Fatalf("failed to load lead attribute definitions: %v", err)
		}

		attrs, err := buildLeadAttributes(ctx, transformer, schemaID, opts.leadCount, random, attrDefs)
		if err != nil {
			log.Fatalf("failed to build lead attributes: %v", err)
		}

		if err := copyAttributesInChunks(ctx, conn, opts.eavTable, attrs, opts.chunkSize); err != nil {
			log.Fatalf("failed to insert lead attributes: %v", err)
		}

		summary["lead"] = insertedSummary{
			records:    opts.leadCount,
			attributes: len(attrs),
		}
	}

	if opts.listingCount > 0 {
		schemaID, ok := schemaIDs["listing"]
		if !ok {
			log.Fatalf("schema 'listing' not found. Ensure listing.json exists in %s", opts.schemaDir)
		}

		if opts.purge {
			if err := withTx(ctx, conn, func(tx pgx.Tx) error {
				return purgeSchema(ctx, tx, opts.eavTable, schemaID)
			}); err != nil {
				log.Fatalf("failed to purge existing listing data: %v", err)
			}
			log.Printf("[info] Cleared existing listing records for schema_id=%d", schemaID)
		}

		// Load attribute definitions
		attrDefs, err := loadAttributeDefinitions(opts.schemaDir, "listing")
		if err != nil {
			log.Fatalf("failed to load listing attribute definitions: %v", err)
		}

		attrs, err := buildListingAttributes(ctx, transformer, schemaID, opts.listingCount, random, attrDefs)
		if err != nil {
			log.Fatalf("failed to build listing attributes: %v", err)
		}

		if err := copyAttributesInChunks(ctx, conn, opts.eavTable, attrs, opts.chunkSize); err != nil {
			log.Fatalf("failed to insert listing attributes: %v", err)
		}

		summary["listing"] = insertedSummary{
			records:    opts.listingCount,
			attributes: len(attrs),
		}
	}

	if len(summary) == 0 {
		log.Println("[info] No data generated (counts were zero).")
		return
	}

	log.Println("[success] Benchmark data generation complete:")
	for schema, s := range summary {
		log.Printf("  - %s: %d records, %d attributes", schema, s.records, s.attributes)
	}
}

func parseFlags() options {
	var opts options

	flag.StringVar(&opts.host, "db-host", getenvDefault("DB_HOST", "localhost"), "database host")
	flag.IntVar(&opts.port, "db-port", getenvDefaultInt("DB_PORT", 5432), "database port")
	flag.StringVar(&opts.database, "db-name", getenvDefault("DB_NAME", "forma"), "database name")
	flag.StringVar(&opts.user, "db-user", getenvDefault("DB_USER", "postgres"), "database user")
	flag.StringVar(&opts.password, "db-password", getenvDefault("DB_PASSWORD", "postgres"), "database password")
	flag.StringVar(&opts.sslMode, "db-ssl-mode", getenvDefault("DB_SSL_MODE", "disable"), "database sslmode")
	flag.StringVar(&opts.schemaTable, "schema-table", getenvDefault("SCHEMA_TABLE", "schema_registry"), "schema registry table")
	flag.StringVar(&opts.eavTable, "eav-table", getenvDefault("EAV_TABLE", "eav_data_2"), "EAV data table")
	flag.StringVar(&opts.schemaDir, "schema-dir", getenvDefault("SCHEMA_DIR", filepath.Join("../", "server", "schemas")), "directory containing JSON schemas")
	flag.BoolVar(&opts.purge, "purge", false, "delete existing records for targeted schemas before seeding")
	flag.IntVar(&opts.leadCount, "leads", 1000*1000, "number of lead records to generate")
	flag.IntVar(&opts.listingCount, "listings", 1000*1000, "number of listing records to generate")
	flag.IntVar(&opts.chunkSize, "chunk-size", 1000, "number of attributes to copy per batch")
	seed := flag.Int64("seed", 0, "random seed (0 uses current time)")

	flag.Parse()

	opts.schemaDir = filepath.Clean(opts.schemaDir)

	if *seed == 0 {
		opts.seed = time.Now().UnixNano()
		opts.seedProvided = false
	} else {
		opts.seed = *seed
		opts.seedProvided = true
	}

	if opts.chunkSize < 1000 {
		opts.chunkSize = 1000
	}

	if opts.leadCount < 0 || opts.listingCount < 0 {
		log.Fatal("record counts must be non-negative")
	}

	return opts
}

func buildConnString(opts options) string {
	hostPort := fmt.Sprintf("%s:%d", opts.host, opts.port)

	var userInfo *url.Userinfo
	if opts.password != "" {
		userInfo = url.UserPassword(opts.user, opts.password)
	} else {
		userInfo = url.User(opts.user)
	}

	u := &url.URL{
		Scheme: "postgres",
		User:   userInfo,
		Host:   hostPort,
		Path:   "/" + opts.database,
	}

	q := u.Query()
	if opts.sslMode != "" {
		q.Set("sslmode", opts.sslMode)
	}
	u.RawQuery = q.Encode()

	return u.String()
}

func withTx(ctx context.Context, conn *pgxpool.Conn, fn func(pgx.Tx) error) error {
	tx, err := conn.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}

	if err := fn(tx); err != nil {
		if rbErr := tx.Rollback(ctx); rbErr != nil {
			return fmt.Errorf("%w; rollback failed: %v", err, rbErr)
		}
		return err
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit tx: %w", err)
	}

	return nil
}

func ensureTables(ctx context.Context, tx pgx.Tx, opts options) error {
	schemaTable := quoteIdentifier(opts.schemaTable)
	eavTable := quoteIdentifier(opts.eavTable)

	ddlSchema := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
		schema_name TEXT PRIMARY KEY,
		schema_id SMALLINT UNIQUE NOT NULL
	)`, schemaTable)

	if _, err := tx.Exec(ctx, ddlSchema); err != nil {
		return fmt.Errorf("ensure schema registry table: %w", err)
	}

	fmt.Printf("Created %s\n", schemaTable)

	ddlEAV := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
		schema_id      SMALLINT NOT NULL,
		row_id         UUID NOT NULL,
		attr_id        SMALLINT NOT NULL,
		array_indices  TEXT NOT NULL DEFAULT '',
		value_text     TEXT,
		value_numeric  NUMERIC,
		value_date     TIMESTAMPTZ,
		value_bool     BOOLEAN,
		PRIMARY KEY (schema_id, row_id, attr_id, array_indices)
	)`, eavTable)

	if _, err := tx.Exec(ctx, ddlEAV); err != nil {
		return fmt.Errorf("ensure eav table: %w", err)
	}
	fmt.Printf("Created %s\n", eavTable)

	// Create type-specific partial indexes
	idxNumeric := quoteIdentifier(makeIndexName(opts.eavTable, "numeric"))
	createIdxNumeric := fmt.Sprintf(`CREATE INDEX IF NOT EXISTS %s ON %s (schema_id, attr_id, value_numeric) WHERE value_numeric IS NOT NULL`, idxNumeric, eavTable)
	if _, err := tx.Exec(ctx, createIdxNumeric); err != nil {
		return fmt.Errorf("create numeric index: %w", err)
	}

	idxText := quoteIdentifier(makeIndexName(opts.eavTable, "text"))
	createIdxText := fmt.Sprintf(`CREATE INDEX IF NOT EXISTS %s ON %s (schema_id, attr_id, value_text) WHERE value_text IS NOT NULL`, idxText, eavTable)
	if _, err := tx.Exec(ctx, createIdxText); err != nil {
		return fmt.Errorf("create text index: %w", err)
	}

	idxDate := quoteIdentifier(makeIndexName(opts.eavTable, "date"))
	createIdxDate := fmt.Sprintf(`CREATE INDEX IF NOT EXISTS %s ON %s (schema_id, attr_id, value_date) WHERE value_date IS NOT NULL`, idxDate, eavTable)
	if _, err := tx.Exec(ctx, createIdxDate); err != nil {
		return fmt.Errorf("create date index: %w", err)
	}

	idxBool := quoteIdentifier(makeIndexName(opts.eavTable, "bool"))
	createIdxBool := fmt.Sprintf(`CREATE INDEX IF NOT EXISTS %s ON %s (schema_id, attr_id, value_bool) WHERE value_bool IS NOT NULL`, idxBool, eavTable)
	if _, err := tx.Exec(ctx, createIdxBool); err != nil {
		return fmt.Errorf("create bool index: %w", err)
	}

	return nil
}

func syncSchemaRegistry(ctx context.Context, tx pgx.Tx, opts options, registry internal.SchemaRegistry) (map[string]int16, error) {
	tableName := quoteIdentifier(opts.schemaTable)
	mapping := make(map[string]int16)

	for _, name := range registry.ListSchemas() {
		id, _, err := registry.GetSchemaByName(name)
		if err != nil {
			return nil, fmt.Errorf("lookup schema %s: %w", name, err)
		}

		query := fmt.Sprintf(`INSERT INTO %s (schema_name, schema_id)
			VALUES ($1, $2)
			ON CONFLICT (schema_name) DO UPDATE SET schema_id = EXCLUDED.schema_id`, tableName)

		if _, err := tx.Exec(ctx, query, name, id); err != nil {
			return nil, fmt.Errorf("upsert schema registry row for %s: %w", name, err)
		}

		mapping[name] = id
	}

	return mapping, nil
}

func purgeSchema(ctx context.Context, tx pgx.Tx, eavTable string, schemaID int16) error {
	query := fmt.Sprintf(`DELETE FROM %s WHERE schema_id = $1`, quoteIdentifier(eavTable))
	if _, err := tx.Exec(ctx, query, schemaID); err != nil {
		return fmt.Errorf("purge schema_id %d: %w", schemaID, err)
	}
	return nil
}

func loadAttributeDefinitions(schemaDir, schemaName string) (map[string]AttributeMetadata, error) {
	filePath := filepath.Join(schemaDir, schemaName+"_attributes.json")
	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("read attribute definitions file %s: %w", filePath, err)
	}

	var defs map[string]AttributeMetadata
	if err := json.Unmarshal(data, &defs); err != nil {
		return nil, fmt.Errorf("parse attribute definitions: %w", err)
	}

	return defs, nil
}

func copyAttributesInChunks(ctx context.Context, conn *pgxpool.Conn, table string, attrs []internal.Attribute, chunkSize int) error {
	if len(attrs) == 0 {
		return nil
	}

	if chunkSize <= 0 {
		chunkSize = len(attrs)
	}

	tableIdent := pgx.Identifier(splitIdentifier(table))
	columns := []string{"schema_id", "row_id", "attr_id", "array_indices", "value_text", "value_numeric", "value_date", "value_bool"}

	for start := 0; start < len(attrs); start += chunkSize {
		end := start + chunkSize
		if end > len(attrs) {
			end = len(attrs)
		}

		rows := make([][]any, end-start)
		for i := start; i < end; i++ {
			attr := attrs[i]
			rows[i-start] = []any{
				attr.SchemaID,
				attr.RowID,
				attr.AttrID,
				attr.ArrayIndices,
				attr.ValueText,
				attr.ValueNumeric,
				attr.ValueDate,
				attr.ValueBool,
			}
		}

		if err := withTx(ctx, conn, func(tx pgx.Tx) error {
			if _, err := tx.CopyFrom(ctx, tableIdent, columns, pgx.CopyFromRows(rows)); err != nil {
				return fmt.Errorf("copy into %s: %w", table, err)
			}
			fmt.Printf("copy data, start pos: %d\n", start)
			return nil
		}); err != nil {
			return err
		}
	}

	return nil
}

func buildLeadAttributes(ctx context.Context, transformer internal.Transformer, schemaID int16, count int, r *rand.Rand, attrDefs map[string]AttributeMetadata) ([]internal.Attribute, error) {
	statuses := []string{"hot", "warm", "cold", "inactive", "converted"}
	firstNames := []string{"Alex", "Taylor", "Jordan", "Morgan", "Casey", "Riley", "Naomi", "Ken"}
	lastNames := []string{"Kim", "Suzuki", "Watanabe", "Sato", "Tanaka", "Kato", "Ito"}
	maritalStatuses := []string{"single", "married", "divorced", "widowed", "other"}
	prefectures := []string{"Tokyo", "Kanagawa", "Osaka", "Chiba", "Saitama", "Fukuoka"}
	cities := []string{"Shibuya", "Meguro", "Setagaya", "Yokohama", "Chiba", "Tenjin"}
	contactMethods := []string{"email", "phone", "sms", "line"}
	propertyTypes := []string{"apartment", "condo", "house", "townhouse", "other"}
	preferencePool := []string{"pet-friendly", "south-facing", "high-floor", "gym", "parking", "renewed"}

	attributes := make([]internal.Attribute, 0, count*40)

	for i := 0; i < count; i++ {
		rowID := uuid.Must(uuid.NewV7())

		first := randomChoice(r, firstNames)
		last := randomChoice(r, lastNames)
		pref := randomChoice(r, prefectures)
		city := randomChoice(r, cities)

		budgetMin := r.Intn(70_000_000-45_000_000) + 45_000_000
		budgetMax := budgetMin + r.Intn(50_000_000) + 5_000_000

		desiredAreas := toAnySlice(uniqueSample(r, prefectures, 2))
		preferences := toAnySlice(uniqueSample(r, preferencePool, 2))

		bedMin := r.Intn(2) + 1
		bedMax := bedMin + r.Intn(2) + 1

		data := map[string]any{
			"id":     fmt.Sprintf("lead-benchmark-%s", strings.ReplaceAll(rowID.String()[:12], "-", "")),
			"status": randomChoice(r, statuses),
			"personalInfo": map[string]any{
				"name": map[string]any{
					"display": fmt.Sprintf("%s %s", first, last),
					"given":   first,
					"family":  last,
				},
				"age":           r.Intn(40) + 25,
				"maritalStatus": randomChoice(r, maritalStatuses),
			},
			"contactInfo": map[string]any{
				"email":                  fmt.Sprintf("%s.%s-%s@example.com", strings.ToLower(first), strings.ToLower(last), rowID.String()[24:36]),
				"phone":                  fmt.Sprintf("+81-90-%04d-%04d", r.Intn(10000), r.Intn(10000)),
				"preferredContactMethod": randomChoice(r, contactMethods),
			},
			"currentAddress": map[string]any{
				"city":        city,
				"prefecture":  pref,
				"fullAddress": fmt.Sprintf("%d-%d-%d %s", r.Intn(4)+1, r.Intn(20)+1, r.Intn(50)+1, city),
			},
			"propertyRequirements": map[string]any{
				"budget": map[string]any{
					"min":      budgetMin,
					"max":      budgetMax,
					"currency": "JPY",
				},
				"desiredAreas":          desiredAreas,
				"propertyType":          randomChoice(r, propertyTypes),
				"bedrooms":              map[string]any{"min": bedMin, "max": bedMax},
				"maxStationWalkMinutes": r.Intn(15) + 1,
				"preferences":           preferences,
				"purpose":               randomChoice(r, []string{"primary-residence", "investment", "vacation"}),
				"targetMoveInDate":      time.Now().AddDate(0, r.Intn(6)+1, 0).Format("2006-01-02"),
				"moveInDateFlexibility": randomChoice(r, []string{"flexible", "somewhat-flexible", "fixed"}),
				"excludeConditions":     toAnySlice(uniqueSample(r, []string{"noisy", "needs renovation", "old building"}, 1)),
			},
			"metadata": map[string]any{
				"createdBy": "benchmark-seeder",
				"source":    randomChoice(r, []string{"web", "referral", "event"}),
				"createdAt": time.Now().Add(-time.Duration(r.Intn(14*24)) * time.Hour).UTC().Format(time.RFC3339),
			},
		}

		attrs, err := transformToTypedAttributes(ctx, transformer, schemaID, rowID, data, attrDefs)
		if err != nil {
			return nil, fmt.Errorf("lead %d transformation: %w", i, err)
		}

		attributes = append(attributes, attrs...)
	}

	return attributes, nil
}

func buildListingAttributes(ctx context.Context, transformer internal.Transformer, schemaID int16, count int, r *rand.Rand, attrDefs map[string]AttributeMetadata) ([]internal.Attribute, error) {
	propertyTypes := []string{"condominium", "house", "land", "commercial"}
	lineNames := []string{"Yamanote Line", "Chuo Line", "Ginza Line", "Hibiya Line", "Den-en-toshi Line"}
	stationNames := []string{"Shibuya", "Shinjuku", "Meguro", "Ebisu", "Ginza", "Nakameguro"}
	accessMethods := []string{"walk", "bus", "car"}
	transactionTypes := []string{"seller", "exclusive_agent", "non_exclusive_agent", "broker"}
	materials := []string{"RC", "SRC", "S", "Wood", "Other"}
	currentConditions := []string{"vacant", "occupied_by_owner", "rented"}
	handoverOptions := []string{"immediate", "consultation", "scheduled"}
	layouts := []string{"1LDK", "2LDK", "3LDK", "4LDK"}
	roomTypes := []string{"living", "bedroom", "kitchen", "study"}
	featuresPool := []string{"south-facing", "system kitchen", "floor heating", "walk-in closet", "auto lock"}
	landRights := []string{"freehold", "leasehold_fixed_term", "leasehold_old_law", "other"}

	attributes := make([]internal.Attribute, 0, count*80)

	for i := 0; i < count; i++ {
		rowID := uuid.Must(uuid.NewV7())
		listingID := uuid.New()

		buildingName := fmt.Sprintf("Benchmark Tower %d", i+1)
		prefecture := randomChoice(r, []string{"Tokyo", "Kanagawa", "Chiba", "Saitama"})
		city := randomChoice(r, []string{"Shibuya", "Minato", "Meguro", "Yokohama"})
		year := r.Intn(2024-1990) + 1990
		month := r.Intn(12) + 1

		floor := r.Intn(38) + 2
		exclusiveArea := math.Round((r.Float64()*80+40)*10) / 10
		balconyArea := math.Round(exclusiveArea*0.18*10) / 10

		access := []map[string]any{
			{
				"line":         randomChoice(r, lineNames),
				"station":      randomChoice(r, stationNames),
				"method":       randomChoice(r, accessMethods),
				"duration_min": r.Intn(18) + 1,
			},
		}

		roomCount := r.Intn(2) + 1
		rooms := make([]map[string]any, roomCount)
		for j := 0; j < roomCount; j++ {
			rooms[j] = map[string]any{
				"type": randomChoice(r, roomTypes),
				"size": math.Round((r.Float64()*15+8)*10) / 10,
			}
		}

		features := toAnySlice(uniqueSample(r, featuresPool, 3))

		data := map[string]any{
			"listingId":    listingID.String(),
			"propertyType": randomChoice(r, propertyTypes),
			"building": map[string]any{
				"name": map[string]any{
					"ja":           buildingName,
					"ja_romanized": strings.ToUpper(buildingName),
				},
				"address": map[string]any{
					"prefecture":  prefecture,
					"city":        city,
					"fullAddress": fmt.Sprintf("%d-%d-%d %s", r.Intn(3)+1, r.Intn(20)+1, r.Intn(60)+1, city),
				},
				"completionDate": fmt.Sprintf("%04d-%02d", year, month),
				"structure": map[string]any{
					"material":           randomChoice(r, materials),
					"storiesAboveGround": r.Intn(40) + 5,
				},
				"totalUnits": r.Intn(150) + 40,
			},
			"unit": map[string]any{
				"unitNumber": fmt.Sprintf("%d%c", floor, 'A'+r.Intn(3)),
				"floor":      floor,
				"price": map[string]any{
					"amount":        r.Intn(150_000_000-45_000_000) + 45_000_000,
					"currency":      "JPY",
					"isTaxIncluded": true,
				},
				"monthlyFees": map[string]any{
					"management":    r.Intn(45_000-12_000) + 12_000,
					"repairReserve": r.Intn(25_000-8_000) + 8_000,
					"otherFees":     []any{},
				},
				"floorPlan": map[string]any{
					"layout": randomChoice(r, layouts),
					"rooms":  mapsToAny(rooms),
				},
				"area": map[string]any{
					"exclusive": exclusiveArea,
					"balcony":   balconyArea,
					"other":     []any{},
				},
				"petPolicy": map[string]any{
					"allowed": r.Intn(2) == 0,
				},
				"features": features,
			},
			"access": mapsToAny(access),
			"legal":  map[string]any{"landRights": randomChoice(r, landRights), "zoning": "residential"},
			"status": map[string]any{"currentCondition": randomChoice(r, currentConditions), "handover": randomChoice(r, handoverOptions)},
			"listingInfo": map[string]any{
				"agent": map[string]any{
					"companyName": "Benchmark Realty",
					"phone":       "+81-3-5555-0000",
				},
				"contacts": mapsToAny([]map[string]any{
					{
						"name":  fmt.Sprintf("Agent %d", i+1),
						"phone": fmt.Sprintf("+81-80-%04d-%04d", r.Intn(10000), r.Intn(10000)),
					},
				}),
				"transactionType": randomChoice(r, transactionTypes),
			},
		}

		attrs, err := transformToTypedAttributes(ctx, transformer, schemaID, rowID, data, attrDefs)
		if err != nil {
			return nil, fmt.Errorf("listing %d transformation: %w", i, err)
		}

		attributes = append(attributes, attrs...)
	}

	return attributes, nil
}

// transformToTypedAttributes converts flattened attributes to typed attributes with attr_id
func transformToTypedAttributes(
	ctx context.Context,
	transformer internal.Transformer,
	schemaID int16,
	rowID uuid.UUID,
	data map[string]any,
	attrDefs map[string]AttributeMetadata,
) ([]internal.Attribute, error) {
	// Use existing transformer to flatten the data
	// Note: This will produce old-style Attribute structs that we'll need to adapt
	// For now, we'll flatten manually to work with the new structure

	typedAttrs := make([]internal.Attribute, 0)
	flattenJSONToTypedAttributes(schemaID, rowID, "", data, []int{}, attrDefs, &typedAttrs)

	return typedAttrs, nil
}

// flattenJSONToTypedAttributes recursively flattens JSON and creates typed attributes
func flattenJSONToTypedAttributes(
	schemaID int16,
	rowID uuid.UUID,
	prefix string,
	data any,
	indices []int,
	attrDefs map[string]AttributeMetadata,
	result *[]internal.Attribute,
) {
	switch v := data.(type) {
	case map[string]any:
		for key, value := range v {
			newPrefix := buildPrefix(prefix, key)
			flattenJSONToTypedAttributes(schemaID, rowID, newPrefix, value, indices, attrDefs, result)
		}
	case []any:
		for i, item := range v {
			newIndices := append(append([]int{}, indices...), i)
			flattenJSONToTypedAttributes(schemaID, rowID, prefix, item, newIndices, attrDefs, result)
		}
	default:
		// Leaf node - create typed attribute
		if v == nil {
			return
		}

		// Look up attribute metadata
		meta, ok := attrDefs[prefix]
		if !ok {
			// Skip unknown attributes
			return
		}

		// Convert indices to comma-separated string
		arrayIndices := ""
		if len(indices) > 0 {
			indicesStr := make([]string, len(indices))
			for i, idx := range indices {
				indicesStr[i] = strconv.Itoa(idx)
			}
			arrayIndices = strings.Join(indicesStr, ",")
		}

		// Create typed attribute
		attr := internal.Attribute{
			SchemaID:     schemaID,
			RowID:        rowID,
			AttrID:       meta.AttrID,
			ArrayIndices: arrayIndices,
		}

		// Convert value based on type
		valueStr := fmt.Sprintf("%v", v)
		convertValueToTypedField(&attr, valueStr, meta.ValueType)

		*result = append(*result, attr)
	}
}

// buildPrefix constructs the next part of the path
func buildPrefix(current string, key string) string {
	if current == "" {
		return key
	}
	return current + "." + key
}

// convertValueToTypedField populates the appropriate typed field in the attribute
func convertValueToTypedField(attr *internal.Attribute, valueStr string, valueType string) {
	switch valueType {
	case "text":
		attr.ValueText = &valueStr
	case "numeric":
		if num, err := strconv.ParseFloat(valueStr, 64); err == nil {
			attr.ValueNumeric = &num
		}
	case "date":
		// Try multiple date formats
		formats := []string{
			time.RFC3339,
			"2006-01-02",
			"2006-01",
		}
		for _, format := range formats {
			if t, err := time.Parse(format, valueStr); err == nil {
				attr.ValueDate = &t
				return
			}
		}
	case "bool":
		if b, err := strconv.ParseBool(valueStr); err == nil {
			attr.ValueBool = &b
		}
	}
}

// parseAttrName parses an attribute name into path and indices
// This is kept for future use if needed
func parseAttrName(name string) (string, []int) {
	// Pattern: "path(0, 1, 2)" or "path(0)" or "path"
	indicesRegex := regexp.MustCompile(`\(([^)]+)\)$`)
	match := indicesRegex.FindStringSubmatch(name)

	var indices []int
	baseName := name

	if match != nil {
		baseName = name[:len(name)-len(match[0])]
		indicesStr := match[1]
		indexParts := strings.Split(indicesStr, ", ")
		for _, part := range indexParts {
			part = strings.TrimSpace(part)
			if idx, err := strconv.Atoi(part); err == nil {
				indices = append(indices, idx)
			}
		}
	}

	// Remove trailing "[]" markers if present
	baseName = strings.ReplaceAll(baseName, "[]", "")

	return baseName, indices
}

func randomChoice(r *rand.Rand, values []string) string {
	return values[r.Intn(len(values))]
}

func uniqueSample(r *rand.Rand, values []string, count int) []string {
	if count <= 0 {
		return []string{}
	}
	if count >= len(values) {
		return append([]string{}, values...)
	}

	perm := r.Perm(len(values))
	result := make([]string, 0, count)
	for i := 0; i < count; i++ {
		result = append(result, values[perm[i]])
	}
	return result
}

func toAnySlice(values []string) []any {
	result := make([]any, len(values))
	for i, v := range values {
		result[i] = v
	}
	return result
}

func mapsToAny(values []map[string]any) []any {
	result := make([]any, len(values))
	for i, v := range values {
		result[i] = v
	}
	return result
}

func quoteIdentifier(name string) string {
	return pgx.Identifier(splitIdentifier(name)).Sanitize()
}

func splitIdentifier(name string) []string {
	parts := strings.Split(name, ".")
	result := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			result = append(result, part)
		}
	}
	if len(result) == 0 {
		return []string{name}
	}
	return result
}

func makeIndexName(table string, suffix string) string {
	base := strings.ReplaceAll(table, ".", "_")
	base = strings.ReplaceAll(base, `"`, "")
	return fmt.Sprintf("%s_%s_idx", base, suffix)
}

func getenvDefault(key, def string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return def
}

func getenvDefaultInt(key string, def int) int {
	if val := os.Getenv(key); val != "" {
		if parsed, err := strconv.Atoi(val); err == nil {
			return parsed
		}
	}
	return def
}
