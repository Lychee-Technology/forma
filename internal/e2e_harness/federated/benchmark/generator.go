package benchmark

import (
	"fmt"
	"math"
	"math/rand"
	"strconv"
	"time"

	"github.com/google/uuid"
)

var (
	benchmarkRegions       = []string{"NA", "EU", "APAC", "LATAM"}
	benchmarkExchanges     = []string{"NYSE", "NASDAQ", "CBOE", "IEX"}
	benchmarkOrderChannels = []string{"web", "mobile", "branch", "api"}
)

type benchmarkCustomer struct {
	rowID  uuid.UUID
	taxID  string
	region string
	status int
	name   string
	email  string
}

type benchmarkSecurity struct {
	rowID  uuid.UUID
	symbol string
	sector int
	name   string
}

// Generator produces deterministic benchmark datasets.
type Generator struct {
	config       GeneratorConfig
	rand         *rand.Rand
	securityZipf *rand.Zipf
	customerZipf *rand.Zipf
}

// NewGenerator creates a generator with deterministic random sources.
func NewGenerator(cfg GeneratorConfig) (*Generator, error) {
	resolved := cfg.WithDefaults()
	if err := resolved.Validate(); err != nil {
		return nil, err
	}
	r := rand.New(rand.NewSource(resolved.Seed))
	return &Generator{config: resolved, rand: r, securityZipf: newZipf(r, resolved.SecurityCount), customerZipf: newZipf(r, resolved.CustomerCount)}, nil
}

// Generate creates the benchmark dataset for all benchmark schemas.
func (g *Generator) Generate() (*GeneratedDataset, error) {
	customers := g.generateCustomers()
	securities := g.generateSecurities()
	records := make([]GeneratedRecord, 0, g.config.CustomerCount+g.config.SecurityCount+g.config.TradeCount)
	for _, customer := range customers {
		records = append(records, g.customerRecord(customer))
	}
	for _, security := range securities {
		records = append(records, g.securityRecord(security))
	}
	records = append(records, g.generateTrades(customers, securities)...)
	sortGeneratedRecords(records)
	return &GeneratedDataset{Config: g.config, Records: records, Summary: summarizeDataset(records)}, nil
}

func (g *Generator) generateCustomers() []benchmarkCustomer {
	customers := make([]benchmarkCustomer, 0, g.config.CustomerCount)
	for i := 0; i < g.config.CustomerCount; i++ {
		customers = append(customers, benchmarkCustomer{
			rowID:  deterministicRowID(g.config.Seed, "customer", i),
			taxID:  fmt.Sprintf("TAX-%08d", i+1),
			region: g.pickRegion(),
			status: i % 4,
			name:   fmt.Sprintf("Customer %08d", i+1),
			email:  fmt.Sprintf("customer-%08d@example.com", i+1),
		})
	}
	return customers
}

func (g *Generator) generateSecurities() []benchmarkSecurity {
	securities := make([]benchmarkSecurity, 0, g.config.SecurityCount)
	for i := 0; i < g.config.SecurityCount; i++ {
		symbol := fmt.Sprintf("SYM%05d", i+1)
		securities = append(securities, benchmarkSecurity{
			rowID:  deterministicRowID(g.config.Seed, "security", i),
			symbol: symbol,
			sector: i % 32,
			name:   fmt.Sprintf("Security %s", symbol),
		})
	}
	return securities
}

func (g *Generator) generateTrades(customers []benchmarkCustomer, securities []benchmarkSecurity) []GeneratedRecord {
	base := make([]GeneratedRecord, 0, g.config.TradeCount)
	for i := 0; i < g.config.TradeCount; i++ {
		customer := customers[g.pickCustomerIndex()]
		security := securities[g.pickSecurityIndex()]
		changedAt := g.pickChangedAt(i)
		price := round2(10 + g.rand.Float64()*490)
		quantity := int64((i%25)+1) * 10
		tradeType := i % 6
		deletedAt := int64(0)
		if g.rand.Float64() < g.config.DeleteRatio {
			deletedAt = changedAt + int64((i%900)+1)*1000
		}
		base = append(base, GeneratedRecord{
			SchemaID:   SchemaIDTrade,
			SchemaName: "trade",
			RowID:      deterministicRowID(g.config.Seed, "trade", i),
			Version:    1,
			ChangedAt:  changedAt,
			DeletedAt:  deletedAt,
			Attributes: map[string]any{
				"symbol":       security.symbol,
				"tradeType":    tradeType,
				"quantity":     quantity,
				"price":        price,
				"tradeTime":    time.UnixMilli(changedAt).UTC().Format(time.RFC3339),
				"customerId":   customer.rowID.String(),
				"region":       customer.region,
				"exchange":     g.pickExchange(security.symbol),
				"commission":   round2(price * float64(quantity) * 0.001),
				"isCash":       g.rand.Intn(2) == 0,
				"brokerId":     fmt.Sprintf("BRK-%04d", (i%250)+1),
				"orderChannel": benchmarkOrderChannels[i%len(benchmarkOrderChannels)],
			},
		})
	}
	if g.config.Distribution != DistributionHotspot {
		return base
	}
	overlapCount := int(float64(g.config.TradeCount) * g.config.OverlapRatio)
	if overlapCount == 0 {
		return base
	}
	records := make([]GeneratedRecord, 0, len(base)+overlapCount)
	records = append(records, base...)
	for i := 0; i < overlapCount && i < len(base); i++ {
		baseRecord := cloneGeneratedRecord(base[i])
		price := attributeFloat64(baseRecord.Attributes, "price")
		quantity := attributeInt64(baseRecord.Attributes, "quantity")
		changedAt := baseRecord.ChangedAt + int64((i%3600)+1)*1000
		updated := cloneGeneratedRecord(baseRecord)
		updated.Version = 2
		updated.ChangedAt = changedAt
		updated.Attributes["price"] = round2(price * 1.015)
		updated.Attributes["commission"] = round2((price * 1.015) * float64(quantity) * 0.0012)
		updated.Attributes["isCash"] = !attributeBool(baseRecord.Attributes, "isCash")
		updated.Attributes["orderChannel"] = benchmarkOrderChannels[(i+1)%len(benchmarkOrderChannels)]
		updated.Attributes["tradeTime"] = time.UnixMilli(changedAt).UTC().Format(time.RFC3339)
		if i%7 == 0 {
			updated.DeletedAt = changedAt + 5000
		}
		records = append(records, updated)
	}
	return records
}

func (g *Generator) customerRecord(customer benchmarkCustomer) GeneratedRecord {
	changedAt := g.config.BaseTime.Add(-45 * 24 * time.Hour).UnixMilli()
	return GeneratedRecord{SchemaID: SchemaIDCustomer, SchemaName: "customer", RowID: customer.rowID, Version: 1, ChangedAt: changedAt, Attributes: map[string]any{"taxId": customer.taxID, "status": customer.status, "region": customer.region, "name": customer.name, "email": customer.email, "creditRating": 500 + float64((customer.status*73)%300)}}
}

func (g *Generator) securityRecord(security benchmarkSecurity) GeneratedRecord {
	changedAt := g.config.BaseTime.Add(-60 * 24 * time.Hour).UnixMilli()
	return GeneratedRecord{SchemaID: SchemaIDSecurity, SchemaName: "security", RowID: security.rowID, Version: 1, ChangedAt: changedAt, Attributes: map[string]any{"symbol": security.symbol, "sector": security.sector, "companyName": security.name, "dividend": round2(float64((security.sector%7)+1) * 0.12), "marketCap": float64((security.sector+1)*1000000) + float64(len(security.symbol))*1000}}
}

func (g *Generator) pickRegion() string {
	weights := []int{25, 25, 25, 25}
	switch g.config.Distribution {
	case DistributionPartitionSkew:
		weights = []int{50, 30, 15, 5}
	case DistributionZipf:
		weights = []int{45, 25, 20, 10}
	case DistributionTemporal:
		weights = []int{40, 25, 20, 15}
	case DistributionHotspot:
		weights = []int{55, 20, 15, 10}
	}
	return weightedChoice(g.rand, benchmarkRegions, weights)
}

func (g *Generator) pickExchange(symbol string) string {
	switch g.config.Distribution {
	case DistributionZipf:
		if len(symbol) > 0 && symbol[len(symbol)-1]%2 == 0 {
			return benchmarkExchanges[0]
		}
	case DistributionPartitionSkew:
		return weightedChoice(g.rand, benchmarkExchanges, []int{45, 30, 15, 10})
	}
	return benchmarkExchanges[g.rand.Intn(len(benchmarkExchanges))]
}

func (g *Generator) pickCustomerIndex() int {
	if g.customerZipf != nil && g.config.Distribution == DistributionZipf {
		return int(g.customerZipf.Uint64())
	}
	return g.rand.Intn(g.config.CustomerCount)
}

func (g *Generator) pickSecurityIndex() int {
	if g.securityZipf != nil && (g.config.Distribution == DistributionZipf || g.config.Distribution == DistributionHotspot) {
		return int(g.securityZipf.Uint64())
	}
	return g.rand.Intn(g.config.SecurityCount)
}

func (g *Generator) pickChangedAt(index int) int64 {
	windowMillis := int64(g.config.TimeWindowDays) * 24 * int64(time.Hour/time.Millisecond)
	var fraction float64
	switch g.config.Distribution {
	case DistributionTemporal, DistributionHotspot:
		fraction = math.Pow(g.rand.Float64(), 4)
	case DistributionZipf:
		fraction = math.Pow(g.rand.Float64(), 2)
	default:
		fraction = g.rand.Float64()
	}
	ageMillis := int64(fraction * float64(windowMillis))
	changedAt := g.config.BaseTime.UnixMilli() - ageMillis
	return changedAt + int64(index%1000)
}

func newZipf(r *rand.Rand, size int) *rand.Zipf {
	if size <= 1 {
		return nil
	}
	return rand.NewZipf(r, 1.2, 1, uint64(size-1))
}

func deterministicRowID(seed int64, schema string, index int) uuid.UUID {
	name := fmt.Sprintf("seed:%d:schema:%s:index:%d", seed, schema, index)
	return uuid.NewSHA1(uuid.NameSpaceOID, []byte(name))
}

func weightedChoice(r *rand.Rand, values []string, weights []int) string {
	total := 0
	for _, weight := range weights {
		total += weight
	}
	pick := r.Intn(total)
	for idx, weight := range weights {
		if pick < weight {
			return values[idx]
		}
		pick -= weight
	}
	return values[len(values)-1]
}

func round2(value float64) float64 {
	return math.Round(value*100) / 100
}

func attributeFloat64(attrs map[string]any, key string) float64 {
	value, ok := attrs[key]
	if !ok {
		return 0
	}
	switch v := value.(type) {
	case float64:
		return v
	case int:
		return float64(v)
	case int64:
		return float64(v)
	case string:
		parsed, _ := strconv.ParseFloat(v, 64)
		return parsed
	default:
		return 0
	}
}

func attributeInt64(attrs map[string]any, key string) int64 {
	value, ok := attrs[key]
	if !ok {
		return 0
	}
	switch v := value.(type) {
	case int64:
		return v
	case int:
		return int64(v)
	case float64:
		return int64(v)
	default:
		return 0
	}
}

func attributeBool(attrs map[string]any, key string) bool {
	value, ok := attrs[key]
	if !ok {
		return false
	}
	v, _ := value.(bool)
	return v
}
