package benchmark

import (
	"fmt"
	"math"
	"sort"
)

// SelectivityBand classifies a filter candidate by match ratio.
type SelectivityBand string

const (
	SelectivityBandHigh   SelectivityBand = "high"
	SelectivityBandMedium SelectivityBand = "medium"
	SelectivityBandLow    SelectivityBand = "low"
)

// CalibrationCandidate represents an attribute/value pair suitable for a filter benchmark.
type CalibrationCandidate struct {
	Attribute string          `json:"attribute"`
	Value     string          `json:"value"`
	Matches   int             `json:"matches"`
	Ratio     float64         `json:"ratio"`
	Band      SelectivityBand `json:"band"`
}

// TradeCalibration groups selectivity candidates for trade workloads.
type TradeCalibration struct {
	High   CalibrationCandidate `json:"high"`
	Medium CalibrationCandidate `json:"medium"`
	Low    CalibrationCandidate `json:"low"`
}

// CalibrateTradeFilters computes representative trade filter values for the three selectivity bands.
func CalibrateTradeFilters(records []GeneratedRecord) (TradeCalibration, error) {
	tradeRecords := LatestRecords(filterRecordsBySchema(records, "trade"))
	if len(tradeRecords) == 0 {
		return TradeCalibration{}, fmt.Errorf("no trade records available for calibration")
	}
	high, err := selectCandidate(tradeRecords, []string{"symbol"}, SelectivityBandHigh)
	if err != nil {
		return TradeCalibration{}, err
	}
	medium, err := selectCandidate(tradeRecords, []string{"tradeType", "exchange"}, SelectivityBandMedium)
	if err != nil {
		return TradeCalibration{}, err
	}
	low, err := selectCandidate(tradeRecords, []string{"region", "orderChannel"}, SelectivityBandLow)
	if err != nil {
		return TradeCalibration{}, err
	}
	return TradeCalibration{High: high, Medium: medium, Low: low}, nil
}

func selectCandidate(records []GeneratedRecord, attributes []string, band SelectivityBand) (CalibrationCandidate, error) {
	total := len(records)
	if total == 0 {
		return CalibrationCandidate{}, fmt.Errorf("cannot calibrate with no records")
	}
	minRatio, maxRatio, target := bandRange(band)
	best := CalibrationCandidate{}
	bestDistance := math.MaxFloat64
	fallback := CalibrationCandidate{}
	fallbackDistance := math.MaxFloat64
	found := false
	fallbackFound := false
	for _, attribute := range attributes {
		counts := make(map[string]int)
		for _, record := range records {
			value, ok := record.Attributes[attribute]
			if !ok {
				continue
			}
			counts[fmt.Sprint(value)]++
		}
		keys := make([]string, 0, len(counts))
		for key := range counts {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			matches := counts[key]
			ratio := float64(matches) / float64(total)
			distance := math.Abs(target - ratio)
			if !fallbackFound || distance < fallbackDistance {
				fallback = CalibrationCandidate{Attribute: attribute, Value: key, Matches: matches, Ratio: ratio, Band: band}
				fallbackDistance = distance
				fallbackFound = true
			}
			if ratio < minRatio || ratio > maxRatio {
				continue
			}
			if !found || distance < bestDistance {
				best = CalibrationCandidate{Attribute: attribute, Value: key, Matches: matches, Ratio: ratio, Band: band}
				bestDistance = distance
				found = true
			}
		}
	}
	if !found {
		if fallbackFound {
			return fallback, nil
		}
		return CalibrationCandidate{}, fmt.Errorf("no candidate found for %s selectivity", band)
	}
	return best, nil
}

func bandRange(band SelectivityBand) (float64, float64, float64) {
	switch band {
	case SelectivityBandHigh:
		return 0, 0.02, 0.01
	case SelectivityBandMedium:
		return 0.02, 0.20, 0.10
	case SelectivityBandLow:
		return 0.20, 1, 0.30
	default:
		return 0, 1, 0.10
	}
}

func filterRecordsBySchema(records []GeneratedRecord, schemaName string) []GeneratedRecord {
	out := make([]GeneratedRecord, 0)
	for _, record := range records {
		if record.SchemaName == schemaName {
			out = append(out, cloneGeneratedRecord(record))
		}
	}
	return out
}
