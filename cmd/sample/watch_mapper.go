package main

import (
	"fmt"
	"strconv"
	"strings"
)

// =============================================================================
// Watch-specific Field Mappers
// =============================================================================

// toPriceMapper converts price strings like "$43,500" to float64.
// Returns nil for "Price on request" or empty strings.
type toPriceMapper struct{}

func (m *toPriceMapper) Map(csvValue string) (any, error) {
	v := strings.TrimSpace(csvValue)
	if v == "" {
		return nil, nil
	}

	// Handle "Price on request" or similar non-numeric prices
	if strings.Contains(strings.ToLower(v), "request") || strings.Contains(strings.ToLower(v), "price on") {
		return nil, nil
	}

	// Remove currency symbols and formatting
	v = strings.ReplaceAll(v, "$", "")
	v = strings.ReplaceAll(v, "€", "")
	v = strings.ReplaceAll(v, "£", "")
	v = strings.ReplaceAll(v, "¥", "")
	v = strings.ReplaceAll(v, ",", "")
	v = strings.TrimSpace(v)

	if v == "" {
		return nil, nil
	}

	f, err := strconv.ParseFloat(v, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid price format: %v", err)
	}
	return f, nil
}

// ToPrice returns a mapper that converts price strings (e.g., "$43,500") to float64.
// Returns nil for "Price on request" or empty values.
func ToPrice() FieldMapper {
	return &toPriceMapper{}
}

// toYearMapper extracts year from strings like "2022", "2022 (Approximation)", "Unknown".
type toYearMapper struct{}

func (m *toYearMapper) Map(csvValue string) (any, error) {
	v := strings.TrimSpace(csvValue)
	if v == "" || strings.EqualFold(v, "unknown") {
		return nil, nil
	}

	// Extract the year number (first 4-digit sequence)
	// Handle formats like "2022", "2022 (Approximation)", "2022 (Approximation)"
	yearStr := v
	if idx := strings.Index(v, " "); idx > 0 {
		yearStr = v[:idx]
	}
	if idx := strings.Index(v, "("); idx > 0 {
		yearStr = strings.TrimSpace(v[:idx])
	}

	year, err := strconv.Atoi(yearStr)
	if err != nil {
		return nil, nil // Return nil for unparseable years
	}

	// Validate year range
	if year < 1900 || year > 2100 {
		return nil, fmt.Errorf("year out of range: %d", year)
	}

	return year, nil
}

// ToYear returns a mapper that extracts year from production year strings.
// Handles formats like "2022", "2022 (Approximation)", "Unknown".
// Returns nil for "Unknown" or unparseable values.
func ToYear() FieldMapper {
	return &toYearMapper{}
}

// isYearApproximateMapper checks if year string indicates an approximation.
type isYearApproximateMapper struct{}

func (m *isYearApproximateMapper) Map(csvValue string) (any, error) {
	v := strings.TrimSpace(csvValue)
	if v == "" || strings.EqualFold(v, "unknown") {
		return nil, nil
	}

	// Check if the value contains "Approximation" or similar indicators
	isApproximate := strings.Contains(strings.ToLower(v), "approximation") ||
		strings.Contains(strings.ToLower(v), "approx") ||
		strings.Contains(strings.ToLower(v), "circa") ||
		strings.Contains(strings.ToLower(v), "~")

	return isApproximate, nil
}

// IsYearApproximate returns a mapper that checks if the year is approximate.
// Returns true if the value contains "Approximation", "approx", "circa", or "~".
func IsYearApproximate() FieldMapper {
	return &isYearApproximateMapper{}
}

// toSizeWidthMapper extracts width from size strings like "42 mm" or "42 x 54 mm".
type toSizeWidthMapper struct{}

func (m *toSizeWidthMapper) Map(csvValue string) (any, error) {
	v := strings.TrimSpace(csvValue)
	if v == "" {
		return nil, nil
	}

	// Remove "mm" suffix and trim
	v = strings.ReplaceAll(v, "mm", "")
	v = strings.TrimSpace(v)

	// Check for "width x height" format (e.g., "42 x 54")
	if strings.Contains(v, "x") {
		parts := strings.Split(v, "x")
		if len(parts) >= 1 {
			widthStr := strings.TrimSpace(parts[0])
			width, err := strconv.ParseFloat(widthStr, 64)
			if err != nil {
				return nil, nil // Return nil for unparseable values
			}
			return width, nil
		}
	}

	// Single value format (e.g., "42")
	width, err := strconv.ParseFloat(v, 64)
	if err != nil {
		return nil, nil // Return nil for unparseable values
	}
	return width, nil
}

// ToSizeWidth returns a mapper that extracts width from size strings.
// Handles formats like "42 mm" (returns 42) or "42 x 54 mm" (returns 42).
func ToSizeWidth() FieldMapper {
	return &toSizeWidthMapper{}
}

// toSizeHeightMapper extracts height from size strings like "42 x 54 mm".
type toSizeHeightMapper struct{}

func (m *toSizeHeightMapper) Map(csvValue string) (any, error) {
	v := strings.TrimSpace(csvValue)
	if v == "" {
		return nil, nil
	}

	// Remove "mm" suffix and trim
	v = strings.ReplaceAll(v, "mm", "")
	v = strings.TrimSpace(v)

	// Check for "width x height" format (e.g., "42 x 54")
	if strings.Contains(v, "x") {
		parts := strings.Split(v, "x")
		if len(parts) >= 2 {
			heightStr := strings.TrimSpace(parts[1])
			height, err := strconv.ParseFloat(heightStr, 64)
			if err != nil {
				return nil, nil // Return nil for unparseable values
			}
			return height, nil
		}
	}

	// Single value format - no height dimension
	return nil, nil
}

// ToSizeHeight returns a mapper that extracts height from size strings.
// Handles formats like "42 x 54 mm" (returns 54). Returns nil for single dimension sizes.
func ToSizeHeight() FieldMapper {
	return &toSizeHeightMapper{}
}

// =============================================================================
// Watch Mapper Definition
// =============================================================================

// NewWatchMapper creates a CSVToSchemaMapper for the Watch schema.
// This mapper handles watch marketplace CSV data with fields like price, year, condition.
//
// Expected CSV columns:
//   - id: Watch identifier (required)
//   - name: Watch name/title (required)
//   - price: Price string (e.g., "$43,500" or "Price on request")
//   - brand: Watch brand (required)
//   - model: Watch model name
//   - ref: Reference number
//   - mvmt: Movement type (Automatic, Manual, Quartz)
//   - casem: Case material
//   - bracem: Bracelet/strap material
//   - yop: Year of production (e.g., "2022", "2022 (Approximation)", "Unknown")
//   - cond: Condition (Unworn, New, Very good, Good, Fair)
//   - sex: Target gender
//   - size: Watch size/diameter (e.g., "42 mm", "42 x 54 mm")
func NewWatchMapper() CSVToSchemaMapper {
	return NewMapperBuilder("watch").
		// Required fields
		RequiredWith("id", "id", ToInt()).
		Required("name", "name").
		Required("brand", "brand").

		// Price fields - nested object price.value and price.display
		MapWith("price", "price.value", ToPrice()).
		Map("price", "price.display").

		// Model and reference
		Map("model", "model").
		Map("ref", "reference").

		// Technical specs
		Map("mvmt", "movement").

		// Material - nested object material.case and material.bracelet
		Map("casem", "material.case").
		Map("bracem", "material.bracelet").

		// Year of production - nested object yearOfProduction.year and yearOfProduction.approximate
		MapWith("yop", "yearOfProduction.year", ToYear()).
		MapWith("yop", "yearOfProduction.approximate", IsYearApproximate()).

		// Condition and other attributes
		MapWith("cond", "condition", Enum("Unworn", "New", "Very good", "Good", "Fair")).
		Map("sex", "gender").

		// Size - parse width and height from size string (e.g., "42 mm" or "42 x 54 mm")
		MapWith("size", "size.width", ToSizeWidth()).
		MapWith("size", "size.height", ToSizeHeight()).
		Build()
}
