package main

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// identityMapper passes through string values unchanged.
type identityMapper struct{}

func (m *identityMapper) Map(csvValue string) (any, error) {
	return csvValue, nil
}

// Identity returns a mapper that passes through string values unchanged.
func Identity() FieldMapper {
	return &identityMapper{}
}

// toStringMapper ensures the value is a string.
type toStringMapper struct{}

func (m *toStringMapper) Map(csvValue string) (any, error) {
	return csvValue, nil
}

// ToString returns a mapper that ensures the value is a string.
func ToString() FieldMapper {
	return &toStringMapper{}
}

// toIntMapper converts string to int.
type toIntMapper struct{}

func (m *toIntMapper) Map(csvValue string) (any, error) {
	v := strings.TrimSpace(csvValue)
	if v == "" {
		return nil, nil
	}
	i, err := strconv.Atoi(v)
	if err != nil {
		return nil, fmt.Errorf("invalid integer format: %v", err)
	}
	return i, nil
}

// ToInt returns a mapper that converts string to int.
func ToInt() FieldMapper {
	return &toIntMapper{}
}

// toInt64Mapper converts string to int64.
type toInt64Mapper struct{}

func (m *toInt64Mapper) Map(csvValue string) (any, error) {
	v := strings.TrimSpace(csvValue)
	if v == "" {
		return nil, nil
	}
	i, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid int64 format: %v", err)
	}
	return i, nil
}

// ToInt64 returns a mapper that converts string to int64.
func ToInt64() FieldMapper {
	return &toInt64Mapper{}
}

// toFloat64Mapper converts string to float64.
type toFloat64Mapper struct{}

func (m *toFloat64Mapper) Map(csvValue string) (any, error) {
	v := strings.TrimSpace(csvValue)
	if v == "" {
		return nil, nil
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid float64 format: %v", err)
	}
	return f, nil
}

// ToFloat64 returns a mapper that converts string to float64.
func ToFloat64() FieldMapper {
	return &toFloat64Mapper{}
}

// toBoolMapper converts string to bool.
// Accepts: "true", "false", "1", "0", "yes", "no" (case-insensitive).
type toBoolMapper struct{}

func (m *toBoolMapper) Map(csvValue string) (any, error) {
	v := strings.TrimSpace(strings.ToLower(csvValue))
	if v == "" {
		return nil, nil
	}
	switch v {
	case "true", "1", "yes":
		return true, nil
	case "false", "0", "no":
		return false, nil
	default:
		return nil, fmt.Errorf("invalid boolean value: %q (expected true/false/1/0/yes/no)", csvValue)
	}
}

// ToBool returns a mapper that converts string to bool.
func ToBool() FieldMapper {
	return &toBoolMapper{}
}

// toDateMapper converts string to time.Time (date only).
type toDateMapper struct {
	layout string
}

func (m *toDateMapper) Map(csvValue string) (any, error) {
	v := strings.TrimSpace(csvValue)
	if v == "" {
		return nil, nil
	}
	t, err := time.Parse(m.layout, v)
	if err != nil {
		return nil, fmt.Errorf("invalid date format (expected %s): %v", m.layout, err)
	}
	// Return date string in ISO format for JSON compatibility
	return t.Format("2006-01-02"), nil
}

// ToDate returns a mapper that converts string to date using the specified layout.
// Common layouts:
//   - "2006-01-02" for YYYY-MM-DD
//   - "01/02/2006" for MM/DD/YYYY
//   - "02-Jan-2006" for DD-Mon-YYYY
func ToDate(layout string) FieldMapper {
	return &toDateMapper{layout: layout}
}

// toDateTimeMapper converts string to time.Time (datetime).
type toDateTimeMapper struct {
	layout string
}

func (m *toDateTimeMapper) Map(csvValue string) (any, error) {
	v := strings.TrimSpace(csvValue)
	if v == "" {
		return nil, nil
	}
	t, err := time.Parse(m.layout, v)
	if err != nil {
		return nil, fmt.Errorf("invalid datetime format (expected %s): %v", m.layout, err)
	}
	// Return datetime string in ISO8601 format for JSON compatibility
	return t.Format(time.RFC3339), nil
}

// ToDateTime returns a mapper that converts string to datetime using the specified layout.
// Common layouts:
//   - "2006-01-02 15:04:05" for YYYY-MM-DD HH:MM:SS
//   - "2006-01-02T15:04:05" for ISO8601 without timezone
//   - time.RFC3339 for ISO8601 with timezone
func ToDateTime(layout string) FieldMapper {
	return &toDateTimeMapper{layout: layout}
}

// ToDateTimeISO8601 returns a mapper that parses ISO8601 datetime strings.
func ToDateTimeISO8601() FieldMapper {
	return &iso8601DateTimeMapper{}
}

// iso8601DateTimeMapper attempts to parse various ISO8601 formats.
type iso8601DateTimeMapper struct{}

func (m *iso8601DateTimeMapper) Map(csvValue string) (any, error) {
	v := strings.TrimSpace(csvValue)
	if v == "" {
		return nil, nil
	}

	// Try various ISO8601 formats
	layouts := []string{
		time.RFC3339,
		time.RFC3339Nano,
		"2006-01-02T15:04:05",
		"2006-01-02T15:04:05Z",
		"2006-01-02 15:04:05",
		"2006-01-02",
	}

	for _, layout := range layouts {
		t, err := time.Parse(layout, v)
		if err == nil {
			return t.Format(time.RFC3339), nil
		}
	}

	return nil, fmt.Errorf("invalid ISO8601 datetime format: %q", csvValue)
}

// splitMapper splits a string by a separator.
type splitMapper struct {
	separator string
}

func (m *splitMapper) Map(csvValue string) (any, error) {
	v := strings.TrimSpace(csvValue)
	if v == "" {
		return []string{}, nil
	}
	parts := strings.Split(v, m.separator)
	// Trim whitespace from each part
	result := make([]string, 0, len(parts))
	for _, part := range parts {
		trimmed := strings.TrimSpace(part)
		if trimmed != "" {
			result = append(result, trimmed)
		}
	}
	return result, nil
}

// Split returns a mapper that splits a string by the specified separator.
func Split(separator string) FieldMapper {
	return &splitMapper{separator: separator}
}

// customMapper wraps a custom transformation function.
type customMapper struct {
	fn func(string) (any, error)
}

func (m *customMapper) Map(csvValue string) (any, error) {
	return m.fn(csvValue)
}

// Custom returns a mapper that uses a custom transformation function.
func Custom(fn func(string) (any, error)) FieldMapper {
	return &customMapper{fn: fn}
}

// enumMapper validates that the value is one of the allowed values.
type enumMapper struct {
	allowed map[string]bool
}

func (m *enumMapper) Map(csvValue string) (any, error) {
	v := strings.TrimSpace(csvValue)
	if v == "" {
		return nil, nil
	}
	if !m.allowed[v] {
		keys := make([]string, 0, len(m.allowed))
		for k := range m.allowed {
			keys = append(keys, k)
		}
		return nil, fmt.Errorf("invalid value %q: must be one of %v", csvValue, keys)
	}
	return v, nil
}

// Enum returns a mapper that validates the value is one of the allowed values.
func Enum(allowed ...string) FieldMapper {
	m := &enumMapper{allowed: make(map[string]bool)}
	for _, a := range allowed {
		m.allowed[a] = true
	}
	return m
}

// defaultMapper provides a default value if the input is empty.
type defaultMapper struct {
	defaultValue any
	inner        FieldMapper
}

func (m *defaultMapper) Map(csvValue string) (any, error) {
	v := strings.TrimSpace(csvValue)
	if v == "" {
		return m.defaultValue, nil
	}
	if m.inner != nil {
		return m.inner.Map(csvValue)
	}
	return v, nil
}

// Default returns a mapper that uses a default value if the input is empty.
func Default(defaultValue any) FieldMapper {
	return &defaultMapper{defaultValue: defaultValue}
}

// DefaultWith returns a mapper that uses a default value if empty, otherwise applies the inner mapper.
func DefaultWith(defaultValue any, inner FieldMapper) FieldMapper {
	return &defaultMapper{defaultValue: defaultValue, inner: inner}
}

// trimMapper trims whitespace and optionally applies another mapper.
type trimMapper struct {
	inner FieldMapper
}

func (m *trimMapper) Map(csvValue string) (any, error) {
	v := strings.TrimSpace(csvValue)
	if m.inner != nil {
		return m.inner.Map(v)
	}
	return v, nil
}

// Trim returns a mapper that trims whitespace.
func Trim() FieldMapper {
	return &trimMapper{}
}

// TrimWith returns a mapper that trims whitespace and then applies the inner mapper.
func TrimWith(inner FieldMapper) FieldMapper {
	return &trimMapper{inner: inner}
}

// toLowerMapper converts string to lowercase.
type toLowerMapper struct{}

func (m *toLowerMapper) Map(csvValue string) (any, error) {
	return strings.ToLower(strings.TrimSpace(csvValue)), nil
}

// ToLower returns a mapper that converts string to lowercase.
func ToLower() FieldMapper {
	return &toLowerMapper{}
}

// toUpperMapper converts string to uppercase.
type toUpperMapper struct{}

func (m *toUpperMapper) Map(csvValue string) (any, error) {
	return strings.ToUpper(strings.TrimSpace(csvValue)), nil
}

// ToUpper returns a mapper that converts string to uppercase.
func ToUpper() FieldMapper {
	return &toUpperMapper{}
}
