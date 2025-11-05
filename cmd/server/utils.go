package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/google/uuid"
	"lychee.technology/ltbase/forma"
	"lychee.technology/ltbase/forma/internal"
)

// parsePath parses /api/v1/{schema_name} or /api/v1/{schema_name}/{row_id}
func parsePath(path string) (schemaName string, rowID string, err error) {
	path = strings.TrimPrefix(path, "/api/v1/")
	path = strings.Trim(path, "/")

	if path == "" {
		return "", "", fmt.Errorf("invalid path: empty schema name")
	}

	parts := strings.Split(path, "/")

	switch len(parts) {
	case 1:
		return parts[0], "", nil
	case 2:
		return parts[0], parts[1], nil
	default:
		return "", "", fmt.Errorf("invalid path format")
	}
}

// parseExpression parses expressions like "equals:value" or "starts_with:value"
func parseExpression(expr string) (forma.FilterType, string, error) {
	parts := strings.SplitN(expr, ":", 2)
	if len(parts) != 2 {
		// Treat as equals if no operator specified
		return forma.FilterEquals, expr, nil
	}

	filterTypeStr := parts[0]
	filterValue := parts[1]

	switch filterTypeStr {
	case "equals":
		return forma.FilterEquals, filterValue, nil
	case "not_equals":
		return forma.FilterNotEquals, filterValue, nil
	case "starts_with":
		return forma.FilterStartsWith, filterValue, nil
	case "contains":
		return forma.FilterContains, filterValue, nil
	case "gt":
		return forma.FilterGreaterThan, filterValue, nil
	case "lt":
		return forma.FilterLessThan, filterValue, nil
	case "gte":
		return forma.FilterGreaterEq, filterValue, nil
	case "lte":
		return forma.FilterLessEq, filterValue, nil
	default:
		return "", "", fmt.Errorf("unsupported filter type: %s", filterTypeStr)
	}
}

// buildFilters constructs a Filter map from query parameters
func buildFilters(queryParams url.Values, schemaName string, metadataCache *internal.MetadataCache) (map[string]forma.Filter, error) {
	filters := make(map[string]forma.Filter)

	// Reserved pagination parameters
	reservedParams := map[string]bool{
		"page":           true,
		"items_per_page": true,
		"q":              true,
		"schemas":        true,
	}

	for key, values := range queryParams {
		if reservedParams[key] {
			continue
		}

		if len(values) == 0 {
			continue
		}

		if key == "attr_name" {
			// Parse the attribute name expression
			filterType, attrName, err := parseExpression(values[0])
			if err != nil {
				return nil, err
			}

			// If schema name is provided, convert attribute name to ID
			if schemaName != "" && metadataCache != nil {
				meta, ok := metadataCache.GetAttributeMeta(schemaName, attrName)
				if !ok {
					return nil, fmt.Errorf("attribute not found: %s in schema %s", attrName, schemaName)
				}
				// Store the attribute ID filter
				filters["attr_name"] = forma.Filter{
					Field: forma.FilterFieldAttributeName,
					Type:  filterType,
					Value: meta.AttributeID,
				}
			} else {
				// Without schema name, we can't convert to ID, pass through as-is
				filters[key] = forma.Filter{
					Field: forma.FilterField(key),
					Type:  filterType,
					Value: attrName,
				}
			}
			continue
		}

		if key == "attr_value" {
			// Parse the attribute value expression
			filterType, filterValue, err := parseExpression(values[0])
			if err != nil {
				return nil, err
			}
			filters[key] = forma.Filter{
				Field: forma.FilterFieldAttributeValue,
				Type:  filterType,
				Value: filterValue,
			}
			continue
		}

		// Other filters
		filterTypeStr, filterValue, err := parseExpression(values[0])
		if err != nil {
			return nil, err
		}

		filters[key] = forma.Filter{
			Field: forma.FilterField(key),
			Type:  filterTypeStr,
			Value: filterValue,
		}
	}

	return filters, nil
}

// parsePagination extracts page and items_per_page from query parameters
func parsePagination(queryParams url.Values) (int, int) {
	page := 1
	itemsPerPage := 20

	if p := queryParams.Get("page"); p != "" {
		if parsed, err := strconv.Atoi(p); err == nil && parsed > 0 {
			page = parsed
		}
	}

	if ipp := queryParams.Get("items_per_page"); ipp != "" {
		if parsed, err := strconv.Atoi(ipp); err == nil && parsed > 0 {
			if parsed > 100 {
				parsed = 100
			}
			itemsPerPage = parsed
		}
	}

	return page, itemsPerPage
}

// APIResponse is the standard response format
type APIResponse struct {
	Success bool        `json:"success"`
	Data    interface{} `json:"data,omitempty"`
	Error   string      `json:"error,omitempty"`
}

// writeJSON writes JSON response to http.ResponseWriter
func writeJSON(w http.ResponseWriter, statusCode int, data interface{}) error {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	return json.NewEncoder(w).Encode(data)
}

// writeError writes an error response
func writeError(w http.ResponseWriter, statusCode int, message string) error {
	return writeJSON(w, statusCode, APIResponse{
		Success: false,
		Error:   message,
	})
}

// writeSuccess writes a success response
func writeSuccess(w http.ResponseWriter, statusCode int, data interface{}) error {
	return writeJSON(w, statusCode, data)
}

// parseUUID parses a UUID string
func parseUUID(s string) (uuid.UUID, error) {
	return uuid.Parse(s)
}

// readJSONBody reads and decodes JSON from request body
func readJSONBody(r *http.Request, v interface{}) error {
	defer r.Body.Close()
	return json.NewDecoder(r.Body).Decode(v)
}
