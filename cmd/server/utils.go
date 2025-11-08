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

// parseSortParams extracts sorting directives from query parameters.
// Supports repeated sort_by values or comma-separated lists.
func parseSortParams(queryParams url.Values) ([]string, forma.SortOrder, error) {
	rawSortBy, hasSort := queryParams["sort_by"]
	sortOrderParam := strings.TrimSpace(queryParams.Get("sort_order"))

	if !hasSort || len(rawSortBy) == 0 {
		if sortOrderParam != "" {
			return nil, "", fmt.Errorf("sort_order requires sort_by to be specified")
		}
		return nil, "", nil
	}

	var sortFields []string
	for _, raw := range rawSortBy {
		for _, part := range strings.Split(raw, ",") {
			field := strings.TrimSpace(part)
			if field != "" {
				sortFields = append(sortFields, field)
			}
		}
	}

	if len(sortFields) == 0 {
		return nil, "", fmt.Errorf("sort_by provided but contained no valid fields")
	}

	if sortOrderParam == "" {
		return sortFields, forma.SortOrderAsc, nil
	}

	switch strings.ToLower(sortOrderParam) {
	case "asc":
		return sortFields, forma.SortOrderAsc, nil
	case "desc":
		return sortFields, forma.SortOrderDesc, nil
	default:
		return nil, "", fmt.Errorf("invalid sort_order: %s", sortOrderParam)
	}
}

// APIResponse is the standard response format
type APIResponse struct {
	Success bool   `json:"success"`
	Data    any    `json:"data,omitempty"`
	Error   string `json:"error,omitempty"`
}

// writeJSON writes JSON response to http.ResponseWriter
func writeJSON(w http.ResponseWriter, statusCode int, data any) error {
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
func writeSuccess(w http.ResponseWriter, statusCode int, data any) error {
	return writeJSON(w, statusCode, data)
}

// parseUUID parses a UUID string
func parseUUID(s string) (uuid.UUID, error) {
	return uuid.Parse(s)
}

// readJSONBody reads and decodes JSON from request body
func readJSONBody(r *http.Request, v any) error {
	defer r.Body.Close()
	return json.NewDecoder(r.Body).Decode(v)
}
