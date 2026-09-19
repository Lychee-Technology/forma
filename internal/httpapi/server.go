package httpapi

import (
	"encoding/json"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
)

type Options struct {
	EnableHealth bool
}

type Manager interface {
	forma.EntityWriter
	forma.EntityReader
	forma.EntityBatchOperator
}

type Server struct {
	manager Manager
	mux     *http.ServeMux
	opts    Options
}

func NewServer(manager Manager, opts Options) *Server {
	server := &Server{
		manager: manager,
		mux:     http.NewServeMux(),
		opts:    opts,
	}
	server.registerRoutes()
	return server
}

func (s *Server) Handler() http.Handler {
	return s.mux
}

func (s *Server) registerRoutes() {
	if s.opts.EnableHealth {
		s.mux.HandleFunc("/health", s.handleHealth)
	}
	s.mux.HandleFunc("/api/v1/advanced_query", s.handleAdvancedQuery)
	s.mux.HandleFunc("/api/v1/search", s.handleSearch)
	s.mux.HandleFunc("/api/v1/", s.apiHandler)
}

// parsePath parses /api/v1/{schema_name} or /api/v1/{schema_name}/{row_id}
func parsePath(path string) (schemaName string, rowID string, err error) {
	path = strings.TrimPrefix(path, "/api/v1/")
	path = strings.Trim(path, "/")

	if path == "" {
		return "", "", forma.InvalidInputf("empty schema name")
	}

	parts := strings.Split(path, "/")

	switch len(parts) {
	case 1:
		return parts[0], "", nil
	case 2:
		return parts[0], parts[1], nil
	default:
		return "", "", forma.InvalidInputf("invalid path format")
	}
}

// parsePagination extracts page and items_per_page from query parameters.
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
			return nil, "", forma.InvalidInputf("sort_order requires sort_by to be specified")
		}
		return nil, "", nil
	}

	var sortFields []string
	for _, raw := range rawSortBy {
		for part := range strings.SplitSeq(raw, ",") {
			field := strings.TrimSpace(part)
			if field != "" {
				sortFields = append(sortFields, field)
			}
		}
	}

	if len(sortFields) == 0 {
		return nil, "", forma.InvalidInputf("sort_by provided but contained no valid fields")
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
		return nil, "", forma.InvalidInputf("invalid sort_order: %s", sortOrderParam)
	}
}

// APIResponse is the standard response format.
type APIResponse struct {
	Success bool   `json:"success"`
	Data    any    `json:"data,omitempty"`
	Error   string `json:"error,omitempty"`
	// ErrorClass is populated on every redacted response and only there (#301) —
	// the stable machine token clients discriminate on. ErrorID, a correlation
	// id echoed on the operator log line that holds the error chain, appears on
	// every redacted response and, since #361, on a disclosed 4xx that withholds
	// operator detail (forma.HasOperatorDetail), where it matches the Warnw line
	// keeping the only copy of that detail. Since #313 a redacted response can
	// be a 4xx as well as a 5xx: an error that carries a client sentinel but
	// publishes no message (a bare sentinel wrap, or a carrier-less mixed chain)
	// keeps its status and loses its body. Both fields are omitempty, so success
	// bodies and detail-less published 4xx bodies are unchanged.
	ErrorClass string `json:"error_class,omitempty"`
	ErrorID    string `json:"error_id,omitempty"`
	// SchemaID names the schema a redacted read failure was addressed to (#301,
	// reinstated by the issue owner after the design excluded it — see
	// errorSchemaID and docs/error-handling.md). omitempty is a lossless "absent"
	// encoding because schema IDs are always positive here, the same invariant
	// that makes a manifest schema_id of zero mean unstamped rather than schema 0.
	SchemaID int16 `json:"schema_id,omitempty"`
}

// writeJSON writes JSON response to http.ResponseWriter.
func writeJSON(w http.ResponseWriter, statusCode int, data any) error {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	return json.NewEncoder(w).Encode(data)
}

// writeSuccess writes a success response.
func writeSuccess(w http.ResponseWriter, statusCode int, data any) error {
	return writeJSON(w, statusCode, data)
}

// parseUUID parses a UUID string. Its error is google/uuid's own prose about
// the malformed literal the caller sent — caller-addressed parse feedback
// carrying no operator data — so call sites publish it deliberately via
// forma.InvalidInputf("%v", err) and route it through respondError (#360); the
// gate's scrub still applies to it.
func parseUUID(s string) (uuid.UUID, error) {
	return uuid.Parse(s)
}

// readJSONBody reads and decodes JSON from the request body. Numeric literals
// decode as json.Number so integer attribute values above 2^53 reach any-typed
// sinks (entity data maps) undamaged (#205, #282); decoding into typed struct
// fields is unaffected by UseNumber.
//
// Its error is encoding/json's own prose — caller-addressed parse feedback
// carrying no operator data — so call sites publish it deliberately via
// forma.InvalidInputf("%v", err) and route it through respondError (#360); the
// gate's scrub still applies to it.
func readJSONBody(r *http.Request, v any) error {
	defer r.Body.Close()
	dec := json.NewDecoder(r.Body)
	dec.UseNumber()
	return dec.Decode(v)
}

// parseCreateObjects parses create payloads that can be either a single object or an object array.
func parseCreateObjects(rawBody any) ([]map[string]any, bool, error) {
	switch v := rawBody.(type) {
	case map[string]any:
		return []map[string]any{v}, true, nil
	case []any:
		if len(v) == 0 {
			return nil, false, forma.InvalidInputf("empty array not allowed")
		}

		objects := make([]map[string]any, len(v))
		for i, item := range v {
			obj, ok := item.(map[string]any)
			if !ok {
				return nil, false, forma.InvalidInputf("body[%d] must be an object", i)
			}
			objects[i] = obj
		}
		return objects, false, nil
	default:
		return nil, false, forma.InvalidInputf("body must be an object or array")
	}
}

// parseCSVParam parses comma-separated query params, trims values, and removes duplicates.
func parseCSVParam(raw string) []string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}

	parts := strings.Split(raw, ",")
	values := make([]string, 0, len(parts))
	seen := make(map[string]struct{}, len(parts))
	for _, part := range parts {
		v := strings.TrimSpace(part)
		if v == "" {
			continue
		}
		if _, exists := seen[v]; exists {
			continue
		}
		seen[v] = struct{}{}
		values = append(values, v)
	}

	if len(values) == 0 {
		return nil
	}
	return values
}

// parseAttrs extracts the attrs parameter from query parameters.
// attrs is a comma-separated list of attribute names (JSON paths) to return.
// Returns nil if attrs is not specified or empty.
func parseAttrs(queryParams url.Values) []string {
	attrsParam := strings.TrimSpace(queryParams.Get("attrs"))
	if attrsParam == "" {
		return nil
	}

	var attrs []string
	for attr := range strings.SplitSeq(attrsParam, ",") {
		attr = strings.TrimSpace(attr)
		if attr != "" {
			attrs = append(attrs, attr)
		}
	}

	return attrs
}
