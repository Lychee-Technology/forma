package httpapi

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
	"go.uber.org/zap"
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

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		_ = writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	_ = writeSuccess(w, http.StatusOK, map[string]string{
		"status": "healthy",
	})
}

// handleCreate handles POST /api/v1/{schema_name}
func (s *Server) handleCreate(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		_ = writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	schemaName, _, err := parsePath(r.URL.Path)
	if err != nil {
		_ = writeError(w, http.StatusBadRequest, fmt.Sprintf("invalid path: %v", err))
		return
	}
	zap.S().Infow("create request received", "schema", schemaName)

	var rawBody any
	if err := readEntityJSONBody(r, &rawBody); err != nil {
		_ = writeError(w, http.StatusBadRequest, fmt.Sprintf("invalid json body: %v", err))
		return
	}

	jsonObjects, isSingleObject, err := parseCreateObjects(rawBody)
	if err != nil {
		_ = writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	zap.S().Debugw("create payload parsed", "schema", schemaName, "records", len(jsonObjects))

	operations := make([]forma.EntityOperation, len(jsonObjects))
	for i, obj := range jsonObjects {
		operations[i] = forma.EntityOperation{
			Type: forma.OperationCreate,
			EntityIdentifier: forma.EntityIdentifier{
				SchemaName: schemaName,
			},
			Data: obj,
		}
	}

	batchOp := &forma.BatchOperation{
		Operations: operations,
		Atomic:     true,
	}

	result, err := s.manager.BatchCreate(r.Context(), batchOp)
	if err != nil {
		respondError(w, "batch create failed", err, "schema", schemaName)
		return
	}
	zap.S().Infow("create request completed", "schema", schemaName, "successful", len(result.Successful), "failed", len(result.Failed))

	if isSingleObject && len(result.Successful) > 0 {
		singleResult := map[string]any{
			"row_id":      result.Successful[0].RowID.String(),
			"schema_name": result.Successful[0].SchemaName,
			"attributes":  result.Successful[0].Attributes,
		}
		_ = writeSuccess(w, http.StatusCreated, singleResult)
		return
	}

	_ = writeSuccess(w, http.StatusCreated, result)
}

// handleGet handles GET /api/v1/{schema_name}/{row_id}
func (s *Server) handleGet(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		_ = writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	schemaName, rowIDStr, err := parsePath(r.URL.Path)
	if err != nil {
		_ = writeError(w, http.StatusBadRequest, fmt.Sprintf("invalid path: %v", err))
		return
	}

	if rowIDStr == "" {
		_ = writeError(w, http.StatusBadRequest, "row_id is required")
		return
	}
	zap.S().Infow("get request received", "schema", schemaName, "rowID", rowIDStr)

	rowID, err := parseUUID(rowIDStr)
	if err != nil {
		_ = writeError(w, http.StatusBadRequest, fmt.Sprintf("invalid row_id: %v", err))
		return
	}

	attrs := parseAttrs(r.URL.Query())
	s.executeGet(w, r, schemaName, rowID, attrs)
}

// executeGet performs the actual Get manager call and writes the response.
// Extracted so that handleQuery can invoke the same logic without re-entering
// handleGet through the http.Handler interface (re-entry pattern).
func (s *Server) executeGet(w http.ResponseWriter, r *http.Request, schemaName string, rowID uuid.UUID, attrs []string) {
	queryReq := &forma.QueryRequest{
		SchemaName: schemaName,
		RowID:      &rowID,
		Attrs:      attrs,
	}

	record, err := s.manager.Get(r.Context(), queryReq)
	if err != nil {
		status := classifyManagerError(err)
		msg := "get failed"
		if status == http.StatusNotFound {
			msg = "record not found"
		}
		respondErrorWithStatus(w, status, msg, err, "schema", schemaName, "rowID", rowID.String())
		return
	}
	zap.S().Infow("get request completed", "schema", schemaName, "rowID", rowID.String(), "attrs", attrs)

	_ = writeSuccess(w, http.StatusOK, record)
}

// handleQuery handles GET /api/v1/{schema_name}?page=...&items_per_page=...&filters=...&attrs=...
func (s *Server) handleQuery(w http.ResponseWriter, r *http.Request) {
	zap.S().Infow("query request received", "path", r.URL.Path, "rawQuery", r.URL.RawQuery)

	if r.Method != http.MethodGet {
		_ = writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	schemaName, rowIDStr, err := parsePath(r.URL.Path)
	if err != nil {
		_ = writeError(w, http.StatusBadRequest, fmt.Sprintf("invalid path: %v", err))
		return
	}

	if rowIDStr != "" {
		rowID, err := parseUUID(rowIDStr)
		if err != nil {
			_ = writeError(w, http.StatusBadRequest, fmt.Sprintf("invalid row_id: %v", err))
			return
		}
		zap.S().Infow("get request received", "schema", schemaName, "rowID", rowIDStr)
		s.executeGet(w, r, schemaName, rowID, parseAttrs(r.URL.Query()))
		return
	}

	queryParams := r.URL.Query()
	page, itemsPerPage := parsePagination(queryParams)

	sortFields, sortOrder, err := parseSortParams(queryParams)
	if err != nil {
		_ = writeError(w, http.StatusBadRequest, fmt.Sprintf("invalid sort parameters: %v", err))
		return
	}

	attrs := parseAttrs(queryParams)

	queryReq := &forma.QueryRequest{
		SchemaName:   schemaName,
		Page:         page,
		ItemsPerPage: itemsPerPage,
		Attrs:        attrs,
	}

	if len(sortFields) > 0 {
		queryReq.SortBy = sortFields
		queryReq.SortOrder = sortOrder
	}
	zap.S().Infow("query request received", "schema", schemaName, "page", page, "itemsPerPage", itemsPerPage, "sortBy", sortFields, "sortOrder", sortOrder, "attrs", attrs)

	result, err := s.manager.Query(r.Context(), queryReq)
	if err != nil {
		respondError(w, "query failed", err, "schema", schemaName, "page", page, "itemsPerPage", itemsPerPage)
		return
	}
	zap.S().Infow("query request completed", "schema", schemaName, "page", page, "itemsPerPage", itemsPerPage, "returned", len(result.Data), "total", result.TotalRecords)

	_ = writeSuccess(w, http.StatusOK, result)
}

// handleUpdate handles PUT /api/v1/{schema_name}/{row_id}
func (s *Server) handleUpdate(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPut {
		_ = writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	schemaName, rowIDStr, err := parsePath(r.URL.Path)
	if err != nil {
		_ = writeError(w, http.StatusBadRequest, fmt.Sprintf("invalid path: %v", err))
		return
	}

	if rowIDStr == "" {
		_ = writeError(w, http.StatusBadRequest, "row_id is required")
		return
	}
	zap.S().Infow("update request received", "schema", schemaName, "rowID", rowIDStr)

	rowID, err := parseUUID(rowIDStr)
	if err != nil {
		_ = writeError(w, http.StatusBadRequest, fmt.Sprintf("invalid row_id: %v", err))
		return
	}

	var body map[string]any
	if err := readEntityJSONBody(r, &body); err != nil {
		_ = writeError(w, http.StatusBadRequest, fmt.Sprintf("invalid json body: %v", err))
		return
	}

	operation := &forma.EntityOperation{
		Type: forma.OperationUpdate,
		EntityIdentifier: forma.EntityIdentifier{
			SchemaName: schemaName,
			RowID:      rowID,
		},
		Data:    body,
		Updates: body,
	}

	record, err := s.manager.Update(r.Context(), operation)
	if err != nil {
		respondError(w, "update failed", err, "schema", schemaName, "rowID", rowIDStr)
		return
	}
	zap.S().Infow("update request completed", "schema", schemaName, "rowID", rowIDStr)

	_ = writeSuccess(w, http.StatusOK, record)
}

// handleSingleDelete handles DELETE for a single row_id
func (s *Server) handleSingleDelete(w http.ResponseWriter, r *http.Request, schemaName string, rowID uuid.UUID) {
	operation := forma.EntityOperation{
		Type: forma.OperationDelete,
		EntityIdentifier: forma.EntityIdentifier{
			SchemaName: schemaName,
			RowID:      rowID,
		},
	}

	batchOp := &forma.BatchOperation{
		Operations: []forma.EntityOperation{operation},
		Atomic:     true,
	}

	result, err := s.manager.BatchDelete(r.Context(), batchOp)
	if err != nil {
		respondError(w, "delete failed", err, "schema", schemaName, "rowID", rowID.String())
		return
	}
	zap.S().Infow("delete request completed", "schema", schemaName, "rowID", rowID.String())

	_ = writeSuccess(w, http.StatusOK, result)
}

// handleDelete handles DELETE /api/v1/{schema_name}
func (s *Server) handleDelete(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodDelete {
		_ = writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	schemaName, _, err := parsePath(r.URL.Path)
	if err != nil {
		_ = writeError(w, http.StatusBadRequest, fmt.Sprintf("invalid path: %v", err))
		return
	}

	var rowIDStrs []string
	if err := readJSONBody(r, &rowIDStrs); err != nil {
		_ = writeError(w, http.StatusBadRequest, fmt.Sprintf("invalid json body: %v", err))
		return
	}

	if len(rowIDStrs) == 0 {
		_ = writeError(w, http.StatusBadRequest, "empty row_id array not allowed")
		return
	}
	zap.S().Infow("batch delete request received", "schema", schemaName, "count", len(rowIDStrs))

	operations := make([]forma.EntityOperation, len(rowIDStrs))
	for i, idStr := range rowIDStrs {
		rowID, err := parseUUID(idStr)
		if err != nil {
			_ = writeError(w, http.StatusBadRequest, fmt.Sprintf("invalid row_id at index %d: %v", i, err))
			return
		}

		operations[i] = forma.EntityOperation{
			Type: forma.OperationDelete,
			EntityIdentifier: forma.EntityIdentifier{
				SchemaName: schemaName,
				RowID:      rowID,
			},
		}
	}

	batchOp := &forma.BatchOperation{
		Operations: operations,
		Atomic:     true,
	}

	result, err := s.manager.BatchDelete(r.Context(), batchOp)
	if err != nil {
		respondError(w, "batch delete failed", err, "schema", schemaName, "requested", len(rowIDStrs))
		return
	}
	zap.S().Infow("batch delete request completed", "schema", schemaName, "requested", len(rowIDStrs))

	_ = writeSuccess(w, http.StatusOK, result)
}

// handleSearch handles GET /api/v1/search?page=...&items_per_page=...&q=...&attrs=...
func (s *Server) handleSearch(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		_ = writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	queryParams := r.URL.Query()
	page, itemsPerPage := parsePagination(queryParams)

	schemaNames := parseCSVParam(queryParams.Get("schemas"))
	searchTerm := strings.TrimSpace(queryParams.Get("q"))

	if len(schemaNames) == 0 {
		_ = writeError(w, http.StatusBadRequest, "schemas query parameter is required")
		return
	}
	if searchTerm == "" {
		_ = writeError(w, http.StatusBadRequest, "q query parameter is required")
		return
	}

	attrs := parseAttrs(queryParams)

	crossSchemaReq := &forma.CrossSchemaRequest{
		SchemaNames:  schemaNames,
		SearchTerm:   searchTerm,
		Page:         page,
		ItemsPerPage: itemsPerPage,
		Attrs:        attrs,
	}
	zap.S().Infow("search request received", "schemas", schemaNames, "searchTerm", crossSchemaReq.SearchTerm, "page", page, "itemsPerPage", itemsPerPage, "attrs", attrs)

	result, err := s.manager.CrossSchemaSearch(r.Context(), crossSchemaReq)
	if err != nil {
		respondError(w, "cross-schema search failed", err, "schemas", schemaNames, "page", page)
		return
	}
	zap.S().Infow("search request completed", "schemas", schemaNames, "page", page, "itemsPerPage", itemsPerPage, "returned", len(result.Data), "total", result.TotalRecords)

	_ = writeSuccess(w, http.StatusOK, result)
}

// handleAdvancedQuery handles POST /api/v1/advanced_query?attrs=...
func (s *Server) handleAdvancedQuery(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		_ = writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	var payload forma.QueryRequest
	if err := readJSONBody(r, &payload); err != nil {
		_ = writeError(w, http.StatusBadRequest, fmt.Sprintf("invalid json body: %v", err))
		return
	}

	if payload.SchemaName == "" {
		_ = writeError(w, http.StatusBadRequest, "schema_name is required")
		return
	}

	if payload.Condition == nil {
		_ = writeError(w, http.StatusBadRequest, "condition is required")
		return
	}

	queryParams := r.URL.Query()
	urlAttrs := parseAttrs(queryParams)
	if len(urlAttrs) > 0 {
		payload.Attrs = urlAttrs
	}

	zap.S().Infow("advanced query request received", "schema", payload.SchemaName, "page", payload.Page, "itemsPerPage", payload.ItemsPerPage, "attrs", payload.Attrs)

	result, err := s.manager.Query(r.Context(), &payload)
	if err != nil {
		respondError(w, "advanced query failed", err, "schema", payload.SchemaName, "page", payload.Page)
		return
	}
	zap.S().Infow("advanced query request completed", "schema", payload.SchemaName, "page", payload.Page, "itemsPerPage", payload.ItemsPerPage, "returned", len(result.Data), "total", result.TotalRecords)

	_ = writeSuccess(w, http.StatusOK, result)
}

// apiHandler is the main router that dispatches to specific handlers
func (s *Server) apiHandler(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Path

	zap.S().Infow("handling request", "path", path, "method", r.Method)

	if r.Method == http.MethodDelete {
		schemaName, rowIDStr, err := parsePath(path)
		if err != nil {
			_ = writeError(w, http.StatusBadRequest, fmt.Sprintf("invalid path: %v", err))
			return
		}

		if rowIDStr != "" {
			rowID, err := parseUUID(rowIDStr)
			if err != nil {
				_ = writeError(w, http.StatusBadRequest, fmt.Sprintf("invalid row_id: %v", err))
				return
			}
			s.handleSingleDelete(w, r, schemaName, rowID)
			return
		}
	}

	switch r.Method {
	case http.MethodPost:
		s.handleCreate(w, r)
	case http.MethodGet:
		s.handleQuery(w, r)
	case http.MethodPut:
		s.handleUpdate(w, r)
	case http.MethodDelete:
		s.handleDelete(w, r)
	default:
		_ = writeError(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}

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
			return nil, "", fmt.Errorf("sort_order requires sort_by to be specified")
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

// APIResponse is the standard response format.
type APIResponse struct {
	Success bool   `json:"success"`
	Data    any    `json:"data,omitempty"`
	Error   string `json:"error,omitempty"`
	// ErrorClass and ErrorID are populated only on redacted 5xx responses
	// (#301): a stable machine token for client discrimination, and a
	// correlation id echoed on the operator log line that holds the full error
	// chain. Both are omitempty, so success and 4xx bodies are unchanged.
	ErrorClass string `json:"error_class,omitempty"`
	ErrorID    string `json:"error_id,omitempty"`
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

// parseUUID parses a UUID string.
func parseUUID(s string) (uuid.UUID, error) {
	return uuid.Parse(s)
}

// readJSONBody reads and decodes JSON from request body.
func readJSONBody(r *http.Request, v any) error {
	defer r.Body.Close()
	return json.NewDecoder(r.Body).Decode(v)
}

// readEntityJSONBody decodes an entity data payload with json.Number for
// numeric literals, so integer attribute values above 2^53 reach the
// transform chain undamaged (#205). Non-entity payloads (row-id arrays,
// typed query requests) keep readJSONBody's plain decoding.
func readEntityJSONBody(r *http.Request, v any) error {
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
			return nil, false, fmt.Errorf("empty array not allowed")
		}

		objects := make([]map[string]any, len(v))
		for i, item := range v {
			obj, ok := item.(map[string]any)
			if !ok {
				return nil, false, fmt.Errorf("body[%d] must be an object", i)
			}
			objects[i] = obj
		}
		return objects, false, nil
	default:
		return nil, false, fmt.Errorf("body must be an object or array")
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
