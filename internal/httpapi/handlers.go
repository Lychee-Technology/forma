package httpapi

import (
	"net/http"
	"strings"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
	"go.uber.org/zap"
)

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
		respondError(w, "invalid path", err)
		return
	}
	zap.S().Infow("create request received", "schema", schemaName)

	var rawBody any
	if err := readJSONBody(r, &rawBody); err != nil {
		respondError(w, "invalid json body", forma.InvalidInputf("%v", err))
		return
	}

	jsonObjects, isSingleObject, err := parseCreateObjects(rawBody)
	if err != nil {
		respondError(w, "invalid json body", err)
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

// executeGet performs the actual Get manager call and writes the response.
// It is handleQuery's row-id branch: GET /api/v1/{schema}/{row_id} reaches it
// through apiHandler, which is the only dispatch path into a single record.
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
		respondError(w, "invalid path", err)
		return
	}

	if rowIDStr != "" {
		rowID, err := parseUUID(rowIDStr)
		if err != nil {
			respondError(w, "invalid row_id", forma.InvalidInputf("%v", err))
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
		respondError(w, "invalid sort parameters", err)
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
		respondError(w, "invalid path", err)
		return
	}

	if rowIDStr == "" {
		_ = writeError(w, http.StatusBadRequest, "row_id is required")
		return
	}
	zap.S().Infow("update request received", "schema", schemaName, "rowID", rowIDStr)

	rowID, err := parseUUID(rowIDStr)
	if err != nil {
		respondError(w, "invalid row_id", forma.InvalidInputf("%v", err))
		return
	}

	var body map[string]any
	if err := readJSONBody(r, &body); err != nil {
		respondError(w, "invalid json body", forma.InvalidInputf("%v", err))
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
		respondError(w, "invalid path", err)
		return
	}

	var rowIDStrs []string
	if err := readJSONBody(r, &rowIDStrs); err != nil {
		respondError(w, "invalid json body", forma.InvalidInputf("%v", err))
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
			respondError(w, "invalid row_id", forma.InvalidInputf("index %d: %v", i, err))
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
		respondError(w, "invalid json body", forma.InvalidInputf("%v", err))
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
			respondError(w, "invalid path", err)
			return
		}

		if rowIDStr != "" {
			rowID, err := parseUUID(rowIDStr)
			if err != nil {
				respondError(w, "invalid row_id", forma.InvalidInputf("%v", err))
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
