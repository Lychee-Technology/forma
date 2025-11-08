package main

import (
	"fmt"
	"log"
	"net/http"

	"github.com/google/uuid"
	"lychee.technology/ltbase/forma"
)

// handleCreate handles POST /api/v1/{schema_name}
func (s *Server) handleCreate(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	schemaName, _, err := parsePath(r.URL.Path)
	if err != nil {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("invalid path: %v", err))
		return
	}

	// Try to read as single object or array
	var rawBody any
	if err := readJSONBody(r, &rawBody); err != nil {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("invalid json body: %v", err))
		return
	}

	// Convert to array format
	var jsonObjects []any
	isSingleObject := false
	switch v := rawBody.(type) {
	case map[string]any:
		// Single object, convert to array
		jsonObjects = []any{v}
		isSingleObject = true
	case []any:
		// Already an array
		jsonObjects = v
	default:
		writeError(w, http.StatusBadRequest, "body must be an object or array")
		return
	}

	if len(jsonObjects) == 0 {
		writeError(w, http.StatusBadRequest, "empty array not allowed")
		return
	}

	// Build batch operation
	operations := make([]forma.EntityOperation, len(jsonObjects))
	for i, obj := range jsonObjects {
		operations[i] = forma.EntityOperation{
			Type: forma.OperationCreate,
			EntityIdentifier: forma.EntityIdentifier{
				SchemaName: schemaName,
				RowID:      uuid.New(),
			},
			Data: obj.(map[string]any),
		}
	}

	batchOp := &forma.BatchOperation{
		Operations: operations,
		Atomic:     true,
	}

	result, err := s.manager.BatchCreate(r.Context(), batchOp)
	if err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("batch create failed: %v", err))
		return
	}

	// If single object request, return single result with row_id
	if isSingleObject && len(result.Successful) > 0 {
		singleResult := map[string]any{
			"row_id":      result.Successful[0].RowID.String(),
			"schema_name": result.Successful[0].SchemaName,
			"attributes":  result.Successful[0].Attributes,
		}
		writeSuccess(w, http.StatusCreated, singleResult)
		return
	}

	// Return batch result
	writeSuccess(w, http.StatusCreated, result)
}

// handleGet handles GET /api/v1/{schema_name}/{row_id}
func (s *Server) handleGet(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	schemaName, rowIDStr, err := parsePath(r.URL.Path)
	if err != nil {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("invalid path: %v", err))
		return
	}

	if rowIDStr == "" {
		writeError(w, http.StatusBadRequest, "row_id is required")
		return
	}

	rowID, err := parseUUID(rowIDStr)
	if err != nil {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("invalid row_id: %v", err))
		return
	}

	queryReq := &forma.QueryRequest{
		SchemaName: schemaName,
		RowID:      &rowID,
	}

	record, err := s.manager.Get(r.Context(), queryReq)
	if err != nil {
		writeError(w, http.StatusNotFound, fmt.Sprintf("record not found: %v", err))
		return
	}

	writeSuccess(w, http.StatusOK, record)
}

// handleQuery handles GET /api/v1/{schema_name}?page=...&items_per_page=...&filters=...
func (s *Server) handleQuery(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	schemaName, rowIDStr, err := parsePath(r.URL.Path)
	if err != nil {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("invalid path: %v", err))
		return
	}

	// If rowID is present, it's a single get request
	if rowIDStr != "" {
		s.handleGet(w, r)
		return
	}

	// Parse query parameters
	queryParams := r.URL.Query()
	page, itemsPerPage := parsePagination(queryParams)

	sortFields, sortOrder, err := parseSortParams(queryParams)
	if err != nil {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("invalid sort parameters: %v", err))
		return
	}

	queryReq := &forma.QueryRequest{
		SchemaName:   schemaName,
		Page:         page,
		ItemsPerPage: itemsPerPage,
	}

	if len(sortFields) > 0 {
		queryReq.SortBy = sortFields
		queryReq.SortOrder = sortOrder
	}

	result, err := s.manager.Query(r.Context(), queryReq)
	if err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("query failed: %v", err))
		return
	}

	writeSuccess(w, http.StatusOK, result)
}

// handleUpdate handles PUT /api/v1/{schema_name}/{row_id}
func (s *Server) handleUpdate(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPut {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	schemaName, rowIDStr, err := parsePath(r.URL.Path)
	if err != nil {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("invalid path: %v", err))
		return
	}

	if rowIDStr == "" {
		writeError(w, http.StatusBadRequest, "row_id is required")
		return
	}

	rowID, err := parseUUID(rowIDStr)
	if err != nil {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("invalid row_id: %v", err))
		return
	}

	// Read JSON from body (can be full object or partial updates)
	var body map[string]any
	if err := readJSONBody(r, &body); err != nil {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("invalid json body: %v", err))
		return
	}

	// Use Data field to represent complete object update
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
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("update failed: %v", err))
		return
	}

	writeSuccess(w, http.StatusOK, record)
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
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("delete failed: %v", err))
		return
	}

	writeSuccess(w, http.StatusOK, result)
}

// handleDelete handles DELETE /api/v1/{schema_name}
func (s *Server) handleDelete(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodDelete {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	schemaName, _, err := parsePath(r.URL.Path)
	if err != nil {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("invalid path: %v", err))
		return
	}

	// Read array of row IDs from body
	var rowIDStrs []string
	if err := readJSONBody(r, &rowIDStrs); err != nil {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("invalid json body: %v", err))
		return
	}

	if len(rowIDStrs) == 0 {
		writeError(w, http.StatusBadRequest, "empty row_id array not allowed")
		return
	}

	// Convert string IDs to UUIDs and build operations
	operations := make([]forma.EntityOperation, len(rowIDStrs))
	for i, idStr := range rowIDStrs {
		rowID, err := parseUUID(idStr)
		if err != nil {
			writeError(w, http.StatusBadRequest, fmt.Sprintf("invalid row_id at index %d: %v", i, err))
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
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("batch delete failed: %v", err))
		return
	}

	writeSuccess(w, http.StatusOK, result)
}

// handleSearch handles GET /api/v1/search?page=...&items_per_page=...&q=...
func (s *Server) handleSearch(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	// Parse query parameters
	queryParams := r.URL.Query()
	page, itemsPerPage := parsePagination(queryParams)

	// Get all schema names from query parameters
	// Use CrossSchemaRequest for cross-schema search
	// If specific schemas are needed, they could be passed as a query parameter
	schemaNames := []string{}
	if schemasParam := queryParams.Get("schemas"); schemasParam != "" {
		// Parse comma-separated schema names if provided
		schemaNames = append(schemaNames, schemasParam)
	}

	crossSchemaReq := &forma.CrossSchemaRequest{
		SchemaNames:  schemaNames,
		SearchTerm:   queryParams.Get("q"),
		Page:         page,
		ItemsPerPage: itemsPerPage,
	}

	result, err := s.manager.CrossSchemaSearch(r.Context(), crossSchemaReq)
	if err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("cross-schema search failed: %v", err))
		return
	}

	writeSuccess(w, http.StatusOK, result)
}

// handleAdvancedQuery handles POST /api/v1/advanced_query
func (s *Server) handleAdvancedQuery(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	var payload forma.QueryRequest
	if err := readJSONBody(r, &payload); err != nil {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("invalid json body: %v", err))
		return
	}

	if payload.SchemaName == "" {
		writeError(w, http.StatusBadRequest, "schema_name is required")
		return
	}

	if payload.Condition == nil {
		writeError(w, http.StatusBadRequest, "condition is required")
		return
	}

	result, err := s.manager.Query(r.Context(), &payload)
	if err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("advanced query failed: %v", err))
		return
	}

	writeSuccess(w, http.StatusOK, result)
}

// apiHandler is the main router that dispatches to specific handlers
func (s *Server) apiHandler(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Path

	log.Printf("handling path: %s", path)

	// For DELETE requests, check if path contains row_id
	if r.Method == http.MethodDelete {
		schemaName, rowIDStr, err := parsePath(path)
		if err != nil {
			writeError(w, http.StatusBadRequest, fmt.Sprintf("invalid path: %v", err))
			return
		}

		// If path contains row_id, use single delete
		if rowIDStr != "" {
			rowID, err := parseUUID(rowIDStr)
			if err != nil {
				writeError(w, http.StatusBadRequest, fmt.Sprintf("invalid row_id: %v", err))
				return
			}
			s.handleSingleDelete(w, r, schemaName, rowID)
			return
		}
	}

	// Route to specific handlers
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
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}
