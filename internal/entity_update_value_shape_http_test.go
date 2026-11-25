package internal

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/lychee-technology/forma/internal/httpapi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestHTTPUpdateRefusesAContainerAtASingleValueWith400 pins #696 at the HTTP
// boundary: under the default report-only validation, the update route
// answers a container at a single value's position with 400 and the
// published message naming the attribute, its type and what was sent, never
// a 500, and still writes an array the schema declares.
func TestHTTPUpdateRefusesAContainerAtASingleValueWith400(t *testing.T) {
	env := newValueShapeEnv(t, "report-only")
	rowID := env.create(t, valueShapeSeed())
	handler := httpapi.NewServer(env.em, httpapi.Options{}).Handler()
	put := func(body string) *httptest.ResponseRecorder {
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, httptest.NewRequest(http.MethodPut, "/api/v1/"+valueShapeSchema+"/"+rowID.String(), strings.NewReader(body)))
		return rec
	}
	for body, want := range map[string]string{
		`{"note": {}}`:                 "invalid value for attribute 'note' (attrID=2): declared type text takes a single value, got an empty object",
		`{"note": []}`:                 "invalid value for attribute 'note' (attrID=2): declared type text takes a single value, got an empty array",
		`{"note": ["a"]}`:              `invalid value for attribute 'note' (attrID=2): declared type text takes a single value, got a value at array indices \"0\"`,
		`{"items": [[{"name": "x"}]]}`: `invalid value for attribute 'items.name' (attrID=9): declared type text takes a single value, got a value at array indices \"0,0\"`,
	} {
		rec := put(body)
		assert.Equal(t, http.StatusBadRequest, rec.Code, "%s: %s", body, rec.Body.String())
		assert.Contains(t, rec.Body.String(), want, body)
		assert.NotContains(t, rec.Body.String(), "validating", "%s: the skeleton's verdict is operator detail", body)
	}
	rec := put(`{"phones": ["p2"]}`)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	assert.Contains(t, rec.Body.String(), `"phones":["p2"]`)
}
