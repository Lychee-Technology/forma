package httpapi

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

// TestMetricsRouteMountedOnlyWhenHandlerSupplied pins the #423 gate at the
// router: with no handler the path is not served, with one it is served at the
// configured path (default /metrics).
func TestMetricsRouteMountedOnlyWhenHandlerSupplied(t *testing.T) {
	probe := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("# scrape\n"))
	})

	cases := []struct {
		name   string
		opts   Options
		path   string
		status int
	}{
		{name: "absent by default", opts: Options{}, path: "/metrics", status: http.StatusNotFound},
		{name: "default path", opts: Options{MetricsHandler: probe}, path: "/metrics", status: http.StatusOK},
		{name: "custom path", opts: Options{MetricsHandler: probe, MetricsPath: "/internal/metrics"}, path: "/internal/metrics", status: http.StatusOK},
		{name: "custom path leaves default unserved", opts: Options{MetricsHandler: probe, MetricsPath: "/internal/metrics"}, path: "/metrics", status: http.StatusNotFound},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := NewServer(&mockEntityManager{}, tc.opts)
			rec := httptest.NewRecorder()
			srv.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, tc.path, nil))
			if rec.Code != tc.status {
				t.Fatalf("GET %s: status %d, want %d (body %q)", tc.path, rec.Code, tc.status, rec.Body.String())
			}
		})
	}
}
