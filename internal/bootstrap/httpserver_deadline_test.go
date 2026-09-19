package bootstrap

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"net/http"
	"testing"
	"time"
)

// TestWriteDeadlineStartsBeforeTheBodyIsRead pins the net/http behaviour
// HTTPServerConfig.Validate is built on (#465 review): the write deadline is
// armed when the headers have been read, so a slow body followed by a
// handler that spends its budget exhausts a WriteTimeout equal to their sum,
// and the response never leaves. The same request against a WriteTimeout
// that satisfies Validate's rule is answered. httptest recorders enforce no
// socket deadlines, which is why this test speaks TCP.
func TestWriteDeadlineStartsBeforeTheBodyIsRead(t *testing.T) {
	const bodyDelay, handlerWork = 400 * time.Millisecond, 400 * time.Millisecond

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, err := io.ReadAll(r.Body); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		time.Sleep(handlerWork)
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "done")
	})

	cases := []struct {
		name         string
		writeTimeout time.Duration
		wantResponse bool
	}{
		{"write timeout spent on the body read and the budget", bodyDelay + handlerWork, false},
		{"write timeout with room for the response", 5 * time.Second, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := HTTPServerConfig{ReadTimeout: 2 * time.Second, WriteTimeout: tc.writeTimeout}
			ln, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatalf("listen: %v", err)
			}
			srv := NewHTTPServer(ln.Addr().String(), handler, cfg)
			go func() { _ = srv.Serve(ln) }()
			defer srv.Close()

			conn, err := net.Dial("tcp", ln.Addr().String())
			if err != nil {
				t.Fatalf("dial: %v", err)
			}
			defer conn.Close()
			body := `{"slow":true}`
			head := fmt.Sprintf("POST / HTTP/1.1\r\nHost: forma\r\nContent-Length: %d\r\n\r\n", len(body))
			if _, err := io.WriteString(conn, head); err != nil {
				t.Fatalf("write headers: %v", err)
			}
			time.Sleep(bodyDelay)
			if _, err := io.WriteString(conn, body); err != nil {
				t.Fatalf("write body: %v", err)
			}
			_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
			resp, err := http.ReadResponse(bufio.NewReader(conn), nil)
			if !tc.wantResponse {
				if err == nil {
					resp.Body.Close()
					t.Fatalf("expected the write deadline to swallow the response, got %d", resp.StatusCode)
				}
				return
			}
			if err != nil {
				t.Fatalf("expected a response, got %v", err)
			}
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("expected 200, got %d", resp.StatusCode)
			}
		})
	}
}
