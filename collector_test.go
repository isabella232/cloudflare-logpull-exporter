package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/bitgo/cloudflare-logpull-exporter/pkg/logpull"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

// TestCollectorHTTPResponses checks that the collector emits correct
// `cloudflare_logs_http_responses` metrics
func TestCollectorHTTPResponses(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		jsonBody := []byte(`{"ClientRequestHost": "example.org", "EdgeResponseStatus": 200, "OriginResponseStatus": 200}`)
		if _, err := w.Write(jsonBody); err != nil {
			t.Fatalf("unexpected error: %s", err)
		}
	}))
	defer ts.Close()

	api := logpull.New("", "")
	api.HTTPClient = ts.Client()
	api.BaseURL = ts.URL

	c, err := newCollector(api, []string{""}, time.Minute, func(err error) {
		t.Fatalf("unexpected error: %s", err)
	})
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	expected := strings.NewReader(`
		# HELP cloudflare_logs_http_responses Cloudflare HTTP responses, obtained via Logpull API
		# TYPE cloudflare_logs_http_responses gauge
		cloudflare_logs_http_responses{client_request_host="example.org",edge_response_status="200",origin_response_status="200",period="1m"} 1
	`)

	if err := testutil.CollectAndCompare(c, expected, "cloudflare_logs_http_responses"); err != nil {
		t.Error(err)
	}
}

// TestCollectorErrors checks that the collector emits the
// `cloudflare_logs_errors_total` metric when errors are returned from the
// Logpull API
func TestCollectorErrors(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, err := w.Write([]byte("the server's on fire"))
		if err != nil {
			t.Fatalf("writing response body: %s", err)
		}
	}))
	defer ts.Close()

	api := logpull.New("", "")
	api.HTTPClient = ts.Client()
	api.BaseURL = ts.URL

	c, err := newCollector(api, []string{""}, time.Minute, func(error) {})
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	expected := strings.NewReader(`
		# HELP cloudflare_logs_errors_total The number of errors that have occurred while collecting metrics
		# TYPE cloudflare_logs_errors_total counter
		cloudflare_logs_errors_total 1
	`)

	if err := testutil.CollectAndCompare(c, expected, "cloudflare_logs_errors_total"); err != nil {
		t.Error(err)
	}
}
