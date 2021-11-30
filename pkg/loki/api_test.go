package loki

import (
	"compress/gzip"
	"encoding/json"
	"errors"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
)

// NewStreamSlice returns a dummy Loki stream slice which may be passed to the
// client's Push method in tests.
func NewStreamSlice() []Stream {
	return []Stream{
		{
			Labels: map[string]string{"foo": "bar"},
			Values: []Value{
				{
					Time: time.Now().Truncate(time.Second),
					Line: "Hello, World!",
				},
			},
		},
	}
}

// RequestBody is the target of JSON unmarshaling of the HTTP request body
// submitted to a Loki push endpoint. It is used to validate that the client is
// correctly encoding the HTTP request body.
type RequestBody struct {
	Streams []RequestStream `json:"streams"`
}

// RequestStream is a component part of a RequestBody. It is used to validate
// that the client is correctly encoding the HTTP request body.
type RequestStream struct {
	Labels map[string]string `json:"stream"`
	Values [][2]string       `json:"values"`
}

// StreamSliceToRequestBody converts a []Stream into a RequestBody.
func StreamSliceToRequestBody(streams []Stream) *RequestBody {
	rb := &RequestBody{}
	rb.Streams = make([]RequestStream, 0, len(streams))
	for _, stream := range streams {
		s := RequestStream{
			Labels: stream.Labels,
			Values: make([][2]string, 0, len(stream.Values)),
		}
		for _, v := range stream.Values {
			s.Values = append(s.Values, [2]string{
				strconv.FormatInt(v.Time.UnixNano(), 10),
				v.Line,
			})
		}
		rb.Streams = append(rb.Streams, s)
	}
	return rb
}

// TestValidHTTPRequest validates that the correct HTTP headers and URL
// parameters are set on the request made by ZoneLogs
func TestValidHTTPRequest(t *testing.T) {
	streams := NewStreamSlice()

	server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()

		if r.URL.Path != "/loki/api/v1/push" {
			t.Errorf("wrong path requested: %s", r.URL.Path)
		}
		if r.Header.Get("Content-Type") != "application/json" {
			t.Errorf("Content-Type header must be 'application/json'")
		}

		if r.Header.Get("Content-Encoding") != "gzip" {
			t.Errorf("Content-Encoding header must be 'gzip'")
		}

		gzipR, err := gzip.NewReader(r.Body)
		if gzipR != nil {
			defer gzipR.Close()
		}
		if err != nil {
			t.Fatalf("gzip reader: %s", err)
		}

		body, err := ioutil.ReadAll(gzipR)
		if err != nil {
			t.Fatalf("reading request body: %s", err)
		}

		var actual *RequestBody
		if err := json.Unmarshal(body, &actual); err != nil {
			t.Fatalf("json decoding: %s", err)
		}

		expected := StreamSliceToRequestBody(streams)
		if diff := cmp.Diff(expected, actual); diff != "" {
			t.Errorf("unexpected request body (-expected, +actual): \n%s", diff)
		}
	}))
	defer server.Close()

	api := New(server.URL)
	api.HTTPClient = server.Client()

	err := api.Push(streams)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
}

// TestReturnsErrorOnHTTPError validates that Push will return an error with
// the HTTP status and response body when any unexpected response is received.
func TestReturnsErrorOnHTTPError(t *testing.T) {
	expected := &HTTPError{
		StatusCode: http.StatusInternalServerError,
		Body:       []byte("the server's on fire"),
	}

	server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		rw.WriteHeader(expected.StatusCode)
		_, err := rw.Write(expected.Body)
		if err != nil {
			t.Fatalf("writing response body: %s", err)
		}
	}))
	defer server.Close()

	api := New(server.URL)
	api.HTTPClient = server.Client()

	streams := NewStreamSlice()
	err := api.Push(streams)
	if err == nil {
		t.Fatalf("expected to receive an error value")
	}

	var actual *HTTPError
	if ok := errors.As(err, &actual); !ok {
		t.Fatalf("expected error value to be a *HTTPError")
	}

	if diff := cmp.Diff(expected, actual); diff != "" {
		t.Errorf("unexpected error returned (-expected, +actual):\n%s", diff)
	}
}

// TestAgainstLiveEndpoint will attempt to push a stream slice into an actual
// Loki instance. It fails if Push returns an error.
//
// This test is skipped unless the LOKI_TEST_LIVE_ENDPOINT environment variable
// is non-empty.
func TestAgainstLiveEndpoint(t *testing.T) {
	baseURL := os.Getenv("LOKI_TEST_LIVE_ENDPOINT")
	if baseURL == "" {
		t.Skip("skipping test of live API endpoint")
	}

	api := New(baseURL)

	streams := NewStreamSlice()
	err := api.Push(streams)
	if err != nil {
		t.Errorf("unexpected error: %s", err)
	}
}
