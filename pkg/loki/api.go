package loki

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"
	"time"
)

// API is a Loki API client
type API struct {
	HTTPClient *http.Client
	BaseURL    string
}

// New creates a new Loki API client from the given base URL
func New(baseURL string) *API {
	return &API{
		HTTPClient: http.DefaultClient,
		BaseURL:    baseURL,
	}
}

// Push a slice of streams to the Loki endpoint.
func (api *API) Push(streams []Stream) error {
	data := map[string]interface{}{"streams": streams}

	var buf bytes.Buffer
	gzipW := gzip.NewWriter(&buf)
	jsonW := json.NewEncoder(gzipW)

	if err := jsonW.Encode(data); err != nil {
		return fmt.Errorf("json encoder: %w", err)
	}

	// GZIP writer must be closed to flush the buffer and write the footer
	if err := gzipW.Close(); err != nil {
		return fmt.Errorf("gzip writer: %w", err)
	}

	url := api.BaseURL + "/loki/api/v1/push"

	req, err := http.NewRequest(http.MethodPost, url, &buf)
	if err != nil {
		return fmt.Errorf("creating api request: %w", err)
	}

	req.Header.Add("Content-Encoding", "gzip")
	req.Header.Add("Content-Type", "application/json")
	req.Header.Add("Accept", "application/json")

	resp, err := api.HTTPClient.Do(req)
	if err != nil {
		return fmt.Errorf("performing api request: %w", err)
	}

	defer resp.Body.Close()

	if resp.StatusCode/100 != 2 {
		respBody, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			err = fmt.Errorf("reading api response body: %w", err)
		} else {
			err = &HTTPError{resp.StatusCode, respBody}
			err = fmt.Errorf("unexpected api response: %w", err)
		}
		return err
	}

	return nil
}

// Stream is a labeled log stream which may be pushed to a Loki endpoint.
type Stream struct {
	Labels map[string]string `json:"stream"`
	Values []Value           `json:"values"`
}

// Value is an individual timestamped log line which may be pushed as part of a
// Stream to a Loki endpoint.
type Value struct {
	Time time.Time
	Line string
}

// MarshalJSON is an implementation of the json Marshaler interface. It is used
// to format a stream value in the format expected by the Loki endpoint.
func (v *Value) MarshalJSON() ([]byte, error) {
	return json.Marshal([2]string{
		strconv.FormatInt(v.Time.UTC().UnixNano(), 10),
		v.Line,
	})
}

// HTTPError is a concrete error type which captures the HTTP status code and
// response body from API calls.
type HTTPError struct {
	StatusCode int
	Body       []byte
}

// Error implements the error interface for *HTTPError
func (err *HTTPError) Error() string {
	return fmt.Sprintf("HTTP %d: %s", err.StatusCode, string(err.Body))
}
