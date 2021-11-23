package logpull

import (
	"errors"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"os"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
)

// RequestParams are a set of request parameters which may be used to create
// API clients and exercise the client's ZoneLogs method in tests.
type RequestParams struct {
	APIToken       string
	APIKey         string
	UserEmail      string
	UserServiceKey string

	ZoneID string
	Fields []string
	Count  int
	Start  time.Time
	End    time.Time
}

// NewRequestParams returns a dummy set of request parameters which may be used
// to create API clients and exercise the client's ZoneLogs method in tests.
func NewRequestParams() *RequestParams {
	now := time.Now().Truncate(time.Second)

	return &RequestParams{
		APIToken:       "APIToken",
		APIKey:         "APIKey",
		UserEmail:      "UserEmail",
		UserServiceKey: "UserServiceKey",

		ZoneID: "ZoneID",
		Fields: []string{
			"ClientRequestHost",
			"EdgeResponseStatus",
			"OriginResponseStatus",
		},
		Count: 100,
		Start: now.Add(-2 * time.Minute),
		End:   now.Add(-1 * time.Minute),
	}
}

// TestValidHTTPRequest validates that the correct HTTP headers and URL
// parameters are set on the request made by ZoneLogs
func TestValidHTTPRequest(t *testing.T) {
	params := NewRequestParams()

	authCases := []struct {
		condition string
		authType  authType
		api       *API
	}{
		{"with API key and email", authKeyEmail, New(params.APIKey, params.UserEmail)},
		{"with API token", authToken, NewWithToken(params.APIToken)},
		{"with user service key", authUserServiceKey, NewWithUserServiceKey(params.UserServiceKey)},
	}

	for _, c := range authCases {
		t.Run(c.condition, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
				switch c.authType {
				case authKeyEmail:
					if r.Header.Get("X-Auth-Key") != params.APIKey {
						t.Error("X-Auth-Key header was not set correctly")
					}
					if r.Header.Get("X-Auth-Email") != params.UserEmail {
						t.Error("X-Auth-Email header was not set correctly")
					}
				case authToken:
					if r.Header.Get("Authorization") != "Bearer "+params.APIToken {
						t.Error("Authorization header was not set correctly")
					}
				case authUserServiceKey:
					if r.Header.Get("X-Auth-User-Service-Key") != params.UserServiceKey {
						t.Error("X-Auth-User-Service-Key header was not set correctly")
					}
				default:
					t.Fatalf("invalid authType provided: %v", c.authType)
				}
			}))
			defer server.Close()

			c.api.HTTPClient = server.Client()
			c.api.BaseURL = server.URL

			_, err := c.api.ZoneLogs(params.ZoneID, params.Fields, params.Count, params.Start, params.End)
			if err != nil {
				t.Fatalf("unexpected error: %s", err)
			}
		})
	}

	server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		pathRegexp := regexp.MustCompile("/zones/(.+)/logs/received")
		if !pathRegexp.MatchString(r.URL.Path) {
			t.Errorf("wrong path requested: %s", r.URL.Path)
		}

		zoneID := pathRegexp.FindStringSubmatch(r.URL.Path)[1]
		if zoneID != params.ZoneID {
			t.Errorf("zone request parameter not set correctly")
		}

		fields := r.URL.Query().Get("fields")
		if fields != strings.Join(params.Fields, ",") {
			t.Errorf("fields request parameter not set correctly")
		}

		count, err := strconv.Atoi(r.URL.Query().Get("count"))
		if err != nil {
			t.Errorf("count request parameter could not be parsed as a number")
		}
		if count != params.Count {
			t.Errorf("count request parameter not set correctly")
		}

		start, err := time.Parse(time.RFC3339, r.URL.Query().Get("start"))
		if err != nil {
			t.Errorf("start request parameter could not be parsed")
		}
		if !start.Equal(params.Start) {
			t.Errorf("start request parameter not set correctly")
		}

		end, err := time.Parse(time.RFC3339, r.URL.Query().Get("end"))
		if err != nil {
			t.Errorf("end request parameter could not be parsed")
		}
		if !end.Equal(params.End) {
			t.Errorf("end request parameter not set correctly")
		}
	}))
	defer server.Close()

	api := New(params.APIKey, params.UserEmail)
	api.HTTPClient = server.Client()
	api.BaseURL = server.URL

	_, err := api.ZoneLogs(params.ZoneID, params.Fields, params.Count, params.Start, params.End)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
}

// TestReturnsResponseBodyOnSuccess validates that the io.ReadCloser returned
// by ZoneLogs emits the HTTP response body when read, which should contain
// NDJSON log data in actual usage.
func TestReturnsResponseBodyOnSuccess(t *testing.T) {
	expected := []byte("ok")

	server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		_, err := rw.Write(expected)
		if err != nil {
			t.Fatalf("writing response body: %s", err)
		}
	}))
	defer server.Close()

	api := New("", "")
	api.HTTPClient = server.Client()
	api.BaseURL = server.URL

	responseBody, err := api.ZoneLogs("", nil, 0, time.Time{}, time.Time{})
	if responseBody != nil {
		defer responseBody.Close()
	}
	if err != nil {
		t.Fatalf("getting zone logs: %s", err)
	}

	actual, err := ioutil.ReadAll(responseBody)
	if err != nil {
		t.Fatalf("reading responseBody: %s", err)
	}

	if diff := cmp.Diff(expected, actual); diff != "" {
		t.Errorf("unexpected data returned (-expected, +actual):\n%s", diff)
	}
}

// TestReturnsErrorOnHTTPError validates that ZoneLogs will return an error
// with the HTTP status and response body when any unexpected response is
// received.
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

	api := New("", "")
	api.HTTPClient = server.Client()
	api.BaseURL = server.URL

	responseBody, err := api.ZoneLogs("", nil, 0, time.Time{}, time.Time{})
	if responseBody != nil {
		defer responseBody.Close()
	}
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

// TestAgainstLiveEndpoint will attempt to pull recent logs from an actual
// Cloudflare zone with log retention enabled. It fails if ZoneLogs returns an
// error.
//
// This test is skipped unless the LOGPULL_TEST_LIVE_ENDPOINT environment
// variable is non-empty, and requires LOGPULL_TEST_API_TOKEN and
// LOGPULL_TEST_ZONE_ID to be set appropriately.
func TestAgainstLiveEndpoint(t *testing.T) {
	if os.Getenv("LOGPULL_TEST_LIVE_ENDPOINT") == "" {
		t.Skip("skipping test of live API endpoint")
	}

	params := NewRequestParams()

	params.APIToken = os.Getenv("LOGPULL_TEST_API_TOKEN")
	if params.APIToken == "" {
		t.Fatal("LOGPULL_TEST_API_TOKEN must be specified")
	}

	params.ZoneID = os.Getenv("LOGPULL_TEST_ZONE_ID")
	if params.ZoneID == "" {
		t.Fatal("LOGPULL_TEST_ZONE_ID must be specified")
	}

	api := NewWithToken(params.APIToken)
	_, err := api.ZoneLogs(params.ZoneID, params.Fields, params.Count, params.Start, params.End)
	if err != nil {
		t.Errorf("unexpected error: %s", err)
	}
}
