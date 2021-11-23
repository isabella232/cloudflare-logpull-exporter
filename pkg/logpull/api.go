package logpull

import (
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// DefaultBaseURL is the default base URL for all API calls
const DefaultBaseURL = "https://api.cloudflare.com/client/v4"

// authType represents the various Cloudflare API authentication schemes
type authType int

const (
	// authKeyEmail specifies that we should authenticate with API key and email address
	authKeyEmail authType = iota
	// authUserServiceKey specifies that we should authenticate with a user service key
	authUserServiceKey
	// authToken specifies that we should authenticate with an API token
	authToken
)

// API is a Cloudflare Logpull API client
type API struct {
	// HTTPClient is the HTTP client used to perform API requests
	HTTPClient *http.Client
	// BaseURL is the base URL for all API calls
	BaseURL string

	authType       authType
	apiKey         string
	apiEmail       string
	apiToken       string
	apiUserService string
}

// New creates a new Logpull API client from the given API key and email
// address.
func New(key, email string) *API {
	return &API{
		HTTPClient: http.DefaultClient,
		BaseURL:    DefaultBaseURL,
		authType:   authKeyEmail,
		apiKey:     key,
		apiEmail:   email,
	}
}

// NewWithToken creates a new Logpull API client from the given API token.
func NewWithToken(token string) *API {
	return &API{
		HTTPClient: http.DefaultClient,
		BaseURL:    DefaultBaseURL,
		authType:   authToken,
		apiToken:   token,
	}
}

// NewWithUserServiceKey creates a new Logpull API client from the given
// user service key.
func NewWithUserServiceKey(key string) *API {
	return &API{
		HTTPClient:     http.DefaultClient,
		BaseURL:        DefaultBaseURL,
		authType:       authUserServiceKey,
		apiUserService: key,
	}
}

// ZoneLogs fetches logs from Cloudflare's Logpull endpoint. The returned
// io.ReadCloser contains NDJSON-encoded log data, and it is the caller's
// responsibility to close it when finished.
func (api *API) ZoneLogs(zoneID string, fields []string, count int, start, end time.Time) (io.ReadCloser, error) {
	url := api.BaseURL + "/zones/" + zoneID + "/logs/received"
	url += "?start=" + start.Format(time.RFC3339)
	url += "&end=" + end.Format(time.RFC3339)
	if fields != nil {
		url += "&fields=" + strings.Join(fields, ",")
	}
	if count != 0 {
		url += "&count=" + strconv.Itoa(count)
	}

	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("creating api request: %w", err)
	}

	req.Header.Add("Accept", "application/json")

	if api.authType == authToken {
		req.Header.Add("Authorization", "Bearer "+api.apiToken)
	}

	if api.authType == authKeyEmail {
		req.Header.Add("X-Auth-Key", api.apiKey)
		req.Header.Add("X-Auth-Email", api.apiEmail)
	}

	if api.authType == authUserServiceKey {
		req.Header.Add("X-Auth-User-Service-Key", api.apiUserService)
	}

	resp, err := api.HTTPClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("performing api request: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		respBody, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return nil, fmt.Errorf("reading api response body: %w", err)
		} else {
			err = &HTTPError{resp.StatusCode, respBody}
			return nil, fmt.Errorf("unexpected api response: %w", err)
		}
	}

	return resp.Body, nil
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
