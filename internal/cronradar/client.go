// Package cronradar is the HTTP client for the CronRadar ingestion API.
package cronradar

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const (
	DefaultBaseURL = "https://cron.life"
	// RequestTimeout bounds every request, as every CronRadar SDK does.
	RequestTimeout = 5 * time.Second
	// MaxSyncMonitors is the most monitors one sync request may carry.
	MaxSyncMonitors = 1000
	maxFailMessage  = 1000
	maxErrorBody    = 2048
)

// Monitor is one job declaration in a registry sync.
type Monitor struct {
	Key         string            `json:"key"`
	Name        string            `json:"name,omitempty"`
	Schedule    string            `json:"schedule"`
	GracePeriod int               `json:"gracePeriod"`
	Metadata    map[string]string `json:"metadata,omitempty"`
}

type SyncRequest struct {
	Source   string    `json:"source"`
	Version  string    `json:"version,omitempty"`
	Mode     string    `json:"mode"`
	Monitors []Monitor `json:"monitors"`
}

type SyncError struct {
	Key   string `json:"key"`
	Error string `json:"error"`
}

type SyncResponse struct {
	Errors  []SyncError `json:"errors"`
	Summary struct {
		Total     int `json:"total"`
		Created   int `json:"created"`
		Updated   int `json:"updated"`
		Unchanged int `json:"unchanged"`
		Removed   int `json:"removed"`
		Errors    int `json:"errors"`
	} `json:"summary"`
}

// Event is one lifecycle report for a monitor. It is comparable so it can be queued.
type Event struct {
	Kind        string // "start", "complete" or "fail"
	MonitorKey  string
	RunID       string
	Schedule    string
	GracePeriod int
	Message     string
}

// HTTPError is a non-2xx response.
type HTTPError struct {
	Status     int
	RetryAfter time.Duration
	Body       string
}

func (e *HTTPError) Error() string {
	return fmt.Sprintf("HTTP %d: %s", e.Status, e.Body)
}

// Retryable reports whether the request may succeed if sent again unchanged:
// network failures, rate limits and server errors. Other 4xx responses need a fix first.
func Retryable(err error) bool {
	var httpErr *HTTPError
	if errors.As(err, &httpErr) {
		return httpErr.Status == http.StatusTooManyRequests || httpErr.Status >= 500
	}
	return err != nil
}

type Client struct {
	baseURL   string
	apiKey    string
	userAgent string
	http      *http.Client
}

func NewClient(baseURL, apiKey, version string) *Client {
	if strings.TrimSpace(baseURL) == "" {
		baseURL = DefaultBaseURL
	}
	return &Client{
		baseURL:   strings.TrimRight(strings.TrimSpace(baseURL), "/"),
		apiKey:    apiKey,
		userAgent: "cronradar-kubernetes/" + version,
		http:      &http.Client{Timeout: RequestTimeout},
	}
}

func (c *Client) Sync(ctx context.Context, request SyncRequest) (*SyncResponse, error) {
	body, err := json.Marshal(request)
	if err != nil {
		return nil, err
	}
	responseBody, err := c.do(ctx, c.baseURL+"/api/v1/sync", bytes.NewReader(body), nil)
	if err != nil {
		return nil, err
	}
	var response SyncResponse
	if err := json.Unmarshal(responseBody, &response); err != nil {
		return nil, fmt.Errorf("decode sync response: %w", err)
	}
	return &response, nil
}

func (c *Client) Report(ctx context.Context, event Event) error {
	query := url.Values{}
	if event.Schedule != "" {
		// Lets a deleted monitor recreate itself from its next run.
		query.Set("schedule", event.Schedule)
		query.Set("gracePeriod", strconv.Itoa(event.GracePeriod))
	}
	if event.RunID != "" {
		query.Set("run_id", event.RunID)
	}
	if event.Kind == "fail" && event.Message != "" {
		query.Set("message", truncate(event.Message, maxFailMessage))
	}
	endpoint := fmt.Sprintf("%s/api/v1/ping/%s/%s?%s",
		c.baseURL, url.PathEscape(event.MonitorKey), event.Kind, query.Encode())
	headers := map[string]string{}
	if event.RunID != "" {
		headers["Idempotency-Key"] = event.RunID + ":" + event.Kind
	}
	_, err := c.do(ctx, endpoint, nil, headers)
	return err
}

func (c *Client) do(ctx context.Context, endpoint string, body io.Reader, headers map[string]string) ([]byte, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, body)
	if err != nil {
		return nil, err
	}
	request.SetBasicAuth(c.apiKey, "")
	request.Header.Set("User-Agent", c.userAgent)
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	for name, value := range headers {
		request.Header.Set(name, value)
	}
	response, err := c.http.Do(request)
	if err != nil {
		return nil, redact(err)
	}
	defer response.Body.Close()
	responseBody, err := io.ReadAll(io.LimitReader(response.Body, 1<<20))
	if err != nil {
		return nil, err
	}
	if response.StatusCode < 200 || response.StatusCode > 299 {
		return nil, &HTTPError{
			Status:     response.StatusCode,
			RetryAfter: parseRetryAfter(response.Header.Get("Retry-After")),
			Body:       truncate(strings.TrimSpace(string(responseBody)), maxErrorBody),
		}
	}
	return responseBody, nil
}

// redact drops the request URL from transport errors; it can carry the failure message.
func redact(err error) error {
	var urlErr *url.Error
	if errors.As(err, &urlErr) {
		return fmt.Errorf("%s request failed: %w", urlErr.Op, urlErr.Err)
	}
	return err
}

func parseRetryAfter(value string) time.Duration {
	seconds, err := strconv.Atoi(strings.TrimSpace(value))
	if err != nil || seconds < 0 {
		return 0
	}
	return time.Duration(seconds) * time.Second
}

func truncate(value string, limit int) string {
	runes := []rune(value)
	if len(runes) <= limit {
		return value
	}
	return string(runes[:limit])
}
