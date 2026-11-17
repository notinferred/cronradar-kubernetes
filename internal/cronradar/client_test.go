package cronradar

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestSyncSendsBasicAuthAndRegistry(t *testing.T) {
	var got SyncRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		user, password, ok := r.BasicAuth()
		if !ok || user != "ck_app_test_secret" || password != "" {
			t.Errorf("basic auth = %q %q %v", user, password, ok)
		}
		if r.Method != http.MethodPost || r.URL.Path != "/api/v1/sync" {
			t.Errorf("request = %s %s", r.Method, r.URL.Path)
		}
		if r.Header.Get("Content-Type") != "application/json" {
			t.Errorf("content type = %q", r.Header.Get("Content-Type"))
		}
		if !strings.HasPrefix(r.Header.Get("User-Agent"), "cronradar-kubernetes/") {
			t.Errorf("user agent = %q", r.Header.Get("User-Agent"))
		}
		body, _ := io.ReadAll(r.Body)
		if err := json.Unmarshal(body, &got); err != nil {
			t.Errorf("body: %v", err)
		}
		_, _ = w.Write([]byte(`{"errors":[{"key":"bad","error":"Invalid schedule"}],"summary":{"total":1,"created":1,"errors":1}}`))
	}))
	defer server.Close()

	client := NewClient(server.URL+"/", "ck_app_test_secret", "0.1.0")
	response, err := client.Sync(context.Background(), SyncRequest{
		Source: "kubernetes", Version: "0.1.0", Mode: "reconcile",
		Monitors: []Monitor{{Key: "prod-backup", Name: "prod/backup", Schedule: "0 2 * * *", GracePeriod: 60}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if got.Mode != "reconcile" || got.Source != "kubernetes" || len(got.Monitors) != 1 || got.Monitors[0].Key != "prod-backup" {
		t.Fatalf("request = %+v", got)
	}
	if response.Summary.Created != 1 || len(response.Errors) != 1 || response.Errors[0].Key != "bad" {
		t.Fatalf("response = %+v", response)
	}
}

func TestReportBuildsLifecycleRequest(t *testing.T) {
	var request *http.Request
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		request = r
		_, _ = w.Write([]byte(`{"status":"failure_recorded"}`))
	}))
	defer server.Close()

	err := NewClient(server.URL, "ck_app_test_secret", "0.1.0").Report(context.Background(), Event{
		Kind: "fail", MonitorKey: "prod backup", RunID: "uid-1", Schedule: "0 2 * * *", GracePeriod: 300,
		Message: strings.Repeat("m", 1500),
	})
	if err != nil {
		t.Fatal(err)
	}
	if request.Method != http.MethodPost || request.URL.EscapedPath() != "/api/v1/ping/prod%20backup/fail" {
		t.Fatalf("request = %s %s", request.Method, request.URL.EscapedPath())
	}
	if strings.Contains(request.URL.String(), "ck_app_") {
		t.Fatal("the API key must not appear in the URL")
	}
	query := request.URL.Query()
	if query.Get("schedule") != "0 2 * * *" || query.Get("gracePeriod") != "300" || query.Get("run_id") != "uid-1" {
		t.Fatalf("query = %v", query)
	}
	if len(query.Get("message")) != 1000 {
		t.Fatalf("message length = %d, want 1000", len(query.Get("message")))
	}
	if request.Header.Get("Idempotency-Key") != "uid-1:fail" {
		t.Fatalf("idempotency key = %q", request.Header.Get("Idempotency-Key"))
	}
}

func TestErrorsAreClassifiedForRetry(t *testing.T) {
	status := http.StatusTooManyRequests
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Retry-After", "30")
		w.WriteHeader(status)
	}))
	defer server.Close()
	client := NewClient(server.URL, "ck_app_test_secret", "0.1.0")

	err := client.Report(context.Background(), Event{Kind: "start", MonitorKey: "k"})
	var httpErr *HTTPError
	if !errors.As(err, &httpErr) || httpErr.RetryAfter != 30*time.Second || !Retryable(err) {
		t.Fatalf("429 should be retryable with Retry-After, got %v", err)
	}

	status = http.StatusUnauthorized
	if err := client.Report(context.Background(), Event{Kind: "start", MonitorKey: "k"}); Retryable(err) || err == nil {
		t.Fatalf("401 should not be retried, got %v", err)
	}
}

func TestRequestsTimeOutAfterFiveSeconds(t *testing.T) {
	if NewClient("", "k", "0").http.Timeout != 5*time.Second {
		t.Fatal("every request must be bounded by the 5-second SDK timeout")
	}
	if NewClient("", "k", "0").baseURL != DefaultBaseURL {
		t.Fatal("an empty base URL must use the production endpoint")
	}
}
