package main

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// statusSequenceServer responds with the given statuses in order, then 200
// once they are exhausted, counting requests.
func statusSequenceServer(t *testing.T, statuses []int, retryAfter string) (*httptest.Server, *int32) {
	t.Helper()
	var hits int32
	var mu sync.Mutex
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&hits, 1)
		mu.Lock()
		status := http.StatusOK
		if len(statuses) > 0 {
			status = statuses[0]
			statuses = statuses[1:]
		}
		mu.Unlock()
		if retryAfter != "" && status != http.StatusOK {
			w.Header().Set("Retry-After", retryAfter)
		}
		if status != http.StatusOK {
			w.WriteHeader(status)
			return
		}
		fmt.Fprint(w, `<html><body><h1>ok</h1></body></html>`)
	}))
	t.Cleanup(server.Close)
	return server, &hits
}

func TestFetchRetryClassification(t *testing.T) {
	cases := []struct {
		name         string
		statuses     []int
		retryAfter   string
		maxRetries   int
		wantRequests int
		wantErr      bool
	}{
		{name: "429 then success", statuses: []int{429}, maxRetries: 3, wantRequests: 2},
		{name: "500 then success", statuses: []int{500}, maxRetries: 3, wantRequests: 2},
		{name: "502 twice then success", statuses: []int{502, 502}, maxRetries: 3, wantRequests: 3},
		{name: "404 not retried", statuses: []int{404}, maxRetries: 3, wantRequests: 1, wantErr: true},
		{name: "403 not retried", statuses: []int{403}, maxRetries: 3, wantRequests: 1, wantErr: true},
		{name: "500 exhausts retries", statuses: []int{500, 500, 500, 500}, maxRetries: 2, wantRequests: 3, wantErr: true},
		{name: "429 exhausts retries", statuses: []int{429, 429}, maxRetries: 1, wantRequests: 2, wantErr: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			server, hits := statusSequenceServer(t, tc.statuses, tc.retryAfter)
			f := newFetcher(batchOptions{maxRetries: tc.maxRetries, concurrency: 2})
			result, err := f.fetch(context.Background(), server.URL+"/")
			if tc.wantErr && err == nil {
				t.Fatal("expected error, got success")
			}
			if !tc.wantErr {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				if result.status != http.StatusOK {
					t.Errorf("status %d, want 200", result.status)
				}
			}
			if got := atomic.LoadInt32(hits); got != int32(tc.wantRequests) {
				t.Errorf("got %d requests, want %d", got, tc.wantRequests)
			}
		})
	}
}

func TestFetchHonorsRetryAfter(t *testing.T) {
	server, hits := statusSequenceServer(t, []int{429}, "1")
	f := newFetcher(batchOptions{maxRetries: 3, concurrency: 1})

	start := time.Now()
	if _, err := f.fetch(context.Background(), server.URL+"/"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	elapsed := time.Since(start)

	if elapsed < 900*time.Millisecond {
		t.Errorf("retry happened after %v, want at least ~1s per Retry-After", elapsed)
	}
	if got := atomic.LoadInt32(hits); got != 2 {
		t.Errorf("got %d requests, want 2", got)
	}
}

func TestParseRetryAfter(t *testing.T) {
	cases := []struct {
		name  string
		value string
		want  time.Duration
	}{
		{name: "empty", value: "", want: 0},
		{name: "seconds", value: "2", want: 2 * time.Second},
		{name: "zero", value: "0", want: 0},
		{name: "negative", value: "-5", want: 0},
		{name: "garbage", value: "soon", want: 0},
		{name: "http date in past", value: "Mon, 02 Jan 2006 15:04:05 GMT", want: 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := parseRetryAfter(tc.value); got != tc.want {
				t.Errorf("got %v, want %v", got, tc.want)
			}
		})
	}
}

// convergingServer serves /a/1 and /b/1 which both link next to /shared,
// which links to /end. It counts requests per path.
func convergingServer(t *testing.T) (*httptest.Server, func(string) int32) {
	t.Helper()
	counts := sync.Map{}
	var server *httptest.Server
	page := func(body string) string { return "<html><body>" + body + "</body></html>" }
	server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		count, _ := counts.LoadOrStore(r.URL.Path, new(int32))
		atomic.AddInt32(count.(*int32), 1)
		switch r.URL.Path {
		case "/a/1", "/b/1":
			fmt.Fprint(w, page(`<a rel="next" href="/shared">next</a>`))
		case "/shared":
			fmt.Fprint(w, page(`<a rel="next" href="/end">next</a>`))
		case "/end":
			fmt.Fprint(w, page(`<h1>end</h1>`))
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(server.Close)
	return server, func(path string) int32 {
		if count, ok := counts.Load(path); ok {
			return atomic.LoadInt32(count.(*int32))
		}
		return 0
	}
}

func TestConcurrentDedup(t *testing.T) {
	server, hits := convergingServer(t)
	seeds := writeSeedsFile(t, server.URL+"/a/1\n"+server.URL+"/b/1\n")
	dir := t.TempDir()

	summary, err := runBatch(context.Background(), batchOptions{
		seedsPath:   seeds,
		outPath:     filepath.Join(dir, "out.jsonl"),
		statePath:   filepath.Join(dir, "state.json"),
		maxPages:    5,
		concurrency: 4,
	})
	if err != nil {
		t.Fatalf("runBatch failed: %v", err)
	}
	if summary.Failed != 0 {
		t.Fatalf("unexpected failures: %+v", summary.Failures)
	}
	if got := hits("/shared"); got != 1 {
		t.Errorf("/shared fetched %d times, want 1", got)
	}
	if got := hits("/end"); got != 1 {
		t.Errorf("/end fetched %d times, want 1", got)
	}
	// 4 unique pages total (/a/1, /b/1, /shared, /end)
	if summary.Pages != 4 {
		t.Errorf("got %d pages, want 4", summary.Pages)
	}
	if summary.Deduped != 2 {
		t.Errorf("got %d deduped, want 2", summary.Deduped)
	}
}

func TestPerHostSerialization(t *testing.T) {
	var current, maxSeen int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		inflight := atomic.AddInt32(&current, 1)
		for {
			old := atomic.LoadInt32(&maxSeen)
			if inflight <= old || atomic.CompareAndSwapInt32(&maxSeen, old, inflight) {
				break
			}
		}
		time.Sleep(50 * time.Millisecond)
		atomic.AddInt32(&current, -1)
		fmt.Fprint(w, `<html><body><h1>ok</h1></body></html>`)
	}))
	defer server.Close()

	f := newFetcher(batchOptions{maxRetries: 0, concurrency: 8, perHostInterval: time.Millisecond})
	var wg sync.WaitGroup
	for i := 0; i < 6; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := f.fetch(context.Background(), server.URL+"/"); err != nil {
				t.Errorf("fetch failed: %v", err)
			}
		}()
	}
	wg.Wait()

	if got := atomic.LoadInt32(&maxSeen); got != 1 {
		t.Errorf("max concurrent requests to one host was %d, want 1", got)
	}
}

func TestPerHostInterval(t *testing.T) {
	var hits int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&hits, 1)
		fmt.Fprint(w, `<html><body><h1>ok</h1></body></html>`)
	}))
	defer server.Close()

	f := newFetcher(batchOptions{maxRetries: 0, concurrency: 1, perHostInterval: 100 * time.Millisecond})
	start := time.Now()
	for i := 0; i < 3; i++ {
		if _, err := f.fetch(context.Background(), server.URL+"/"); err != nil {
			t.Fatalf("fetch failed: %v", err)
		}
	}
	if elapsed := time.Since(start); elapsed < 200*time.Millisecond {
		t.Errorf("3 requests took %v, want at least 2 intervals of 100ms", elapsed)
	}
}
