package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func batchTestOptions(seedsPath, dir string) batchOptions {
	return batchOptions{
		seedsPath:   seedsPath,
		outPath:     filepath.Join(dir, "out.jsonl"),
		statePath:   filepath.Join(dir, "state.json"),
		maxPages:    5,
		concurrency: 4,
	}
}

func readRecords(t *testing.T, outPath string) []pageRecord {
	t.Helper()
	data, err := os.ReadFile(outPath)
	if err != nil {
		t.Fatalf("failed to read output: %v", err)
	}
	var records []pageRecord
	for _, line := range strings.Split(strings.TrimSpace(string(data)), "\n") {
		if line == "" {
			continue
		}
		var record pageRecord
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			t.Fatalf("invalid JSONL line %q: %v", line, err)
		}
		records = append(records, record)
	}
	return records
}

func TestBatchOutputOrdering(t *testing.T) {
	// seed1 is deliberately slow so seed2 finishes first; output must still
	// be ordered by seed list order, then page number.
	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/one/1":
			time.Sleep(150 * time.Millisecond)
			fmt.Fprint(w, `<html><body><a rel="next" href="/one/2">next</a></body></html>`)
		case "/one/2":
			fmt.Fprint(w, `<html><body><h1>one 2</h1></body></html>`)
		case "/two/1":
			fmt.Fprint(w, `<html><body><a rel="next" href="/two/2">next</a></body></html>`)
		case "/two/2":
			fmt.Fprint(w, `<html><body><h1>two 2</h1></body></html>`)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	seed1 := server.URL + "/one/1"
	seed2 := server.URL + "/two/1"
	seeds := writeSeedsFile(t, seed1+"\n"+seed2+"\n")
	dir := t.TempDir()
	opts := batchTestOptions(seeds, dir)
	summary, err := runBatch(context.Background(), opts)
	if err != nil {
		t.Fatalf("runBatch failed: %v", err)
	}
	if summary.Succeeded != 2 || summary.Failed != 0 {
		t.Fatalf("unexpected summary: %+v", summary)
	}
	records := readRecords(t, opts.outPath)

	want := []struct {
		url    string
		seed   string
		page   int
		status int
	}{
		{server.URL + "/one/1", seed1, 1, 200},
		{server.URL + "/one/2", seed1, 2, 200},
		{server.URL + "/two/1", seed2, 1, 200},
		{server.URL + "/two/2", seed2, 2, 200},
	}
	if len(records) != len(want) {
		t.Fatalf("got %d records, want %d", len(records), len(want))
	}
	for i, record := range records {
		if record.URL != want[i].url || record.Seed != want[i].seed ||
			record.Page != want[i].page || record.Status != want[i].status {
			t.Errorf("record %d = {%s %s %d %d}, want {%s %s %d %d}",
				i, record.URL, record.Seed, record.Page, record.Status,
				want[i].url, want[i].seed, want[i].page, want[i].status)
		}
		var nodes []map[string]any
		if err := json.Unmarshal(record.Nodes, &nodes); err != nil {
			t.Errorf("record %d nodes invalid: %v", i, err)
		}
	}
}

// gatedServer serves, on the given loopback address, a 2-page chain where
// /page/2 blocks until release is closed (or the client goes away), letting
// a test interrupt the run at a deterministic point. A separate loopback
// address keeps its host limiter independent from other test servers.
func gatedServer(t *testing.T, addr string) (*httptest.Server, chan<- struct{}) {
	t.Helper()
	release := make(chan struct{})
	listener, err := net.Listen("tcp", addr+":0")
	if err != nil {
		t.Fatalf("failed to listen on %s: %v", addr, err)
	}
	server := &httptest.Server{
		Listener: listener,
		Config: &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			switch r.URL.Path {
			case "/page/1":
				fmt.Fprint(w, `<html><body><a rel="next" href="/page/2">next</a></body></html>`)
			case "/page/2":
				select {
				case <-release:
					fmt.Fprint(w, `<html><body><h1>page 2</h1></body></html>`)
				case <-r.Context().Done():
				}
			default:
				http.NotFound(w, r)
			}
		})},
	}
	server.Start()
	t.Cleanup(server.Close)
	return server, release
}

func waitForState(t *testing.T, statePath string, want func(*progressState) bool) *progressState {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		data, err := os.ReadFile(statePath)
		if err == nil {
			var state progressState
			if json.Unmarshal(data, &state) == nil && want(&state) {
				return &state
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("timed out waiting for progress state")
	return nil
}

func TestResumeAfterInterruption(t *testing.T) {
	// two distinct hosts so per-host serialization does not couple the seeds
	serverA := chainServer(t, 2)
	serverB, release := gatedServer(t, "127.0.0.2")
	seeds := writeSeedsFile(t, serverA.URL+"/page/1\n"+serverB.URL+"/page/1\n")
	dir := t.TempDir()
	opts := batchTestOptions(seeds, dir)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		summary, err := runBatch(ctx, opts)
		if err != nil {
			t.Errorf("interrupted run failed: %v", err)
			return
		}
		if !summary.Interrupted {
			t.Errorf("expected interrupted summary, got %+v", summary)
		}
	}()

	// wait until seed B is persisted as in_progress at page 2, then cancel
	waitForState(t, opts.statePath, func(state *progressState) bool {
		return len(state.Seeds) == 2 &&
			state.Seeds[0].Status == seedStatusDone &&
			state.Seeds[1].Status == seedStatusInProgress &&
			state.Seeds[1].NextPage == 2
	})
	cancel()
	<-done
	close(release)

	// resume: must complete and produce exactly the reference output
	summary, err := runBatch(context.Background(), opts)
	if err != nil {
		t.Fatalf("resume run failed: %v", err)
	}
	if summary.Failed != 0 || summary.Interrupted {
		t.Fatalf("resume summary: %+v", summary)
	}
	gotOut, err := os.ReadFile(opts.outPath)
	if err != nil {
		t.Fatal(err)
	}

	// reference: same servers, no interruption
	refDir := t.TempDir()
	if _, err := runBatch(context.Background(), batchTestOptions(seeds, refDir)); err != nil {
		t.Fatalf("reference run failed: %v", err)
	}
	refOut, err := os.ReadFile(filepath.Join(refDir, "out.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	if string(gotOut) != string(refOut) {
		t.Errorf("resumed output differs from uninterrupted run:\ngot:\n%s\nwant:\n%s", gotOut, refOut)
	}
}

func TestRerunAfterCompletion(t *testing.T) {
	server1 := chainServer(t, 2)
	server2 := chainServer(t, 1)
	seeds := writeSeedsFile(t, server1.URL+"/page/1\n"+server2.URL+"/page/1\n")
	opts := batchTestOptions(seeds, t.TempDir())

	summary, err := runBatch(context.Background(), opts)
	if err != nil {
		t.Fatalf("first run failed: %v", err)
	}
	if summary.Succeeded != 2 || summary.Pages != 3 {
		t.Fatalf("first run summary: %+v", summary)
	}
	first, err := os.ReadFile(opts.outPath)
	if err != nil {
		t.Fatal(err)
	}

	summary, err = runBatch(context.Background(), opts)
	if err != nil {
		t.Fatalf("second run failed: %v", err)
	}
	if summary.Skipped != 2 || summary.Succeeded != 0 || summary.Failed != 0 || summary.Interrupted {
		t.Errorf("second run summary: %+v, want 2 skipped and nothing else", summary)
	}
	second, err := os.ReadFile(opts.outPath)
	if err != nil {
		t.Fatal(err)
	}
	if string(first) != string(second) {
		t.Errorf("output changed on rerun:\nfirst:\n%s\nsecond:\n%s", first, second)
	}
}

func TestFailedSeedPersistedAndSkippedOnRerun(t *testing.T) {
	var brokenHits int32
	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/ok/1":
			fmt.Fprint(w, `<html><body><a rel="next" href="/broken">next</a></body></html>`)
		case "/broken":
			atomic.AddInt32(&brokenHits, 1)
			w.WriteHeader(http.StatusNotFound)
		case "/good":
			fmt.Fprint(w, `<html><body><h1>good</h1></body></html>`)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	badSeed := server.URL + "/ok/1"
	goodSeed := server.URL + "/good"
	seeds := writeSeedsFile(t, badSeed+"\n"+goodSeed+"\n")
	opts := batchTestOptions(seeds, t.TempDir())

	summary, err := runBatch(context.Background(), opts)
	if err != nil {
		t.Fatalf("first run failed: %v", err)
	}
	if summary.Failed != 1 || summary.Succeeded != 1 {
		t.Fatalf("first run summary: %+v", summary)
	}
	if len(summary.Failures) != 1 || summary.Failures[0].Seed != badSeed ||
		summary.Failures[0].URL != server.URL+"/broken" {
		t.Errorf("unexpected failures: %+v", summary.Failures)
	}
	// partial pages of the failed seed are still written
	records := readRecords(t, opts.outPath)
	if len(records) != 2 || records[0].URL != badSeed || records[1].URL != goodSeed {
		t.Errorf("unexpected records: %+v", records)
	}
	hitsAfterFirst := atomic.LoadInt32(&brokenHits)
	first, _ := os.ReadFile(opts.outPath)

	// rerun: failed seed is skipped entirely, failure still reported
	summary, err = runBatch(context.Background(), opts)
	if err != nil {
		t.Fatalf("second run failed: %v", err)
	}
	if summary.Failed != 1 || summary.Skipped != 1 || summary.Succeeded != 0 {
		t.Errorf("second run summary: %+v", summary)
	}
	if len(summary.Failures) != 1 || summary.Failures[0].URL != server.URL+"/broken" {
		t.Errorf("second run failures: %+v", summary.Failures)
	}
	if got := atomic.LoadInt32(&brokenHits); got != hitsAfterFirst {
		t.Errorf("/broken requested again on rerun: %d -> %d", hitsAfterFirst, got)
	}
	second, _ := os.ReadFile(opts.outPath)
	if string(first) != string(second) {
		t.Errorf("output changed on rerun:\nfirst:\n%s\nsecond:\n%s", first, second)
	}
}
