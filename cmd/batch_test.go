package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func writeSeedsFile(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "seeds.txt")
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("failed to write seeds file: %v", err)
	}
	return path
}

func TestLoadSeeds(t *testing.T) {
	cases := []struct {
		name    string
		content string
		want    []string
	}{
		{
			name:    "skips blanks and comments",
			content: "# comment\n\nhttps://a.example/1\n   \n#another\nhttps://a.example/2\n",
			want:    []string{"https://a.example/1", "https://a.example/2"},
		},
		{
			name:    "trims whitespace",
			content: "  https://a.example/1  \n\thttps://a.example/2\t\n",
			want:    []string{"https://a.example/1", "https://a.example/2"},
		},
		{
			name:    "dedupes exact duplicates keeping order",
			content: "https://a.example/1\nhttps://a.example/2\nhttps://a.example/1\n",
			want:    []string{"https://a.example/1", "https://a.example/2"},
		},
		{
			name:    "dedupes by normalized form",
			content: "http://a.example/x#frag\nhttp://A.EXAMPLE:80/x\nhttp://a.example/x\n",
			want:    []string{"http://a.example/x#frag"},
		},
		{
			name:    "keeps invalid lines for failure reporting",
			content: "not-a-url\n",
			want:    []string{"not-a-url"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := loadSeeds(writeSeedsFile(t, tc.content))
			if err != nil {
				t.Fatalf("loadSeeds failed: %v", err)
			}
			if strings.Join(got, ",") != strings.Join(tc.want, ",") {
				t.Errorf("got %q, want %q", got, tc.want)
			}
		})
	}
}

func TestNormalizeURL(t *testing.T) {
	cases := []struct {
		name    string
		in      string
		want    string
		wantErr bool
	}{
		{name: "strips fragment", in: "http://a.example/x#frag", want: "http://a.example/x"},
		{name: "lowercases host", in: "http://A.Example.COM/x", want: "http://a.example.com/x"},
		{name: "lowercases scheme", in: "HTTP://a.example/x", want: "http://a.example/x"},
		{name: "drops default http port", in: "http://a.example:80/x", want: "http://a.example/x"},
		{name: "drops default https port", in: "https://a.example:443/x", want: "https://a.example/x"},
		{name: "keeps non-default port", in: "http://a.example:8080/x", want: "http://a.example:8080/x"},
		{name: "empty path becomes slash", in: "http://a.example", want: "http://a.example/"},
		{name: "keeps query", in: "http://a.example/x?a=1&b=2", want: "http://a.example/x?a=1&b=2"},
		{name: "rejects relative", in: "/just/a/path", wantErr: true},
		{name: "rejects empty", in: "", wantErr: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := normalizeURL(tc.in)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error, got %q", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tc.want {
				t.Errorf("got %q, want %q", got, tc.want)
			}
		})
	}
}

// chainServer serves a rel="next" pagination chain of the given length.
// Page i links to page i+1 with a relative href.
func chainServer(t *testing.T, pages int) *httptest.Server {
	t.Helper()
	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var page int
		if _, err := fmt.Sscanf(r.URL.Path, "/page/%d", &page); err != nil || page < 1 || page > pages {
			http.NotFound(w, r)
			return
		}
		next := ""
		if page < pages {
			next = fmt.Sprintf(`<a rel="next" href="/page/%d">next</a>`, page+1)
		}
		fmt.Fprintf(w, `<html><body><h1>page %d</h1>%s</body></html>`, page, next)
	}))
	t.Cleanup(server.Close)
	return server
}

func TestCrawlSeedPagination(t *testing.T) {
	server := chainServer(t, 3)

	cases := []struct {
		name      string
		seed      string
		maxPages  int
		wantPages []int
	}{
		{name: "follows chain to the end", seed: server.URL + "/page/1", maxPages: 5, wantPages: []int{1, 2, 3}},
		{name: "stops at max pages", seed: server.URL + "/page/1", maxPages: 2, wantPages: []int{1, 2}},
		{name: "single page without next", seed: server.URL + "/page/3", maxPages: 5, wantPages: []int{3}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := newCrawler(batchOptions{maxPages: tc.maxPages})
			outcome := c.crawlSeed(context.Background(), 0, tc.seed)
			if outcome.failure != nil {
				t.Fatalf("unexpected failure: %+v", outcome.failure)
			}
			records := outcome.records
			if len(records) != len(tc.wantPages) {
				t.Fatalf("got %d records, want %d", len(records), len(tc.wantPages))
			}
			for i, record := range records {
				if record.Page != i+1 {
					t.Errorf("record %d has page %d, want %d", i, record.Page, i+1)
				}
				wantURL := fmt.Sprintf("%s/page/%d", server.URL, tc.wantPages[i])
				if record.URL != wantURL {
					t.Errorf("record %d url %q, want %q", i, record.URL, wantURL)
				}
				if record.Status != http.StatusOK {
					t.Errorf("record %d status %d, want 200", i, record.Status)
				}
				if record.Seed != tc.seed {
					t.Errorf("record %d seed %q, want %q", i, record.Seed, tc.seed)
				}
				var nodes []map[string]any
				if err := json.Unmarshal(record.Nodes, &nodes); err != nil {
					t.Errorf("record %d nodes not valid JSON: %v", i, err)
				}
			}
		})
	}
}

func TestCrawlSeedFailureKeepsPartialRecords(t *testing.T) {
	server := chainServer(t, 3)
	c := newCrawler(batchOptions{maxPages: 5})

	outcome := c.crawlSeed(context.Background(), 0, server.URL+"/page/2")
	if outcome.failure != nil {
		t.Fatalf("unexpected failure: %+v", outcome.failure)
	}
	if len(outcome.records) != 2 {
		t.Fatalf("got %d records, want 2", len(outcome.records))
	}

	// page 3 is the last in the chain; starting beyond it 404s immediately
	outcome = c.crawlSeed(context.Background(), 0, server.URL+"/page/99")
	if outcome.failure == nil {
		t.Fatal("expected failure for missing page")
	}
	if len(outcome.records) != 0 {
		t.Fatalf("got %d records, want 0", len(outcome.records))
	}
	if outcome.failure.URL != server.URL+"/page/99" {
		t.Errorf("failure url %q, want %q", outcome.failure.URL, server.URL+"/page/99")
	}
}
