package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/yhat/scrape"
	"golang.org/x/net/html"
)

// batchOptions carries every setting batch mode needs, collected from the
// command line flags in main.
type batchOptions struct {
	seedsPath       string
	outPath         string
	statePath       string
	maxPages        int
	concurrency     int
	perHostInterval time.Duration
	maxRetries      int
	userAgent       string
	headers         map[string]string
	debug           bool
}

// pageRecord is one line of the batch JSONL output.
type pageRecord struct {
	URL    string          `json:"url"`
	Seed   string          `json:"seed"`
	Page   int             `json:"page"`
	Status int             `json:"status"`
	Nodes  json.RawMessage `json:"nodes"`
}

// seedFailure describes a seed whose pagination chain could not be completed.
type seedFailure struct {
	Seed   string
	URL    string
	Reason string
}

// batchSummary aggregates the outcome of a batch run for the final report.
type batchSummary struct {
	Succeeded   int
	Failed      int
	Skipped     int
	Deduped     int
	Pages       int
	Interrupted bool
	Failures    []seedFailure
}

// loadSeeds reads the seeds file: one URL per line, blank lines and lines
// starting with '#' are ignored, and duplicates (by normalized form) are
// collapsed while preserving first-seen order.
func loadSeeds(path string) ([]string, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("failed to open seeds file: %w", err)
	}
	defer file.Close()

	var seeds []string
	seen := make(map[string]bool)
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		norm, err := normalizeURL(line)
		if err != nil {
			// keep invalid entries so they are reported as failed seeds
			norm = line
		}
		if seen[norm] {
			continue
		}
		seen[norm] = true
		seeds = append(seeds, line)
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("failed to read seeds file: %w", err)
	}
	return seeds, nil
}

// normalizeURL produces the canonical form used for de-duplication: fragment
// removed, scheme and host lower-cased, default port dropped.
func normalizeURL(raw string) (string, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return "", err
	}
	if u.Scheme == "" || u.Host == "" {
		return "", fmt.Errorf("not an absolute URL: %q", raw)
	}
	u.Scheme = strings.ToLower(u.Scheme)
	host := strings.ToLower(u.Hostname())
	if port := u.Port(); port != "" && !isDefaultPort(u.Scheme, port) {
		host = net.JoinHostPort(host, port)
	}
	u.Host = host
	u.Fragment = ""
	if u.Path == "" {
		u.Path = "/"
	}
	return u.String(), nil
}

func isDefaultPort(scheme, port string) bool {
	switch scheme {
	case "http":
		return port == "80"
	case "https":
		return port == "443"
	}
	return false
}

// pageResult is the fetched and parsed representation of a single page.
type pageResult struct {
	finalURL string
	status   int
	nextHref string
	nodes    json.RawMessage
}

// seedOutcome is what one seed's crawl produced.
type seedOutcome struct {
	records     []pageRecord
	deduped     int
	failure     *seedFailure
	interrupted bool
}

// crawler holds the shared state of a batch run.
type crawler struct {
	opts    batchOptions
	fetcher *fetcher
	dedup   *deduper
}

func newCrawler(opts batchOptions) *crawler {
	return &crawler{
		opts:    opts,
		fetcher: newFetcher(opts),
		dedup:   newDeduper(),
	}
}

// findNextLink returns the href of the first element carrying rel="next".
func findNextLink(root *html.Node) string {
	matcher := func(node *html.Node) bool {
		if node.Type != html.ElementNode {
			return false
		}
		rel := scrape.Attr(node, "rel")
		if rel == "" || scrape.Attr(node, "href") == "" {
			return false
		}
		for _, token := range strings.Fields(rel) {
			if token == "next" {
				return true
			}
		}
		return false
	}
	if node, ok := scrape.Find(root, matcher); ok {
		return scrape.Attr(node, "href")
	}
	return ""
}

// resolveURL makes href absolute against the current page's URL.
func resolveURL(base, href string) string {
	if href == "" {
		return ""
	}
	baseURL, err := url.Parse(base)
	if err != nil {
		return ""
	}
	ref, err := url.Parse(href)
	if err != nil {
		return ""
	}
	return baseURL.ResolveReference(ref).String()
}

// crawlSeed follows the rel="next" chain starting at seed. Pages already
// fetched by another seed are not duplicated, but pagination still advances
// using their content. A failure does not discard pages already gathered.
func (c *crawler) crawlSeed(ctx context.Context, seedIndex int, seed string) seedOutcome {
	var outcome seedOutcome
	current := seed
	for page := 1; current != "" && page <= c.opts.maxPages; page++ {
		norm, err := normalizeURL(current)
		if err != nil {
			outcome.failure = &seedFailure{Seed: seed, URL: current, Reason: err.Error()}
			return outcome
		}
		result, owned, err := c.dedup.get(ctx, seedIndex, norm, current, c.fetcher.fetch)
		if err != nil {
			if ctx.Err() != nil {
				outcome.interrupted = true
				return outcome
			}
			outcome.failure = &seedFailure{Seed: seed, URL: current, Reason: err.Error()}
			return outcome
		}
		if !owned {
			outcome.deduped++
		} else {
			outcome.records = append(outcome.records, pageRecord{
				URL:    result.finalURL,
				Seed:   seed,
				Page:   page,
				Status: result.status,
				Nodes:  result.nodes,
			})
		}
		current = resolveURL(result.finalURL, result.nextHref)
	}
	return outcome
}

// runBatch executes the whole batch crawl and reports the summary. Seeds are
// crawled concurrently; the fetcher bounds the overall request rate.
func runBatch(ctx context.Context, opts batchOptions) (*batchSummary, error) {
	seeds, err := loadSeeds(opts.seedsPath)
	if err != nil {
		return nil, err
	}
	if len(seeds) == 0 {
		return nil, fmt.Errorf("no seeds found in %s", opts.seedsPath)
	}

	c := newCrawler(opts)
	outcomes := make([]seedOutcome, len(seeds))
	var wg sync.WaitGroup
	for i, seed := range seeds {
		wg.Add(1)
		go func() {
			defer wg.Done()
			outcomes[i] = c.crawlSeed(ctx, i, seed)
		}()
	}
	wg.Wait()

	summary := &batchSummary{}
	for _, outcome := range outcomes {
		summary.Pages += len(outcome.records)
		summary.Deduped += outcome.deduped
		switch {
		case outcome.interrupted:
			summary.Interrupted = true
		case outcome.failure != nil:
			summary.Failed++
			summary.Failures = append(summary.Failures, *outcome.failure)
		default:
			summary.Succeeded++
		}
	}
	return summary, nil
}
