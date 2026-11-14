package main

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/itzg/restify"
	"golang.org/x/net/html"
)

// fetchError classifies a failed fetch so the retry loop knows whether
// another attempt is worthwhile.
type fetchError struct {
	msg       string
	retryable bool
}

func (e *fetchError) Error() string { return e.msg }

// hostLimiter serializes requests to one host and enforces the minimum
// interval between consecutive request starts. The semaphore has capacity
// one; last is only accessed while holding it.
type hostLimiter struct {
	sem  chan struct{}
	last time.Time
}

// fetcher executes HTTP requests with a global in-flight cap, per-host
// serialization plus pacing, and retries for transient failures.
type fetcher struct {
	client          *http.Client
	userAgent       string
	headers         map[string]string
	maxRetries      int
	perHostInterval time.Duration
	globalSem       chan struct{}
	hostsMu         sync.Mutex
	hosts           map[string]*hostLimiter
}

func newFetcher(opts batchOptions) *fetcher {
	global := opts.concurrency
	if global < 1 {
		global = 1
	}
	return &fetcher{
		client: &http.Client{
			Timeout: restify.HttpRequestTimeout,
		},
		userAgent:       opts.userAgent,
		headers:         opts.headers,
		maxRetries:      opts.maxRetries,
		perHostInterval: opts.perHostInterval,
		globalSem:       make(chan struct{}, global),
		hosts:           make(map[string]*hostLimiter),
	}
}

// fetch retrieves one page, retrying retryable failures up to maxRetries
// times and honoring Retry-After when the server provides it.
func (f *fetcher) fetch(ctx context.Context, rawURL string) (*pageResult, error) {
	for attempt := 0; ; attempt++ {
		result, retryAfter, ferr := f.fetchOnce(ctx, rawURL)
		if ferr == nil {
			return result, nil
		}
		if !ferr.retryable || attempt >= f.maxRetries || ctx.Err() != nil {
			return nil, ferr
		}
		wait := retryAfter
		if wait <= 0 {
			wait = time.Duration(200*(1<<attempt)) * time.Millisecond
		}
		select {
		case <-time.After(wait):
		case <-ctx.Done():
			return nil, &fetchError{msg: ctx.Err().Error()}
		}
	}
}

// fetchOnce performs a single attempt. On retryable HTTP statuses it also
// returns the server-provided Retry-After delay, if any.
func (f *fetcher) fetchOnce(ctx context.Context, rawURL string) (*pageResult, time.Duration, *fetchError) {
	parsed, err := url.Parse(rawURL)
	if err != nil || parsed.Host == "" {
		return nil, 0, &fetchError{msg: fmt.Sprintf("invalid URL %q", rawURL)}
	}

	select {
	case f.globalSem <- struct{}{}:
		defer func() { <-f.globalSem }()
	case <-ctx.Done():
		return nil, 0, &fetchError{msg: ctx.Err().Error()}
	}

	releaseHost, err := f.acquireHost(ctx, strings.ToLower(parsed.Hostname()))
	if err != nil {
		return nil, 0, &fetchError{msg: err.Error()}
	}
	defer releaseHost()

	request, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return nil, 0, &fetchError{msg: fmt.Sprintf("failed to build request: %v", err)}
	}
	request.Header.Set("accept", "*/*")
	if f.userAgent != "" {
		request.Header.Set("user-agent", f.userAgent)
	}
	for key, value := range f.headers {
		request.Header.Set(key, value)
	}

	resp, err := f.client.Do(request)
	if err != nil {
		if ctx.Err() != nil {
			return nil, 0, &fetchError{msg: ctx.Err().Error()}
		}
		return nil, 0, &fetchError{msg: fmt.Sprintf("request failed: %v", err), retryable: true}
	}
	defer resp.Body.Close()

	status := resp.StatusCode
	switch {
	case status >= 200 && status <= 299:
		// success, handled below
	case status == http.StatusTooManyRequests || status >= 500:
		return nil, parseRetryAfter(resp.Header.Get("Retry-After")),
			&fetchError{msg: fmt.Sprintf("server returned status %d", status), retryable: true}
	default:
		return nil, 0, &fetchError{msg: fmt.Sprintf("server returned status %d", status)}
	}

	root, err := html.Parse(resp.Body)
	if err != nil {
		return nil, 0, &fetchError{msg: fmt.Sprintf("failed to parse response body: %v", err)}
	}

	nodes, err := restify.ConvertHtmlToJson([]*html.Node{root})
	if err != nil {
		return nil, 0, &fetchError{msg: fmt.Sprintf("failed to convert page to JSON: %v", err)}
	}

	return &pageResult{
		finalURL: resp.Request.URL.String(),
		status:   status,
		nextHref: findNextLink(root),
		nodes:    nodes,
	}, 0, nil
}

// acquireHost blocks until the host may be requested again: no other request
// to the host is in flight and the per-host interval has elapsed. The
// returned function releases the host.
func (f *fetcher) acquireHost(ctx context.Context, host string) (func(), error) {
	f.hostsMu.Lock()
	limiter, ok := f.hosts[host]
	if !ok {
		limiter = &hostLimiter{sem: make(chan struct{}, 1)}
		f.hosts[host] = limiter
	}
	f.hostsMu.Unlock()

	select {
	case limiter.sem <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	if wait := time.Until(limiter.last.Add(f.perHostInterval)); wait > 0 {
		timer := time.NewTimer(wait)
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			<-limiter.sem
			return nil, ctx.Err()
		}
	}
	limiter.last = time.Now()
	return func() { <-limiter.sem }, nil
}

// parseRetryAfter interprets a Retry-After header value, either as seconds
// or as an HTTP date. It returns 0 when absent or unparseable.
func parseRetryAfter(value string) time.Duration {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0
	}
	if seconds, err := strconv.Atoi(value); err == nil {
		if seconds < 0 {
			return 0
		}
		return time.Duration(seconds) * time.Second
	}
	if when, err := http.ParseTime(value); err == nil {
		if delay := time.Until(when); delay > 0 {
			return delay
		}
	}
	return 0
}

// inFlightPage represents a fetch that is either in progress or completed;
// concurrent seeds asking for the same normalized URL share one fetch.
type inFlightPage struct {
	owner  int
	done   chan struct{}
	result *pageResult
	err    error
}

// deduper ensures each normalized URL is fetched at most once per run and
// tracks which seed owns the resulting record.
type deduper struct {
	mu    sync.Mutex
	pages map[string]*inFlightPage
}

func newDeduper() *deduper {
	return &deduper{pages: make(map[string]*inFlightPage)}
}

// get returns the page for rawURL (normalized as normURL), fetching it if
// this seed is the first to ask. owned reports whether the caller's seed
// claimed the URL and should therefore emit a record for it.
func (d *deduper) get(ctx context.Context, seedIndex int, normURL, rawURL string, fetch func(context.Context, string) (*pageResult, error)) (result *pageResult, owned bool, err error) {
	d.mu.Lock()
	if page, ok := d.pages[normURL]; ok {
		d.mu.Unlock()
		select {
		case <-page.done:
			return page.result, page.owner == seedIndex, page.err
		case <-ctx.Done():
			return nil, false, ctx.Err()
		}
	}
	page := &inFlightPage{owner: seedIndex, done: make(chan struct{})}
	d.pages[normURL] = page
	d.mu.Unlock()

	page.result, page.err = fetch(ctx, rawURL)

	// Register the post-redirect URL as an alias so later seeds linking
	// directly to it reuse this fetch too.
	if page.err == nil && page.result != nil {
		if finalNorm, normErr := normalizeURL(page.result.finalURL); normErr == nil && finalNorm != normURL {
			d.mu.Lock()
			if _, ok := d.pages[finalNorm]; !ok {
				d.pages[finalNorm] = page
			}
			d.mu.Unlock()
		}
	}

	close(page.done)
	return page.result, true, page.err
}
