package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"sort"
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

	// output and progress state; nil in stateless mode (unit tests)
	out       *os.File
	statePath string
	state     *progressState

	mu            sync.Mutex
	persistedSeen map[string]bool
	frontier      int
	buffers       map[int][]bufferedRecord
	progress      map[int]seedProgress
	finished      map[int]*seedState
	err           error
}

func newCrawler(opts batchOptions) *crawler {
	return &crawler{
		opts:     opts,
		fetcher:  newFetcher(opts),
		dedup:    newDeduper(),
		buffers:  make(map[int][]bufferedRecord),
		progress: make(map[int]seedProgress),
		finished: make(map[int]*seedState),
	}
}

// bufferedRecord is a fetched page waiting for its turn in the output
// ordering, along with the normalized request URL used for de-duplication.
type bufferedRecord struct {
	rec     pageRecord
	reqNorm string
}

// seedProgress tracks how far a seed has advanced through its chain.
type seedProgress struct {
	nextURL  string
	nextPage int
}

// attachOutput switches the crawler to stateful mode: records are appended
// to out in seed order and progress is persisted after every write.
func (c *crawler) attachOutput(out *os.File, statePath string, state *progressState) {
	c.out = out
	c.statePath = statePath
	c.state = state
	c.persistedSeen = make(map[string]bool, len(state.Seen))
	for _, norm := range state.Seen {
		c.persistedSeen[norm] = true
	}
	// seeds recorded as done or failed may have already advanced the frontier
	for c.frontier = 0; c.frontier < len(state.Seeds); c.frontier++ {
		status := state.Seeds[c.frontier].Status
		if status != seedStatusDone && status != seedStatusFailed {
			break
		}
	}
}

func (c *crawler) isPersistedSeen(norm string) bool {
	if c.state == nil {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.persistedSeen[norm]
}

// reportPage records one processed page: a fetched record to write, or nil
// for a de-duplicated page. When the seed is at the output frontier the
// record is appended immediately and progress is persisted.
func (c *crawler) reportPage(seedIndex int, rec *bufferedRecord, nextURL string, nextPage int) {
	if c.state == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.progress[seedIndex] = seedProgress{nextURL: nextURL, nextPage: nextPage}
	if rec == nil {
		c.state.Deduped++
	} else {
		c.buffers[seedIndex] = append(c.buffers[seedIndex], *rec)
	}
	if seedIndex == c.frontier {
		c.flushBufferLocked(seedIndex)
		st := &c.state.Seeds[seedIndex]
		st.Status = seedStatusInProgress
		st.NextURL = nextURL
		st.NextPage = nextPage
		c.saveLocked()
	}
}

// finishSeed marks a seed done or failed and advances the output frontier,
// flushing any earlier seeds that completed out of order.
func (c *crawler) finishSeed(seedIndex int, outcome seedOutcome) {
	if c.state == nil || outcome.interrupted {
		return
	}
	final := seedState{URL: c.state.Seeds[seedIndex].URL, Status: seedStatusDone}
	if outcome.failure != nil {
		final.Status = seedStatusFailed
		final.FailedPage = outcome.failure.URL
		final.Error = outcome.failure.Reason
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.finished[seedIndex] = &final
	c.advanceLocked()
}

// advanceLocked moves the output frontier past finished seeds, writing their
// buffered records, and makes the new frontier seed's progress durable.
func (c *crawler) advanceLocked() {
	for c.frontier < len(c.state.Seeds) {
		final, ok := c.finished[c.frontier]
		if !ok {
			break
		}
		c.flushBufferLocked(c.frontier)
		c.state.Seeds[c.frontier] = *final
		delete(c.finished, c.frontier)
		delete(c.progress, c.frontier)
		c.frontier++
		c.saveLocked()
	}
	if c.frontier < len(c.state.Seeds) {
		if prog, ok := c.progress[c.frontier]; ok {
			c.flushBufferLocked(c.frontier)
			st := &c.state.Seeds[c.frontier]
			st.Status = seedStatusInProgress
			st.NextURL = prog.nextURL
			st.NextPage = prog.nextPage
			c.saveLocked()
		}
	}
}

// flushBufferLocked appends a seed's buffered records to the output file.
func (c *crawler) flushBufferLocked(seedIndex int) {
	for _, buffered := range c.buffers[seedIndex] {
		line, err := json.Marshal(buffered.rec)
		if err != nil {
			c.setErrLocked(fmt.Errorf("failed to encode record: %w", err))
			continue
		}
		n, err := c.out.Write(append(line, '\n'))
		if err != nil {
			c.setErrLocked(fmt.Errorf("failed to write record: %w", err))
			return
		}
		c.state.OutOffset += int64(n)
		c.state.Pages++
		c.persistedSeen[buffered.reqNorm] = true
		if finalNorm, err := normalizeURL(buffered.rec.URL); err == nil {
			c.persistedSeen[finalNorm] = true
		}
	}
	c.buffers[seedIndex] = nil
}

func (c *crawler) saveLocked() {
	seen := make([]string, 0, len(c.persistedSeen))
	for norm := range c.persistedSeen {
		seen = append(seen, norm)
	}
	sort.Strings(seen)
	c.state.Seen = seen
	if err := saveProgress(c.statePath, c.state); err != nil {
		c.setErrLocked(err)
	}
}

func (c *crawler) setErrLocked(err error) {
	if c.err == nil {
		c.err = err
	}
}

func (c *crawler) firstErr() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.err
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
	page := 1
	current := seed
	if c.state != nil && c.state.Seeds[seedIndex].Status == seedStatusInProgress {
		// resume from the persisted checkpoint
		page = c.state.Seeds[seedIndex].NextPage
		current = c.state.Seeds[seedIndex].NextURL
	}
	for ; current != "" && page <= c.opts.maxPages; page++ {
		norm, err := normalizeURL(current)
		if err != nil {
			outcome.failure = &seedFailure{Seed: seed, URL: current, Reason: err.Error()}
			return outcome
		}
		var result *pageResult
		var owned bool
		if c.isPersistedSeen(norm) {
			// fetched in a previous run: no new record, but fetch again
			// (shared within this run) to continue the chain
			result, _, err = c.dedup.get(ctx, -1, norm, current, c.fetcher.fetch)
		} else {
			result, owned, err = c.dedup.get(ctx, seedIndex, norm, current, c.fetcher.fetch)
		}
		if err != nil {
			if ctx.Err() != nil {
				outcome.interrupted = true
				return outcome
			}
			outcome.failure = &seedFailure{Seed: seed, URL: current, Reason: err.Error()}
			return outcome
		}
		next := resolveURL(result.finalURL, result.nextHref)
		if owned {
			rec := pageRecord{
				URL:    result.finalURL,
				Seed:   seed,
				Page:   page,
				Status: result.status,
				Nodes:  result.nodes,
			}
			outcome.records = append(outcome.records, rec)
			c.reportPage(seedIndex, &bufferedRecord{rec: rec, reqNorm: norm}, next, page+1)
		} else {
			outcome.deduped++
			c.reportPage(seedIndex, nil, next, page+1)
		}
		current = next
	}
	return outcome
}

// openOutput opens the JSONL output file, truncating it to the offset
// recorded in the progress file so a resumed run continues exactly where
// the previous one stopped.
func openOutput(path string, offset int64) (*os.File, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, fmt.Errorf("failed to open output file: %w", err)
	}
	if err := file.Truncate(offset); err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to truncate output file: %w", err)
	}
	if _, err := file.Seek(offset, io.SeekStart); err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to seek output file: %w", err)
	}
	return file, nil
}

// runBatch executes the whole batch crawl and reports the summary. Seeds are
// crawled concurrently; the fetcher bounds the overall request rate. Records
// are appended to the output file in seed order and progress is persisted
// after every record, so an interrupted run can resume where it stopped.
func runBatch(ctx context.Context, opts batchOptions) (*batchSummary, error) {
	seeds, err := loadSeeds(opts.seedsPath)
	if err != nil {
		return nil, err
	}
	if len(seeds) == 0 {
		return nil, fmt.Errorf("no seeds found in %s", opts.seedsPath)
	}

	state := loadProgress(opts.statePath, seeds)
	if state != nil {
		// progress is only usable together with its output file
		if _, err := os.Stat(opts.outPath); err != nil {
			state = nil
		}
	}
	if state == nil {
		state = freshProgress(seeds)
	}

	out, err := openOutput(opts.outPath, state.OutOffset)
	if err != nil {
		return nil, err
	}

	c := newCrawler(opts)
	c.attachOutput(out, opts.statePath, state)

	initialStatus := make([]string, len(seeds))
	for i := range seeds {
		initialStatus[i] = state.Seeds[i].Status
	}

	var wg sync.WaitGroup
	for i, seed := range seeds {
		if initialStatus[i] == seedStatusDone || initialStatus[i] == seedStatusFailed {
			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			outcome := c.crawlSeed(ctx, i, seed)
			c.finishSeed(i, outcome)
		}()
	}
	wg.Wait()
	if err := out.Close(); err != nil {
		return nil, fmt.Errorf("failed to close output file: %w", err)
	}
	if err := c.firstErr(); err != nil {
		return nil, err
	}

	summary := &batchSummary{Pages: state.Pages, Deduped: state.Deduped}
	for i := range seeds {
		switch state.Seeds[i].Status {
		case seedStatusDone:
			if initialStatus[i] == seedStatusDone {
				summary.Skipped++
			} else {
				summary.Succeeded++
			}
		case seedStatusFailed:
			summary.Failed++
			summary.Failures = append(summary.Failures, seedFailure{
				Seed:   state.Seeds[i].URL,
				URL:    state.Seeds[i].FailedPage,
				Reason: state.Seeds[i].Error,
			})
		default:
			summary.Interrupted = true
		}
	}
	return summary, nil
}
