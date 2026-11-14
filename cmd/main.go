package main

import (
	"context"
	"fmt"
	"github.com/alecthomas/kingpin/v2"
	"github.com/itzg/restify"
	"github.com/yhat/scrape"
	"golang.org/x/net/html"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
)

var (
	version = "dev"
	commit  = "master"
)

var (
	urlArg = kingpin.Arg("url", "A URL to RESTify into JSON. Not used when --seeds is given.").
		URL()
	byClass = kingpin.Flag("class", "If specified, first-level elements encountered with this class will be extracted.").
		String()
	byId = kingpin.Flag("id", "If specified, the element with this id will be extracted.").
		String()
	byTagName   = kingpin.Flag("tag", "If specified, the first-level element with this tag name will be extracted.").String()
	byAttribute = kingpin.Flag("attribute",
		"If specified, as key=value, the element with the given attribute name set to the given value is extracted.").
		String()
	showVersion = kingpin.Flag("version", "Print version and exit").
			Bool()
	debug = kingpin.Flag("debug", "Enable debugging output").
		Bool()
	userAgent = kingpin.Flag("user-agent", "user-agent header to provide with request").
			Default("restify/" + version).
			String()
	headers = kingpin.Flag("headers", "Additional headers to pass with request").
		StringMap()
	seedsPath = kingpin.Flag("seeds", "Path to a file of seed URLs, one per line. Enables batch mode.").
			String()
	outPath = kingpin.Flag("out", "Path of the JSONL file batch mode writes page records to").
		String()
	statePath = kingpin.Flag("state", "Path of the progress file batch mode uses to resume interrupted runs").
			String()
	maxPages = kingpin.Flag("max-pages", "Maximum number of pages to fetch per seed when following rel=next links").
			Default("5").Int()
	concurrency = kingpin.Flag("concurrency", "Maximum number of requests in flight at once in batch mode").
			Default("4").Int()
	perHostInterval = kingpin.Flag("per-host-interval", "Minimum interval between consecutive requests to the same host").
			Default("500ms").Duration()
	maxRetries = kingpin.Flag("max-retries", "Maximum retries for retryable failures (network errors, 429, 5xx)").
			Default("3").Int()
)

func main() {

	kingpin.Parse()

	if *showVersion {
		log.Printf("Version: %s, Commit: %s\n", version, commit)
		os.Exit(0)
	}

	configs := make([]restify.RequestConfig, 0)
	if headers != nil {
		configs = append(configs, restify.WithHeaders(*headers))
	}

	if *seedsPath != "" {
		runBatchMode()
		return
	}

	if *urlArg == nil {
		kingpin.CommandLine.Fatalf("required argument 'url' not provided")
	}

	root, err := restify.LoadContent(*urlArg, *userAgent, configs...)
	if err != nil {
		log.Fatal("Failed to load content: ", err)
	}

	var subset []*html.Node
	if *byId != "" {
		elem, ok := restify.FindSubsetById(root, *byId)
		if !ok {
			log.Fatalf("Unable to find an element with the ID '%s'\n", *byId)
		}
		subset = append(subset, elem)
	} else if *byClass != "" {
		subset = restify.FindSubsetByClass(root, *byClass)
		if len(subset) == 0 {
			log.Fatalf("Unable to find an element with the class '%s'\n", *byClass)
		}
	} else if *byTagName != "" {
		subset = restify.FindSubsetByTagName(root, *byTagName)
		if len(subset) == 0 {
			log.Fatalf("Unable to find an element with the tag name '%s'\n", *byTagName)
		}
	} else if *byAttribute != "" {
		keyVal := strings.SplitN(*byAttribute, "=", 2)
		key := keyVal[0]
		if len(keyVal) == 1 {
			subset = restify.FindSubsetByAttributeName(root, key)
		} else {
			subset = restify.FindSubsetByAttributeNameValue(root, key, keyVal[1])
		}
		if len(subset) == 0 {
			log.Fatalf("Unable to find an element with attribute matcher %s", *byAttribute)
		}
	} else {
		subset = append(subset, root)
	}

	asJson, err := restify.ConvertHtmlToJson(subset)
	if err != nil {
		log.Fatal("Failed to parse HTML into JSON", err)
	}

	fmt.Print(string(asJson))
}

func runBatchMode() {
	if *outPath == "" {
		kingpin.CommandLine.Fatalf("--out is required when --seeds is given")
	}
	if *statePath == "" {
		kingpin.CommandLine.Fatalf("--state is required when --seeds is given")
	}

	opts := batchOptions{
		seedsPath:       *seedsPath,
		outPath:         *outPath,
		statePath:       *statePath,
		maxPages:        *maxPages,
		concurrency:     *concurrency,
		perHostInterval: *perHostInterval,
		maxRetries:      *maxRetries,
		userAgent:       *userAgent,
		debug:           *debug,
	}
	if headers != nil {
		opts.headers = *headers
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	summary, err := runBatch(ctx, opts)
	if err != nil {
		log.Fatal("Batch run failed: ", err)
	}
	printBatchSummary(summary)
	if summary.Failed > 0 {
		os.Exit(1)
	}
}

func printBatchSummary(summary *batchSummary) {
	fmt.Printf("Batch summary: %d succeeded, %d failed, %d skipped, %d deduplicated, %d pages written\n",
		summary.Succeeded, summary.Failed, summary.Skipped, summary.Deduped, summary.Pages)
	for _, failure := range summary.Failures {
		fmt.Printf("  FAILED %s (page %s): %s\n", failure.Seed, failure.URL, failure.Reason)
	}
}

func matchByAttribute(key, value string) scrape.Matcher {
	return func(node *html.Node) bool {
		if node.Type == html.ElementNode {
			result := scrape.Attr(node, key)
			if *debug {
				fmt.Printf("Saw %s and result %s with attrs %+v\n", node.Data, result, node.Attr)
			}
			if result != "" && (value == "" || value == result) {
				return true
			}
		}
		return false
	}
}
