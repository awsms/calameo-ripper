package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"image"
	_ "image/jpeg"
	"io"
	"math"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf16"

	"golang.org/x/text/encoding/charmap"
	"google.golang.org/protobuf/encoding/protowire"
)

const (
	bookInfoEndpoint = "https://d.calameo.com/pinwheel/viewer/book/get?bkcode=%s"
	defaultWorkers   = 6
	defaultTimeout   = 45 * time.Second
)

var errTextBinUnavailable = errors.New("text.bin unavailable")

type bookResponse struct {
	Status  string `json:"status"`
	Content struct {
		ID   string `json:"id"`
		Key  string `json:"key"`
		Name string `json:"name"`
		URL  struct {
			Public string `json:"public"`
		} `json:"url"`
		Document struct {
			Width  int `json:"width"`
			Height int `json:"height"`
			Pages  int `json:"pages"`
		} `json:"document"`
		Domains struct {
			Secured struct {
				Image string `json:"image"`
				SVG   string `json:"svg"`
				Text  string `json:"text"`
			} `json:"secured"`
		} `json:"domains"`
		Features struct {
			SVG struct {
				Enabled bool `json:"enabled"`
			} `json:"svg"`
			Search struct {
				Bin struct {
					URL  string `json:"url"`
					Path string `json:"path"`
				} `json:"bin"`
			} `json:"search"`
		} `json:"features"`
	} `json:"content"`
}

type bookMetadata struct {
	ID         string
	Key        string
	Name       string
	PublicURL  string
	PageCount  int
	PageWidth  int
	PageHeight int
	ImageBase  string
	Token      string
	TextBinURL string
	TextBase   string
	TextPath   string
	SVGEnabled bool
}

type pageFile struct {
	Path   string
	Width  int
	Height int
}

type outputOptions struct {
	PDF  bool
	JPG  bool
	SVG  bool
	SVGZ bool
}

func main() {
	var (
		outputPath      = flag.String("o", "", "output PDF path")
		formats         = flag.String("formats", "pdf", "comma-separated outputs: pdf,jpg,svg,svgz")
		outDir          = flag.String("outdir", "", "output directory for generated files (PDF, text exports)")
		assetsOutDir    = flag.String("assets-outdir", "", "directory for raw page downloads when requesting jpg/svg/svgz")
		pdfSource       = flag.String("pdf-source", "svgz", "PDF source: svgz or jpg")
		renderer        = flag.String("svg-renderer", "auto", "SVG renderer for pdf-source=svgz: auto,resvg,rsvg")
		optimize        = flag.String("optimize-pdf", "lossless", "PDF optimization: off, lossless, lossy (requires gs)")
		jpegQ           = flag.Int("jpeg-quality", 82, "JPEG quality for lossy optimization (1-100)")
		textBinOut      = flag.String("text-bin", "", "save OCR/search text.bin to this path (use 'auto' or '-' for stdout)")
		textBinJSON     = flag.String("text-bin-json", "", "dump OCR/search text.bin to JSON (use 'auto' or '-' for stdout)")
		embedOCR        = flag.Bool("embed-ocr", true, "embed OCR text layer into PDF (uses text.bin)")
		ocrFlipY        = flag.Bool("ocr-flip-y", true, "flip OCR Y coordinates (enable if text is vertically inverted)")
		ocrFit          = flag.String("ocr-fit", "page", "OCR fit mode: page or bbox")
		ocrUseSVG       = flag.Bool("ocr-use-svg-size", true, "use SVG size for OCR scaling (default uses metadata page size)")
		ocrSort         = flag.String("ocr-sort", "line", "OCR sort order: source, line, or pdf")
		ocrPlace        = flag.String("ocr-placement", "simple", "OCR placement: simple or matrix")
		ocrDebugPage    = flag.Int("ocr-debug-page", 0, "overlay debug markers for this page (1-based, 0 disables)")
		ocrDebugMarkers = flag.Bool("ocr-debug-markers", false, "draw debug markers for OCR placement")
		ocrScaleX       = flag.Float64("ocr-scale-x", 1, "OCR X scale multiplier")
		ocrScaleY       = flag.Float64("ocr-scale-y", 1, "OCR Y scale multiplier")
		ocrOffsetX      = flag.Float64("ocr-offset-x", 0, "OCR X offset (source units, applied before scaling)")
		ocrOffsetY      = flag.Float64("ocr-offset-y", 0, "OCR Y offset (source units, applied before scaling)")
		accountPages    = flag.Int("account-pages", 10, "max account pages to scan when input is /accounts/<id>")
		overwrite       = flag.Bool("overwrite", false, "overwrite existing output files")
		workers         = flag.Int("workers", defaultWorkers, "number of concurrent page downloads")
		timeout         = flag.Duration("timeout", defaultTimeout, "HTTP timeout")
	)
	flag.Parse()

	if flag.NArg() < 1 {
		fmt.Fprintf(os.Stderr, "usage: %s [flags] <calameo-url-or-bkcode> [more...]\n", filepath.Base(os.Args[0]))
		os.Exit(2)
	}

	if flag.NArg() > 1 && *outputPath != "" {
		exitErr(errors.New("-o can only be used with a single input"))
	}

	transport := &http.Transport{
		ForceAttemptHTTP2: false,
	}
	client := &http.Client{
		Timeout:   *timeout,
		Transport: transport,
	}
	ctx := context.Background()

	outputs, err := parseOutputOptions(*formats)
	if err != nil {
		exitErr(err)
	}
	inputs, err := expandInputs(ctx, client, flag.Args(), *accountPages)
	if err != nil {
		exitErr(err)
	}
	if len(inputs) == 0 {
		exitErr(errors.New("no inputs to process after expansion"))
	}
	if len(inputs) > 1 && *outputPath != "" {
		exitErr(errors.New("-o can only be used with a single input"))
	}
	for _, input := range inputs {
		if err := processBook(ctx, client, input, len(inputs), outputs, *outputPath, *outDir, *assetsOutDir, *textBinOut, *textBinJSON, *embedOCR, *pdfSource, *renderer, *optimize, *jpegQ, *ocrFlipY, *ocrFit, *ocrScaleX, *ocrScaleY, *ocrOffsetX, *ocrOffsetY, *ocrUseSVG, *ocrSort, *ocrPlace, *ocrDebugPage, *ocrDebugMarkers, *overwrite, *workers); err != nil {
			exitErr(err)
		}
	}
}

func expandInputs(ctx context.Context, client *http.Client, inputs []string, accountPages int) ([]string, error) {
	if len(inputs) == 0 {
		return nil, nil
	}
	if accountPages <= 0 {
		accountPages = 1
	}
	seen := make(map[string]struct{})
	var out []string
	for _, input := range inputs {
		if isAccountURL(input) {
			codes, err := fetchAccountBookCodes(ctx, client, input, accountPages)
			if err != nil {
				return nil, err
			}
			for _, code := range codes {
				if _, ok := seen[code]; ok {
					continue
				}
				seen[code] = struct{}{}
				out = append(out, code)
			}
			continue
		}
		if _, ok := seen[input]; ok {
			continue
		}
		seen[input] = struct{}{}
		out = append(out, input)
	}
	return out, nil
}

func isAccountURL(input string) bool {
	u, err := url.Parse(strings.TrimSpace(input))
	if err != nil || u.Scheme == "" || u.Host == "" {
		return false
	}
	parts := strings.Split(strings.Trim(u.Path, "/"), "/")
	if len(parts) >= 2 && parts[0] == "accounts" && parts[1] != "" {
		return true
	}
	return false
}

func fetchAccountBookCodes(ctx context.Context, client *http.Client, accountURL string, maxPages int) ([]string, error) {
	accountID, err := extractAccountID(accountURL)
	if err != nil {
		return nil, err
	}
	codes, err := fetchAccountBookCodesAPI(ctx, client, accountID, maxPages)
	if err == nil && len(codes) > 0 {
		return codes, nil
	}

	base, err := url.Parse(accountURL)
	if err != nil {
		return nil, err
	}
	base.RawQuery = ""
	base.Fragment = ""
	baseURL := base.String()

	codes, err = fetchAccountBookCodesHTML(ctx, client, baseURL, maxPages)
	if err != nil {
		return nil, err
	}
	if len(codes) == 0 {
		return nil, fmt.Errorf("no publications found on account page %s (page may be JS-rendered)", baseURL)
	}
	return codes, nil
}

type accountBooksResponse struct {
	Status  string `json:"status"`
	Content struct {
		Total int `json:"total"`
		Start int `json:"start"`
		Step  struct {
			Current int `json:"current"`
			Max     int `json:"max"`
		} `json:"step"`
		List []struct {
			Code string `json:"code"`
		} `json:"list"`
	} `json:"content"`
}

func fetchAccountBookCodesAPI(ctx context.Context, client *http.Client, accountID string, maxPages int) ([]string, error) {
	if maxPages <= 0 {
		maxPages = 1
	}
	step := 100
	start := 0
	var codes []string
	seen := make(map[string]struct{})

	for page := 1; page <= maxPages; page++ {
		endpoint := fmt.Sprintf("https://d.calameo.com/pinwheel/public/account/book/get?account=%s&start=%d&step=%d", url.QueryEscape(accountID), start, step)
		body, err := downloadWithRetry(ctx, client, endpoint, "https://www.calameo.com/accounts/"+accountID)
		if err != nil {
			return nil, fmt.Errorf("fetch account books (api): %w", err)
		}
		var resp accountBooksResponse
		if err := json.Unmarshal(body, &resp); err != nil {
			return nil, fmt.Errorf("decode account books (api): %w", err)
		}
		if resp.Status != "ok" {
			return nil, fmt.Errorf("account books (api) returned status %q", resp.Status)
		}
		if resp.Content.Step.Current > 0 {
			step = resp.Content.Step.Current
		}
		for _, item := range resp.Content.List {
			if item.Code == "" {
				continue
			}
			if _, ok := seen[item.Code]; ok {
				continue
			}
			seen[item.Code] = struct{}{}
			codes = append(codes, item.Code)
		}
		start += step
		if start >= resp.Content.Total || len(resp.Content.List) == 0 {
			break
		}
	}
	return codes, nil
}

func fetchAccountBookCodesHTML(ctx context.Context, client *http.Client, baseURL string, maxPages int) ([]string, error) {
	var codes []string
	seen := make(map[string]struct{})
	emptyPages := 0
	for page := 1; page <= maxPages; page++ {
		pageURL := baseURL
		if page > 1 {
			u, err := url.Parse(baseURL)
			if err != nil {
				return nil, err
			}
			q := u.Query()
			q.Set("page", strconv.Itoa(page))
			u.RawQuery = q.Encode()
			pageURL = u.String()
		}
		body, err := downloadWithRetry(ctx, client, pageURL, baseURL)
		if err != nil {
			return nil, fmt.Errorf("fetch account page %d: %w", page, err)
		}
		found := extractBookCodesFromHTML(string(body))
		newCount := 0
		for _, code := range found {
			if _, ok := seen[code]; ok {
				continue
			}
			seen[code] = struct{}{}
			codes = append(codes, code)
			newCount++
		}
		if newCount == 0 {
			emptyPages++
			if page > 1 || emptyPages >= 2 {
				break
			}
		}
	}
	return codes, nil
}

func extractAccountID(input string) (string, error) {
	u, err := url.Parse(strings.TrimSpace(input))
	if err != nil || u.Scheme == "" || u.Host == "" {
		return "", fmt.Errorf("invalid account url %q", input)
	}
	parts := strings.Split(strings.Trim(u.Path, "/"), "/")
	if len(parts) >= 2 && parts[0] == "accounts" && parts[1] != "" {
		return parts[1], nil
	}
	return "", fmt.Errorf("could not extract account id from %q", input)
}

func extractBookCodesFromHTML(html string) []string {
	re := regexp.MustCompile(`/((?:books|read))/([A-Za-z0-9]{20,24})`)
	matches := re.FindAllStringSubmatch(html, -1)
	seen := make(map[string]struct{})
	var codes []string
	for _, m := range matches {
		if len(m) < 3 {
			continue
		}
		code := m[2]
		if _, ok := seen[code]; ok {
			continue
		}
		seen[code] = struct{}{}
		codes = append(codes, code)
	}
	return codes
}

func processBook(ctx context.Context, client *http.Client, input string, totalInputs int, outputs outputOptions, outputPath, outDir, assetsOutDir, textBinOut, textBinJSON string, embedOCR bool, pdfSource, renderer, optimize string, jpegQ int, ocrFlipY bool, ocrFit string, ocrScaleX, ocrScaleY, ocrOffsetX, ocrOffsetY float64, ocrUseSVG bool, ocrSort, ocrPlace string, ocrDebugPage int, ocrDebugMarkers bool, overwrite bool, workers int) error {
	bookCode, err := extractBookCode(input)
	if err != nil {
		return err
	}

	meta, err := fetchBookMetadata(ctx, client, bookCode)
	if err != nil {
		return err
	}

	if meta.PageCount <= 0 {
		return fmt.Errorf("book %s returned no pages", bookCode)
	}

	localOutputs := outputs
	pdfSourceLocal := pdfSource
	if !meta.SVGEnabled {
		if localOutputs.SVG || localOutputs.SVGZ {
			fmt.Printf("SVG not enabled for %s (skipping SVG/SVGZ downloads)\n", meta.Name)
			localOutputs.SVG = false
			localOutputs.SVGZ = false
		}
		if localOutputs.PDF && strings.EqualFold(strings.TrimSpace(pdfSourceLocal), "svgz") {
			fmt.Printf("SVG not enabled for %s (falling back to JPG PDF)\n", meta.Name)
			pdfSourceLocal = "jpg"
		}
	}

	out := outputPath
	if out == "" {
		out = sanitizeFilename(meta.Name) + ".pdf"
		if outDir != "" {
			out = filepath.Join(outDir, out)
		}
	}
	if outDir != "" {
		if err := os.MkdirAll(outDir, 0o755); err != nil {
			return fmt.Errorf("create output dir: %w", err)
		}
	}
	if outputs.PDF && !overwrite {
		if _, err := os.Stat(out); err == nil {
			fmt.Printf("Skipping %s (already exists)\n", out)
			return nil
		} else if err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("check output %s: %w", out, err)
		}
	}

	rawDir := assetsOutDir
	if rawDir == "" && outputs.needsRawDir() {
		rawDir = sanitizeFilename(meta.Name) + "-assets"
	} else if rawDir != "" && outputs.needsRawDir() && totalInputs > 1 {
		rawDir = filepath.Join(rawDir, sanitizeFilename(meta.Name)+"-assets")
	}

	fmt.Printf("Book: %s\nPages: %d\nOutput: %s\n", meta.Name, meta.PageCount, out)

	var textOverlay *textBin
	if textBinOut != "" || textBinJSON != "" || embedOCR {
		data, err := fetchTextBinBytes(ctx, client, meta)
		if err != nil {
			if errors.Is(err, errTextBinUnavailable) && textBinOut == "" && textBinJSON == "" && embedOCR {
				fmt.Printf("OCR text.bin not available for %s (skipping OCR layer)\n", meta.Name)
				data = nil
			} else {
				return err
			}
		}

		if textBinOut != "" && data != nil {
			target := textBinOut
			if target == "auto" {
				target = sanitizeFilename(meta.Name) + ".text.bin"
				if outDir != "" {
					target = filepath.Join(outDir, target)
				}
			} else if target != "-" && totalInputs > 1 {
				target = sanitizeFilename(meta.Name) + "." + filepath.Base(target)
			}
			if err := writeTextBin(target, data); err != nil {
				return err
			}
		}

		if textBinJSON != "" && data != nil {
			target := textBinJSON
			if target == "auto" {
				target = sanitizeFilename(meta.Name) + ".text.json"
				if outDir != "" {
					target = filepath.Join(outDir, target)
				}
			} else if target != "-" && totalInputs > 1 {
				target = sanitizeFilename(meta.Name) + "." + filepath.Base(target)
			}
			if err := writeTextBinJSON(target, data); err != nil {
				return err
			}
		}

		if embedOCR && data != nil {
			tb, err := parseTextBin(data)
			if err != nil {
				return err
			}
			textOverlay = &tb
		}
	}

	if localOutputs.needsRawDir() {
		fmt.Printf("Raw assets: %s\n", rawDir)
		if err := os.MkdirAll(rawDir, 0o755); err != nil {
			return fmt.Errorf("create raw output dir: %w", err)
		}
	}

	var jpgPages []pageFile
	if localOutputs.JPG {
		jpgPages, err = downloadImagePages(ctx, client, meta, rawDir, workers, true)
		if err != nil {
			return err
		}
	}

	if localOutputs.SVG || localOutputs.SVGZ {
		if err := downloadSVGPages(ctx, client, meta, rawDir, workers, localOutputs.SVG, localOutputs.SVGZ); err != nil {
			return err
		}
	}

	if localOutputs.PDF {
		switch strings.ToLower(strings.TrimSpace(pdfSourceLocal)) {
		case "svgz":
			if err := buildPDFfromSVG(ctx, client, meta, out, rawDir, localOutputs.SVG, workers, renderer, optimize, jpegQ, textOverlay, ocrFlipY, ocrFit, ocrScaleX, ocrScaleY, ocrOffsetX, ocrOffsetY, ocrUseSVG, ocrSort, ocrPlace, ocrDebugPage, ocrDebugMarkers); err != nil {
				return err
			}
		case "jpg":
			pages := jpgPages
			if len(pages) == 0 {
				tmpDir, err := os.MkdirTemp("", "calameo-pages-*")
				if err != nil {
					return fmt.Errorf("create temp dir: %w", err)
				}
				defer os.RemoveAll(tmpDir)

				pages, err = downloadImagePages(ctx, client, meta, tmpDir, workers, true)
				if err != nil {
					return err
				}
			}

			if err := writePDF(out, meta, pages); err != nil {
				return err
			}
		default:
			return fmt.Errorf("unsupported -pdf-source %q (use svgz or jpg)", pdfSourceLocal)
		}
	}

	if localOutputs.PDF {
		fmt.Printf("Saved %s\n", out)
	}
	return nil
}

func fetchBookMetadata(ctx context.Context, client *http.Client, bookCode string) (bookMetadata, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, fmt.Sprintf(bookInfoEndpoint, bookCode), nil)
	if err != nil {
		return bookMetadata{}, err
	}

	req.Header.Set("Origin", "https://www.calameo.com")
	req.Header.Set("Referer", "https://www.calameo.com/")
	req.Header.Set("User-Agent", "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")

	resp, err := client.Do(req)
	if err != nil {
		return bookMetadata{}, fmt.Errorf("request book metadata: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return bookMetadata{}, fmt.Errorf("book metadata request failed: %s", resp.Status)
	}

	var data bookResponse
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		return bookMetadata{}, fmt.Errorf("decode book metadata: %w", err)
	}

	if data.Status != "ok" {
		return bookMetadata{}, fmt.Errorf("calameo returned status %q", data.Status)
	}

	expires := resp.Header.Get("X-Calameo-Hash-Expires")
	path := resp.Header.Get("X-Calameo-Hash-Path")
	signature := resp.Header.Get("X-Calameo-Hash-Signature")
	if expires == "" || path == "" || signature == "" {
		return bookMetadata{}, errors.New("missing Calameo signing headers")
	}

	imageBase := strings.TrimSpace(data.Content.Domains.Secured.Image)
	if imageBase == "" {
		imageBase = strings.TrimSpace(data.Content.Domains.Secured.SVG)
	}
	if imageBase == "" {
		return bookMetadata{}, errors.New("missing secured image domain in metadata")
	}

	return bookMetadata{
		ID:         data.Content.ID,
		Key:        data.Content.Key,
		Name:       data.Content.Name,
		PublicURL:  data.Content.URL.Public,
		PageCount:  data.Content.Document.Pages,
		PageWidth:  data.Content.Document.Width,
		PageHeight: data.Content.Document.Height,
		ImageBase:  strings.TrimRight(imageBase, "/"),
		Token:      buildToken(expires, path, signature),
		TextBinURL: normalizeURL(data.Content.Features.Search.Bin.URL),
		TextBase:   normalizeURL(data.Content.Domains.Secured.Text),
		TextPath:   strings.TrimSpace(data.Content.Features.Search.Bin.Path),
		SVGEnabled: data.Content.Features.SVG.Enabled,
	}, nil
}

func downloadImagePages(ctx context.Context, client *http.Client, meta bookMetadata, dir string, workers int, logLabel bool) ([]pageFile, error) {
	if workers <= 0 {
		workers = 1
	}

	type result struct {
		index int
		page  pageFile
		err   error
	}

	jobs := make(chan int)
	results := make(chan result, meta.PageCount)

	var wg sync.WaitGroup
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for pageNum := range jobs {
				page, err := downloadPage(ctx, client, meta, dir, pageNum)
				results <- result{index: pageNum - 1, page: page, err: err}
			}
		}()
	}

	go func() {
		for pageNum := 1; pageNum <= meta.PageCount; pageNum++ {
			jobs <- pageNum
		}
		close(jobs)
		wg.Wait()
		close(results)
	}()

	pages := make([]pageFile, meta.PageCount)
	for res := range results {
		if res.err != nil {
			return nil, res.err
		}
		pages[res.index] = res.page
		if logLabel {
			fmt.Printf("Downloaded JPG page %d/%d\n", res.index+1, meta.PageCount)
		}
	}

	return pages, nil
}

func downloadPage(ctx context.Context, client *http.Client, meta bookMetadata, dir string, pageNum int) (pageFile, error) {
	pageURL := buildSignedPageURL(meta.ImageBase, meta.Key, pageNum, meta.Token, "jpg")
	body, err := downloadWithRetry(ctx, client, pageURL, meta.PublicURL)
	if err != nil {
		return pageFile{}, fmt.Errorf("download page %d: %w", pageNum, err)
	}

	cfg, _, err := image.DecodeConfig(bytes.NewReader(body))
	if err != nil {
		return pageFile{}, fmt.Errorf("decode page %d image config: %w", pageNum, err)
	}

	filename := filepath.Join(dir, fmt.Sprintf("page-%04d.jpg", pageNum))
	if err := os.WriteFile(filename, body, 0o600); err != nil {
		return pageFile{}, fmt.Errorf("write page %d: %w", pageNum, err)
	}

	return pageFile{
		Path:   filename,
		Width:  cfg.Width,
		Height: cfg.Height,
	}, nil
}

func downloadSVGPages(ctx context.Context, client *http.Client, meta bookMetadata, dir string, workers int, saveSVG, saveSVGZ bool) error {
	if workers <= 0 {
		workers = 1
	}

	type result struct {
		index int
		err   error
	}

	jobs := make(chan int)
	results := make(chan result, meta.PageCount)

	var wg sync.WaitGroup
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for pageNum := range jobs {
				results <- result{index: pageNum - 1, err: downloadSVGPage(ctx, client, meta, dir, pageNum, saveSVG, saveSVGZ)}
			}
		}()
	}

	go func() {
		for pageNum := 1; pageNum <= meta.PageCount; pageNum++ {
			jobs <- pageNum
		}
		close(jobs)
		wg.Wait()
		close(results)
	}()

	for res := range results {
		if res.err != nil {
			return res.err
		}
		fmt.Printf("Downloaded SVG page %d/%d\n", res.index+1, meta.PageCount)
	}

	return nil
}

func downloadSVGPage(ctx context.Context, client *http.Client, meta bookMetadata, dir string, pageNum int, saveSVG, saveSVGZ bool) error {
	pageURL := buildSignedPageURL(meta.ImageBase, meta.Key, pageNum, meta.Token, "svgz")
	body, err := downloadWithRetry(ctx, client, pageURL, meta.PublicURL)
	if err != nil {
		return fmt.Errorf("download SVG page %d: %w", pageNum, err)
	}

	base := filepath.Join(dir, fmt.Sprintf("page-%04d", pageNum))
	if saveSVGZ {
		if err := os.WriteFile(base+".svgz", body, 0o644); err != nil {
			return fmt.Errorf("write SVGZ page %d: %w", pageNum, err)
		}
	}

	if saveSVG {
		svgData := body
		if len(body) >= 2 && body[0] == 0x1f && body[1] == 0x8b {
			gr, err := gzip.NewReader(bytes.NewReader(body))
			if err != nil {
				return fmt.Errorf("decompress SVG page %d: %w", pageNum, err)
			}
			defer gr.Close()

			svgData, err = io.ReadAll(gr)
			if err != nil {
				return fmt.Errorf("read decompressed SVG page %d: %w", pageNum, err)
			}
		}
		if err := os.WriteFile(base+".svg", svgData, 0o644); err != nil {
			return fmt.Errorf("write SVG page %d: %w", pageNum, err)
		}
	}

	return nil
}

func downloadWithRetry(ctx context.Context, client *http.Client, url, referer string) ([]byte, error) {
	const maxAttempts = 4
	var lastErr error

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Referer", referer)
		req.Header.Set("User-Agent", "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")

		resp, err := client.Do(req)
		if err != nil {
			lastErr = err
		} else {
			body, readErr := io.ReadAll(resp.Body)
			resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				lastErr = httpStatusError{Status: resp.Status, Code: resp.StatusCode}
			} else if readErr != nil {
				lastErr = readErr
			} else {
				return body, nil
			}
		}

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(time.Duration(attempt) * 350 * time.Millisecond):
		}
	}

	return nil, lastErr
}

type httpStatusError struct {
	Status string
	Code   int
}

func (e httpStatusError) Error() string {
	return "status " + e.Status
}

func fetchTextBinBytes(ctx context.Context, client *http.Client, meta bookMetadata) ([]byte, error) {
	var candidates []string
	if meta.TextBinURL != "" {
		candidates = append(candidates, meta.TextBinURL)
		if meta.Token != "" && !strings.Contains(meta.TextBinURL, "_token_") {
			candidates = append(candidates, meta.TextBinURL+meta.Token)
		}
	}
	if meta.TextBase != "" && meta.Key != "" {
		base := strings.TrimRight(meta.TextBase, "/")
		candidates = append(candidates, base+"/"+meta.Key+"/text.bin")
		if meta.Token != "" {
			candidates = append(candidates, base+"/"+meta.Key+"/text.bin"+meta.Token)
		}
	}
	if meta.TextBase != "" && meta.TextPath != "" {
		base := strings.TrimRight(meta.TextBase, "/")
		path := strings.TrimLeft(meta.TextPath, "/")
		candidates = append(candidates, base+"/"+path)
		if meta.Token != "" {
			candidates = append(candidates, base+"/"+path+meta.Token)
		}
	}
	if len(candidates) == 0 {
		return nil, errors.New("text.bin URL not present in metadata")
	}

	var body []byte
	var err error
	var lastStatus int
	for i, candidate := range candidates {
		if candidate == "" {
			continue
		}
		body, err = downloadWithRetry(ctx, client, candidate, meta.PublicURL)
		if err == nil {
			break
		}
		if statusErr, ok := err.(httpStatusError); ok {
			lastStatus = statusErr.Code
		}
		if statusErr, ok := err.(httpStatusError); ok && statusErr.Code == http.StatusForbidden && i < len(candidates)-1 {
			continue
		}
		if i < len(candidates)-1 {
			continue
		}
		if lastStatus == http.StatusForbidden || lastStatus == http.StatusNotFound {
			return nil, errTextBinUnavailable
		}
		return nil, fmt.Errorf("download text.bin: %w", err)
	}
	return body, nil
}

func writeTextBin(outputPath string, data []byte) error {
	if outputPath == "-" {
		_, err := os.Stdout.Write(data)
		return err
	}
	if err := os.WriteFile(outputPath, data, 0o644); err != nil {
		return fmt.Errorf("write text.bin: %w", err)
	}
	fmt.Printf("Saved %s\n", outputPath)
	return nil
}

type textBin struct {
	Pages []textPage `json:"pages"`
}

type textPage struct {
	Page   int        `json:"page"`
	Width  int        `json:"width,omitempty"`
	Height int        `json:"height,omitempty"`
	Words  []textWord `json:"words"`
}

type textWord struct {
	Text     string    `json:"text"`
	FontSize float32   `json:"font_size,omitempty"`
	Indices  []int     `json:"indices,omitempty"`
	Coords   []float32 `json:"coords,omitempty"`
}

func writeTextBinJSON(outputPath string, data []byte) error {
	tb, err := parseTextBin(data)
	if err != nil {
		return err
	}
	encoded, err := json.MarshalIndent(tb, "", "  ")
	if err != nil {
		return fmt.Errorf("encode text.bin json: %w", err)
	}
	if outputPath == "-" {
		_, err := os.Stdout.Write(encoded)
		return err
	}
	if err := os.WriteFile(outputPath, encoded, 0o644); err != nil {
		return fmt.Errorf("write text.bin json: %w", err)
	}
	fmt.Printf("Saved %s\n", outputPath)
	return nil
}

func parseTextBin(data []byte) (textBin, error) {
	var tb textBin
	for len(data) > 0 {
		num, typ, n := protowire.ConsumeTag(data)
		if n < 0 {
			return tb, fmt.Errorf("parse text.bin: %v", protowire.ParseError(n))
		}
		data = data[n:]
		switch typ {
		case protowire.BytesType:
			var b []byte
			b, n = protowire.ConsumeBytes(data)
			if n < 0 {
				return tb, fmt.Errorf("parse text.bin: %v", protowire.ParseError(n))
			}
			data = data[n:]
			if num == 4 {
				page, err := parseTextPage(b)
				if err != nil {
					return tb, err
				}
				tb.Pages = append(tb.Pages, page)
			}
		default:
			n = protowire.ConsumeFieldValue(num, typ, data)
			if n < 0 {
				return tb, fmt.Errorf("parse text.bin: %v", protowire.ParseError(n))
			}
			data = data[n:]
		}
	}
	return tb, nil
}

func parseTextPage(data []byte) (textPage, error) {
	var page textPage
	for len(data) > 0 {
		num, typ, n := protowire.ConsumeTag(data)
		if n < 0 {
			return page, fmt.Errorf("parse page: %v", protowire.ParseError(n))
		}
		data = data[n:]
		switch typ {
		case protowire.VarintType:
			v, m := protowire.ConsumeVarint(data)
			if m < 0 {
				return page, fmt.Errorf("parse page: %v", protowire.ParseError(m))
			}
			data = data[m:]
			switch num {
			case 1:
				page.Page = int(v)
			case 2:
				page.Width = int(v)
			case 3:
				page.Height = int(v)
			}
		case protowire.BytesType:
			b, m := protowire.ConsumeBytes(data)
			if m < 0 {
				return page, fmt.Errorf("parse page: %v", protowire.ParseError(m))
			}
			data = data[m:]
			if num == 4 {
				word, err := parseTextWord(b)
				if err != nil {
					return page, err
				}
				page.Words = append(page.Words, word)
			}
		default:
			m := protowire.ConsumeFieldValue(num, typ, data)
			if m < 0 {
				return page, fmt.Errorf("parse page: %v", protowire.ParseError(m))
			}
			data = data[m:]
		}
	}
	return page, nil
}

func parseTextWord(data []byte) (textWord, error) {
	var word textWord
	for len(data) > 0 {
		num, typ, n := protowire.ConsumeTag(data)
		if n < 0 {
			return word, fmt.Errorf("parse word: %v", protowire.ParseError(n))
		}
		data = data[n:]
		switch typ {
		case protowire.BytesType:
			b, m := protowire.ConsumeBytes(data)
			if m < 0 {
				return word, fmt.Errorf("parse word: %v", protowire.ParseError(m))
			}
			data = data[m:]
			switch num {
			case 1:
				word.Text = string(b)
			case 3:
				word.Indices = decodePackedVarints(b)
			case 4:
				word.Coords = decodePackedFloat32(b)
			}
		case protowire.Fixed32Type:
			v, m := protowire.ConsumeFixed32(data)
			if m < 0 {
				return word, fmt.Errorf("parse word: %v", protowire.ParseError(m))
			}
			data = data[m:]
			if num == 2 {
				word.FontSize = math.Float32frombits(uint32(v))
			}
		default:
			m := protowire.ConsumeFieldValue(num, typ, data)
			if m < 0 {
				return word, fmt.Errorf("parse word: %v", protowire.ParseError(m))
			}
			data = data[m:]
		}
	}
	return word, nil
}

func decodePackedVarints(b []byte) []int {
	var out []int
	for len(b) > 0 {
		v, n := protowire.ConsumeVarint(b)
		if n < 0 {
			break
		}
		out = append(out, int(v))
		b = b[n:]
	}
	return out
}

func decodePackedFloat32(b []byte) []float32 {
	if len(b)%4 != 0 {
		return nil
	}
	out := make([]float32, 0, len(b)/4)
	for i := 0; i < len(b); i += 4 {
		u := uint32(b[i]) | uint32(b[i+1])<<8 | uint32(b[i+2])<<16 | uint32(b[i+3])<<24
		out = append(out, math.Float32frombits(u))
	}
	return out
}

func detectSVGSize(path string) (float64, float64, bool) {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0, 0, false
	}
	s := string(data)
	width, height := parseSVGSizeAttr(s)
	if width > 0 && height > 0 {
		return width, height, true
	}
	if vw, vh := parseSVGViewBox(s); vw > 0 && vh > 0 {
		return vw, vh, true
	}
	return 0, 0, false
}

func detectPDFMediaBox(path string) (float64, float64, bool) {
	pdfinfoPath, err := exec.LookPath("pdfinfo")
	if err != nil {
		return 0, 0, false
	}
	cmd := exec.Command(pdfinfoPath, "-box", path)
	out, err := cmd.Output()
	if err != nil {
		return 0, 0, false
	}
	re := regexp.MustCompile(`MediaBox:\s*([0-9.]+)\s+([0-9.]+)\s+([0-9.]+)\s+([0-9.]+)`)
	m := re.FindSubmatch(out)
	if len(m) < 5 {
		return 0, 0, false
	}
	x1, _ := strconv.ParseFloat(string(m[1]), 64)
	y1, _ := strconv.ParseFloat(string(m[2]), 64)
	x2, _ := strconv.ParseFloat(string(m[3]), 64)
	y2, _ := strconv.ParseFloat(string(m[4]), 64)
	w := x2 - x1
	h := y2 - y1
	if w <= 0 || h <= 0 {
		return 0, 0, false
	}
	return w, h, true
}

func parseSVGSizeAttr(s string) (float64, float64) {
	reW := regexp.MustCompile(`\bwidth=["']\s*([0-9.]+)`)
	reH := regexp.MustCompile(`\bheight=["']\s*([0-9.]+)`)
	wm := reW.FindStringSubmatch(s)
	hm := reH.FindStringSubmatch(s)
	if len(wm) < 2 || len(hm) < 2 {
		return 0, 0
	}
	w, _ := strconv.ParseFloat(wm[1], 64)
	h, _ := strconv.ParseFloat(hm[1], 64)
	return w, h
}

func parseSVGViewBox(s string) (float64, float64) {
	re := regexp.MustCompile(`\bviewBox=["']\s*([0-9.+-]+)\s+([0-9.+-]+)\s+([0-9.+-]+)\s+([0-9.+-]+)`)
	m := re.FindStringSubmatch(s)
	if len(m) < 5 {
		return 0, 0
	}
	w, _ := strconv.ParseFloat(m[3], 64)
	h, _ := strconv.ParseFloat(m[4], 64)
	return w, h
}

func writeTextOverlayPDF(outputPath string, meta bookMetadata, tb textBin, flipY bool, fitMode string, scaleX, scaleY, offsetX, offsetY float64, targetW, targetH float64, sortMode string, placement string, debugPage int, debugMarkers bool, useSVGSize bool, svgDir string) error {
	if meta.PageWidth <= 0 || meta.PageHeight <= 0 {
		return errors.New("missing page dimensions for OCR overlay")
	}
	if len(tb.Pages) == 0 {
		return errors.New("text.bin contained no pages")
	}

	pages := make([]textPage, meta.PageCount)
	hasExplicit := false
	for _, p := range tb.Pages {
		if p.Page > 0 {
			hasExplicit = true
			break
		}
	}
	if hasExplicit {
		for _, p := range tb.Pages {
			if p.Page <= 0 || p.Page > meta.PageCount {
				continue
			}
			pages[p.Page-1] = p
		}
	} else {
		for i := 0; i < meta.PageCount && i < len(tb.Pages); i++ {
			pages[i] = tb.Pages[i]
		}
	}

	type pdfObject struct {
		data []byte
	}
	objects := []pdfObject{{}}
	addObject := func(data []byte) int {
		objects = append(objects, pdfObject{data: data})
		return len(objects) - 1
	}

	infoID := addObject([]byte("<< /Producer (calameo-ripper-go) /Title " + pdfString(meta.Name) + " >>"))
	fontID := addObject([]byte("<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>"))

	contentIDs := make([]int, meta.PageCount)
	pageIDs := make([]int, meta.PageCount)
	pageWidths := make([]float64, meta.PageCount)
	pageHeights := make([]float64, meta.PageCount)

	for i := 0; i < meta.PageCount; i++ {
		w := targetW
		h := targetH
		if useSVGSize && svgDir != "" {
			svgPath := filepath.Join(svgDir, fmt.Sprintf("page-%04d.svg", i+1))
			if sw, sh, ok := detectSVGSize(svgPath); ok {
				w = sw
				h = sh
			}
		}
		if w <= 0 || h <= 0 {
			w = float64(meta.PageWidth)
			h = float64(meta.PageHeight)
		}
		pageWidths[i] = w
		pageHeights[i] = h
		pages[i].Words = sortWords(pages[i].Words, sortMode, w, h, scaleX, scaleY, offsetX, offsetY, flipY)
		content := buildTextContent(pages[i], w, h, flipY, fitMode, scaleX, scaleY, offsetX, offsetY, placement, debugPage, debugMarkers, i+1)
		contentObj := fmt.Sprintf("<< /Length %d >>\nstream\n%sendstream", len(content), content)
		contentIDs[i] = addObject([]byte(contentObj))
	}

	pagesNodeID := len(objects) + meta.PageCount
	for i := 0; i < meta.PageCount; i++ {
		w := pageWidths[i]
		h := pageHeights[i]
		if w <= 0 || h <= 0 {
			w = float64(meta.PageWidth)
			h = float64(meta.PageHeight)
		}
		pageObj := fmt.Sprintf("<< /Type /Page /Parent %d 0 R /MediaBox [0 0 %d %d] /Resources << /Font << /F1 %d 0 R >> >> /Contents %d 0 R >>",
			pagesNodeID, int(w), int(h), fontID, contentIDs[i])
		pageIDs[i] = addObject([]byte(pageObj))
	}

	var kids strings.Builder
	for _, id := range pageIDs {
		kids.WriteString(strconv.Itoa(id))
		kids.WriteString(" 0 R ")
	}
	pagesObj := fmt.Sprintf("<< /Type /Pages /Count %d /Kids [%s] >>", len(pageIDs), strings.TrimSpace(kids.String()))
	actualPagesID := addObject([]byte(pagesObj))
	if actualPagesID != pagesNodeID {
		return errors.New("internal PDF object numbering mismatch (overlay)")
	}
	catalogID := addObject([]byte(fmt.Sprintf("<< /Type /Catalog /Pages %d 0 R >>", actualPagesID)))

	var buf bytes.Buffer
	buf.WriteString("%PDF-1.4\n%\xFF\xFF\xFF\xFF\n")

	offsets := make([]int, len(objects))
	for id := 1; id < len(objects); id++ {
		offsets[id] = buf.Len()
		fmt.Fprintf(&buf, "%d 0 obj\n", id)
		buf.Write(objects[id].data)
		buf.WriteString("\nendobj\n")
	}

	xrefOffset := buf.Len()
	fmt.Fprintf(&buf, "xref\n0 %d\n", len(objects))
	buf.WriteString("0000000000 65535 f \n")
	for id := 1; id < len(objects); id++ {
		fmt.Fprintf(&buf, "%010d 00000 n \n", offsets[id])
	}

	fmt.Fprintf(&buf, "trailer\n<< /Size %d /Root %d 0 R /Info %d 0 R >>\nstartxref\n%d\n%%%%EOF\n",
		len(objects), catalogID, infoID, xrefOffset)

	if err := os.WriteFile(outputPath, buf.Bytes(), 0o644); err != nil {
		return fmt.Errorf("write text overlay pdf: %w", err)
	}

	return nil
}

func buildTextContent(page textPage, targetWidth, targetHeight float64, flipY bool, fitMode string, userScaleX, userScaleY, userOffsetX, userOffsetY float64, placement string, debugPage int, debugMarkers bool, pageIndex int) string {
	if len(page.Words) == 0 {
		return ""
	}
	scaleX := 1.0
	scaleY := 1.0
	offsetX := 0.0
	offsetY := 0.0
	mode := strings.ToLower(strings.TrimSpace(fitMode))
	if mode == "bbox" {
		minX, maxX, minY, maxY, ok := textBounds(page)
		if ok {
			if maxX > minX {
				scaleX = targetWidth / (maxX - minX)
				offsetX = -minX
			}
			if maxY > minY {
				scaleY = targetHeight / (maxY - minY)
				offsetY = -minY
			}
		}
	} else if page.Width > 0 && page.Height > 0 && targetWidth > 0 && targetHeight > 0 {
		scaleX = targetWidth / float64(page.Width)
		scaleY = targetHeight / float64(page.Height)
	}
	if userScaleX > 0 {
		scaleX *= userScaleX
	}
	if userScaleY > 0 {
		scaleY *= userScaleY
	}
	offsetX += userOffsetX
	offsetY += userOffsetY
	var b strings.Builder
	var dbg strings.Builder
	b.WriteString("BT\n3 Tr\n")
	for _, w := range page.Words {
		if w.Text == "" || len(w.Coords) < 6 {
			continue
		}
		fontSize := float64(w.FontSize) * scaleY
		if fontSize <= 0 {
			fontSize = 12
		}
		place := strings.ToLower(strings.TrimSpace(placement))
		ax := 1.0
		by := 0.0
		cx := 0.0
		dy := 1.0
		ex, fy := wordAnchor(w)
		ex = (ex + offsetX) * scaleX
		fy = (fy + offsetY) * scaleY
		if place == "matrix" {
			if len(w.Coords) >= 6 {
				ax = float64(w.Coords[0]) * scaleX
				by = float64(w.Coords[1]) * scaleY
				cx = float64(w.Coords[2]) * scaleX
				dy = float64(w.Coords[3]) * scaleY
				ex = (float64(w.Coords[4]) + offsetX) * scaleX
				fy = (float64(w.Coords[5]) + offsetY) * scaleY
			}
		}
		if flipY {
			by = -by
			dy = -dy
			fy = targetHeight - fy
		}
		actual := pdfUTF16Hex(w.Text)
		b.WriteString("/Span << /ActualText " + actual + " >> BDC\n")
		fmt.Fprintf(&b, "/F1 %.2f Tf\n%.4f %.4f %.4f %.4f %.4f %.4f Tm\n%s Tj\nEMC\n",
			fontSize, ax, by, cx, dy, ex, fy, pdfTextString(w.Text))
		if debugMarkers && (debugPage == 0 || debugPage == pageIndex) {
			dbg.WriteString("q\n0 0 1 rg\n")
			fmt.Fprintf(&dbg, "1 w\n%.4f %.4f m %.4f %.4f l S\n", ex-2, fy, ex+2, fy)
			fmt.Fprintf(&dbg, "%.4f %.4f m %.4f %.4f l S\n", ex, fy-2, ex, fy+2)
			dbg.WriteString("Q\n")
		}
	}
	b.WriteString("ET\n")
	if dbg.Len() > 0 {
		b.WriteString(dbg.String())
	}
	return b.String()
}

func textBounds(page textPage) (minX, maxX, minY, maxY float64, ok bool) {
	minX = math.MaxFloat64
	minY = math.MaxFloat64
	maxX = -math.MaxFloat64
	maxY = -math.MaxFloat64
	for _, w := range page.Words {
		if len(w.Coords) < 2 || len(w.Coords)%2 != 0 {
			continue
		}
		if len(w.Coords) == 6 {
			x := float64(w.Coords[4])
			y := float64(w.Coords[5])
			if x < minX {
				minX = x
			}
			if x > maxX {
				maxX = x
			}
			if y < minY {
				minY = y
			}
			if y > maxY {
				maxY = y
			}
			continue
		}
		for i := 0; i+1 < len(w.Coords); i += 2 {
			x := float64(w.Coords[i])
			y := float64(w.Coords[i+1])
			if x < minX {
				minX = x
			}
			if x > maxX {
				maxX = x
			}
			if y < minY {
				minY = y
			}
			if y > maxY {
				maxY = y
			}
		}
	}
	if minX == math.MaxFloat64 || minY == math.MaxFloat64 {
		return 0, 0, 0, 0, false
	}
	return minX, maxX, minY, maxY, true
}

func sortWords(words []textWord, mode string, targetW, targetH, scaleX, scaleY, offsetX, offsetY float64, flipY bool) []textWord {
	if len(words) == 0 {
		return words
	}
	mode = strings.ToLower(strings.TrimSpace(mode))
	out := make([]textWord, len(words))
	copy(out, words)
	if mode == "pdf" || mode == "line" {
		return sortWordsByLine(out, targetW, targetH, scaleX, scaleY, offsetX, offsetY, flipY)
	}
	sort.Slice(out, func(i, j int) bool {
		wi := out[i]
		wj := out[j]
		yi := wordY(wi)
		yj := wordY(wj)
		if math.Abs(yi-yj) > 0.5 {
			if flipY {
				return yi > yj
			}
			return yi < yj
		}
		return wordX(wi) < wordX(wj)
	})
	return out
}

func wordX(w textWord) float64 {
	x, _ := wordAnchor(w)
	return x
}

func wordY(w textWord) float64 {
	_, y := wordAnchor(w)
	return y
}

func wordAnchor(w textWord) (float64, float64) {
	if len(w.Coords) < 2 || len(w.Coords)%2 != 0 {
		return 0, 0
	}
	if len(w.Coords) == 6 {
		return float64(w.Coords[4]), float64(w.Coords[5])
	}
	minX := math.MaxFloat64
	maxY := -math.MaxFloat64
	for i := 0; i+1 < len(w.Coords); i += 2 {
		x := float64(w.Coords[i])
		y := float64(w.Coords[i+1])
		if x < minX {
			minX = x
		}
		if y > maxY {
			maxY = y
		}
	}
	if minX == math.MaxFloat64 || maxY == -math.MaxFloat64 {
		return 0, 0
	}
	return minX, maxY
}

func sortWordsByLine(words []textWord, targetW, targetH, scaleX, scaleY, offsetX, offsetY float64, flipY bool) []textWord {
	type line struct {
		y     float64
		words []textWord
	}
	const tol = 6.0
	var lines []line
	for _, w := range words {
		_, y := wordAnchor(w)
		y = (y + offsetY) * scaleY
		if flipY {
			y = targetH - y
		}
		placed := false
		for i := range lines {
			if math.Abs(lines[i].y-y) <= tol {
				lines[i].words = append(lines[i].words, w)
				placed = true
				break
			}
		}
		if !placed {
			lines = append(lines, line{y: y, words: []textWord{w}})
		}
	}
	sort.Slice(lines, func(i, j int) bool {
		if flipY {
			return lines[i].y > lines[j].y
		}
		return lines[i].y < lines[j].y
	})
	var out []textWord
	for _, ln := range lines {
		sort.Slice(ln.words, func(i, j int) bool {
			xi, _ := wordAnchor(ln.words[i])
			xj, _ := wordAnchor(ln.words[j])
			return xi < xj
		})
		out = append(out, ln.words...)
	}
	return out
}

func pdfTextString(s string) string {
	enc, err := charmap.Windows1252.NewEncoder().Bytes([]byte(s))
	if err != nil {
		enc = []byte(s)
	}
	return "<" + hex.EncodeToString(enc) + ">"
}

func pdfUTF16Hex(s string) string {
	enc := utf16.Encode([]rune(s))
	out := make([]byte, 0, 2+len(enc)*2)
	out = append(out, 0xFE, 0xFF)
	for _, r := range enc {
		out = append(out, byte(r>>8), byte(r))
	}
	return "<" + hex.EncodeToString(out) + ">"
}

type svgConverter struct {
	kind string
	path string
}

func buildPDFfromSVG(ctx context.Context, client *http.Client, meta bookMetadata, outputPath, rawDir string, hasSVG bool, workers int, renderer string, optimize string, jpegQuality int, overlay *textBin, flipY bool, fitMode string, scaleX, scaleY, offsetX, offsetY float64, useSVGSize bool, sortMode string, placement string, debugPage int, debugMarkers bool) error {
	converter, err := findSVGConverter(renderer)
	if err != nil {
		return err
	}
	pdfunitePath, err := exec.LookPath("pdfunite")
	if err != nil {
		return errors.New("pdfunite not found in PATH (needed to merge per-page PDFs)")
	}

	useDir := rawDir
	needsCleanup := false
	if !hasSVG || rawDir == "" {
		tmpDir, err := os.MkdirTemp("", "calameo-svg-*")
		if err != nil {
			return fmt.Errorf("create temp dir: %w", err)
		}
		useDir = tmpDir
		needsCleanup = true
		if err := downloadSVGPages(ctx, client, meta, useDir, workers, true, false); err != nil {
			return err
		}
	}
	if needsCleanup {
		defer os.RemoveAll(useDir)
	}

	pdfDir, err := os.MkdirTemp("", "calameo-pdf-pages-*")
	if err != nil {
		return fmt.Errorf("create temp pdf dir: %w", err)
	}
	defer os.RemoveAll(pdfDir)

	type result struct {
		index int
		err   error
	}
	jobs := make(chan int)
	results := make(chan result, meta.PageCount)

	if workers <= 0 {
		workers = 1
	}

	var wg sync.WaitGroup
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for pageNum := range jobs {
				svgPath := filepath.Join(useDir, fmt.Sprintf("page-%04d.svg", pageNum))
				pdfPath := filepath.Join(pdfDir, fmt.Sprintf("page-%04d.pdf", pageNum))
				err := converter.convert(ctx, svgPath, pdfPath)
				results <- result{index: pageNum - 1, err: err}
			}
		}()
	}

	go func() {
		for pageNum := 1; pageNum <= meta.PageCount; pageNum++ {
			jobs <- pageNum
		}
		close(jobs)
		wg.Wait()
		close(results)
	}()

	for res := range results {
		if res.err != nil {
			return res.err
		}
		fmt.Printf("Rendered SVG page %d/%d\n", res.index+1, meta.PageCount)
	}

	pdfInputs := make([]string, 0, meta.PageCount)
	for pageNum := 1; pageNum <= meta.PageCount; pageNum++ {
		pdfInputs = append(pdfInputs, filepath.Join(pdfDir, fmt.Sprintf("page-%04d.pdf", pageNum)))
	}

	if err := runPdfUnite(ctx, pdfunitePath, pdfInputs, outputPath); err != nil {
		return err
	}

	if overlay != nil {
		tmpDir, err := os.MkdirTemp("", "calameo-text-overlay-*")
		if err != nil {
			return fmt.Errorf("create text overlay dir: %w", err)
		}
		defer os.RemoveAll(tmpDir)

		targetW := float64(meta.PageWidth)
		targetH := float64(meta.PageHeight)
		if !useSVGSize {
			if w, h, ok := detectPDFMediaBox(outputPath); ok {
				targetW = w
				targetH = h
			}
		}

		overlayPath := filepath.Join(tmpDir, "text-overlay.pdf")
		if err := writeTextOverlayPDF(overlayPath, meta, *overlay, flipY, fitMode, scaleX, scaleY, offsetX, offsetY, targetW, targetH, sortMode, placement, debugPage, debugMarkers, useSVGSize, useDir); err != nil {
			return err
		}
		if err := overlayPDF(ctx, outputPath, overlayPath); err != nil {
			return err
		}
	}

	switch strings.ToLower(strings.TrimSpace(optimize)) {
	case "", "lossless":
		if err := optimizePDF(ctx, outputPath); err != nil {
			return err
		}
	case "lossy":
		if err := optimizePDFLossy(ctx, outputPath, jpegQuality); err != nil {
			return err
		}
	case "off", "false", "no":
	default:
		return fmt.Errorf("unsupported -optimize-pdf value %q (use off, lossless, lossy)", optimize)
	}

	return nil
}

func overlayPDF(ctx context.Context, basePath, overlayPath string) error {
	qpdfPath, err := exec.LookPath("qpdf")
	if err != nil {
		return errors.New("qpdf not found in PATH (needed for text overlay)")
	}
	tmp := basePath + ".overlay.pdf"
	args := []string{
		basePath,
		"--overlay", overlayPath,
		"--",
		tmp,
	}
	cmd := exec.CommandContext(ctx, qpdfPath, args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		if repaired, repErr := overlayPDFWithGS(ctx, basePath, overlayPath, tmp); repErr == nil {
			if err := os.Rename(repaired, basePath); err != nil {
				return fmt.Errorf("replace overlaid pdf: %w", err)
			}
			return nil
		}
		return fmt.Errorf("qpdf overlay failed: %w", err)
	}
	if err := os.Rename(tmp, basePath); err != nil {
		return fmt.Errorf("replace overlaid pdf: %w", err)
	}
	return nil
}

func overlayPDFWithGS(ctx context.Context, basePath, overlayPath, outputPath string) (string, error) {
	gsPath, err := exec.LookPath("gs")
	if err != nil {
		return "", errors.New("gs not found in PATH (needed for overlay repair fallback)")
	}

	tmpDir, err := os.MkdirTemp("", "calameo-gs-overlay-*")
	if err != nil {
		return "", fmt.Errorf("create gs overlay dir: %w", err)
	}
	defer os.RemoveAll(tmpDir)

	repairedBase := filepath.Join(tmpDir, "base.pdf")
	repairedOverlay := filepath.Join(tmpDir, "overlay.pdf")
	if err := rewritePDFLossless(ctx, gsPath, basePath, repairedBase); err != nil {
		return "", fmt.Errorf("gs rewrite base pdf: %w", err)
	}
	if err := rewritePDFLossless(ctx, gsPath, overlayPath, repairedOverlay); err != nil {
		return "", fmt.Errorf("gs rewrite overlay pdf: %w", err)
	}

	qpdfPath, err := exec.LookPath("qpdf")
	if err != nil {
		return "", errors.New("qpdf not found in PATH (needed for text overlay)")
	}
	args := []string{
		repairedBase,
		"--overlay", repairedOverlay,
		"--",
		outputPath,
	}
	cmd := exec.CommandContext(ctx, qpdfPath, args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("qpdf overlay after gs rewrite failed: %w", err)
	}
	return outputPath, nil
}

func rewritePDFLossless(ctx context.Context, gsPath, input, output string) error {
	args := []string{
		"-sDEVICE=pdfwrite",
		"-dCompatibilityLevel=1.7",
		"-dNOPAUSE",
		"-dBATCH",
		"-dSAFER",
		"-dDetectDuplicateImages=true",
		"-dCompressFonts=true",
		"-dSubsetFonts=true",
		"-dDownsampleColorImages=false",
		"-dDownsampleGrayImages=false",
		"-dDownsampleMonoImages=false",
		"-dAutoFilterColorImages=false",
		"-dAutoFilterGrayImages=false",
		"-dAutoFilterMonoImages=false",
		"-dColorImageFilter=/FlateEncode",
		"-dGrayImageFilter=/FlateEncode",
		"-dMonoImageFilter=/CCITTFaxEncode",
		"-sOutputFile=" + output,
		input,
	}
	cmd := exec.CommandContext(ctx, gsPath, args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return err
	}
	return nil
}

func runPdfUnite(ctx context.Context, pdfunitePath string, inputs []string, outputPath string) error {
	args := append(append([]string{}, inputs...), outputPath)
	cmd := exec.CommandContext(ctx, pdfunitePath, args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err == nil {
		return nil
	} else {
		qpdfPath, qerr := exec.LookPath("qpdf")
		if qerr != nil {
			return fmt.Errorf("pdfunite failed: %w", err)
		}

		repairedDir, rerr := os.MkdirTemp("", "calameo-pdf-repair-*")
		if rerr != nil {
			return fmt.Errorf("pdfunite failed and repair dir unavailable: %w", err)
		}
		defer os.RemoveAll(repairedDir)

		repaired := make([]string, 0, len(inputs))
		for _, input := range inputs {
			out := filepath.Join(repairedDir, filepath.Base(input))
			rcmd := exec.CommandContext(ctx, qpdfPath, "--repair", input, out)
			rcmd.Stdout = os.Stdout
			rcmd.Stderr = os.Stderr
			if rerr := rcmd.Run(); rerr != nil {
				return fmt.Errorf("pdfunite failed and qpdf repair failed on %s: %w", filepath.Base(input), rerr)
			}
			repaired = append(repaired, out)
		}

		args = append(append([]string{}, repaired...), outputPath)
		cmd = exec.CommandContext(ctx, pdfunitePath, args...)
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		if err := cmd.Run(); err != nil {
			return fmt.Errorf("pdfunite failed after repair: %w", err)
		}
		return nil
	}
}

func findSVGConverter(preference string) (svgConverter, error) {
	switch strings.ToLower(strings.TrimSpace(preference)) {
	case "", "auto":
		if path, err := exec.LookPath("rsvg-convert"); err == nil {
			return svgConverter{kind: "rsvg-convert", path: path}, nil
		}
		if path, err := exec.LookPath("resvg"); err == nil {
			return svgConverter{kind: "resvg", path: path}, nil
		}
	case "resvg":
		if path, err := exec.LookPath("resvg"); err == nil {
			return svgConverter{kind: "resvg", path: path}, nil
		}
		return svgConverter{}, errors.New("resvg not found in PATH")
	case "rsvg":
		if path, err := exec.LookPath("rsvg-convert"); err == nil {
			return svgConverter{kind: "rsvg-convert", path: path}, nil
		}
		return svgConverter{}, errors.New("rsvg-convert not found in PATH")
	default:
		return svgConverter{}, fmt.Errorf("unsupported svg renderer %q (use auto,resvg,rsvg)", preference)
	}
	return svgConverter{}, errors.New("neither rsvg-convert nor resvg found in PATH (needed for svgz -> pdf)")
}

func (c svgConverter) convert(ctx context.Context, inputPath, outputPath string) error {
	var cmd *exec.Cmd
	switch c.kind {
	case "rsvg-convert":
		cmd = exec.CommandContext(ctx, c.path, "-f", "pdf", "-o", outputPath, inputPath)
	case "resvg":
		cmd = exec.CommandContext(ctx, c.path, inputPath, outputPath)
	default:
		return fmt.Errorf("unsupported svg converter %q", c.kind)
	}
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("%s failed for %s: %w", c.kind, filepath.Base(inputPath), err)
	}
	return nil
}

func optimizePDF(ctx context.Context, path string) error {
	gsPath, err := exec.LookPath("gs")
	if err != nil {
		return errors.New("gs not found in PATH (needed for PDF optimization)")
	}
	tmp := path + ".opt.pdf"
	args := []string{
		"-sDEVICE=pdfwrite",
		"-dCompatibilityLevel=1.7",
		"-dNOPAUSE",
		"-dBATCH",
		"-dSAFER",
		"-dDetectDuplicateImages=true",
		"-dCompressFonts=true",
		"-dSubsetFonts=true",
		"-dDownsampleColorImages=false",
		"-dDownsampleGrayImages=false",
		"-dDownsampleMonoImages=false",
		"-dAutoFilterColorImages=false",
		"-dAutoFilterGrayImages=false",
		"-dAutoFilterMonoImages=false",
		"-dColorImageFilter=/FlateEncode",
		"-dGrayImageFilter=/FlateEncode",
		"-dMonoImageFilter=/CCITTFaxEncode",
		"-sOutputFile=" + tmp,
		path,
	}
	cmd := exec.CommandContext(ctx, gsPath, args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("gs optimization failed: %w", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		return fmt.Errorf("replace optimized pdf: %w", err)
	}
	return nil
}

func optimizePDFLossy(ctx context.Context, path string, jpegQuality int) error {
	gsPath, err := exec.LookPath("gs")
	if err != nil {
		return errors.New("gs not found in PATH (needed for PDF optimization)")
	}
	if jpegQuality < 1 || jpegQuality > 100 {
		return fmt.Errorf("jpeg-quality must be 1-100, got %d", jpegQuality)
	}
	tmp := path + ".opt.pdf"
	args := []string{
		"-sDEVICE=pdfwrite",
		"-dCompatibilityLevel=1.7",
		"-dNOPAUSE",
		"-dBATCH",
		"-dSAFER",
		"-dDetectDuplicateImages=true",
		"-dCompressFonts=true",
		"-dSubsetFonts=true",
		"-dPDFSETTINGS=/ebook",
		"-dDownsampleColorImages=true",
		"-dDownsampleGrayImages=true",
		"-dDownsampleMonoImages=true",
		"-dColorImageDownsampleType=/Bicubic",
		"-dGrayImageDownsampleType=/Bicubic",
		"-dMonoImageDownsampleType=/Subsample",
		"-dColorImageResolution=200",
		"-dGrayImageResolution=200",
		"-dMonoImageResolution=300",
		"-dAutoFilterColorImages=false",
		"-dAutoFilterGrayImages=false",
		"-dAutoFilterMonoImages=false",
		"-dColorImageFilter=/DCTEncode",
		"-dGrayImageFilter=/DCTEncode",
		"-dMonoImageFilter=/CCITTFaxEncode",
		"-dJPEGQ=" + strconv.Itoa(jpegQuality),
		"-sOutputFile=" + tmp,
		path,
	}
	cmd := exec.CommandContext(ctx, gsPath, args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("gs lossy optimization failed: %w", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		return fmt.Errorf("replace optimized pdf: %w", err)
	}
	return nil
}

func writePDF(outputPath string, meta bookMetadata, pages []pageFile) error {
	type pdfObject struct {
		data []byte
	}

	objects := []pdfObject{{}}
	addObject := func(data []byte) int {
		objects = append(objects, pdfObject{data: data})
		return len(objects) - 1
	}

	infoID := addObject([]byte("<< /Producer (calameo-ripper-go) /Title " + pdfString(meta.Name) + " >>"))

	imageIDs := make([]int, len(pages))
	contentIDs := make([]int, len(pages))
	pageIDs := make([]int, len(pages))

	for i, page := range pages {
		data, err := os.ReadFile(page.Path)
		if err != nil {
			return fmt.Errorf("read %s: %w", page.Path, err)
		}

		imageObj := fmt.Sprintf("<< /Type /XObject /Subtype /Image /Width %d /Height %d /ColorSpace /DeviceRGB /BitsPerComponent 8 /Filter /DCTDecode /Length %d >>\nstream\n",
			page.Width, page.Height, len(data))
		imageIDs[i] = addObject(append(append([]byte(imageObj), data...), []byte("\nendstream")...))

		pageWidth := page.Width
		pageHeight := page.Height
		if meta.PageWidth > 0 && meta.PageHeight > 0 {
			pageWidth = meta.PageWidth
			pageHeight = meta.PageHeight
		}

		scale := minFloat(float64(pageWidth)/float64(page.Width), float64(pageHeight)/float64(page.Height))
		drawWidth := float64(page.Width) * scale
		drawHeight := float64(page.Height) * scale
		offsetX := (float64(pageWidth) - drawWidth) / 2
		offsetY := (float64(pageHeight) - drawHeight) / 2

		content := fmt.Sprintf("q\n%.4f 0 0 %.4f %.4f %.4f cm\n/Im%d Do\nQ\n", drawWidth, drawHeight, offsetX, offsetY, i+1)
		contentObj := fmt.Sprintf("<< /Length %d >>\nstream\n%sendstream", len(content), content)
		contentIDs[i] = addObject([]byte(contentObj))
	}

	pagesNodeID := len(objects) + len(pages)
	for i := range pages {
		pageWidth := pages[i].Width
		pageHeight := pages[i].Height
		if meta.PageWidth > 0 && meta.PageHeight > 0 {
			pageWidth = meta.PageWidth
			pageHeight = meta.PageHeight
		}

		pageObj := fmt.Sprintf("<< /Type /Page /Parent %d 0 R /MediaBox [0 0 %d %d] /Resources << /XObject << /Im%d %d 0 R >> >> /Contents %d 0 R >>",
			pagesNodeID, pageWidth, pageHeight, i+1, imageIDs[i], contentIDs[i])
		pageIDs[i] = addObject([]byte(pageObj))
	}

	var kids strings.Builder
	for _, id := range pageIDs {
		kids.WriteString(strconv.Itoa(id))
		kids.WriteString(" 0 R ")
	}
	pagesObj := fmt.Sprintf("<< /Type /Pages /Count %d /Kids [%s] >>", len(pageIDs), strings.TrimSpace(kids.String()))
	actualPagesID := addObject([]byte(pagesObj))

	if actualPagesID != pagesNodeID {
		return errors.New("internal PDF object numbering mismatch")
	}

	catalogID := addObject([]byte(fmt.Sprintf("<< /Type /Catalog /Pages %d 0 R >>", actualPagesID)))

	var buf bytes.Buffer
	buf.WriteString("%PDF-1.4\n%\xFF\xFF\xFF\xFF\n")

	offsets := make([]int, len(objects))
	for id := 1; id < len(objects); id++ {
		offsets[id] = buf.Len()
		fmt.Fprintf(&buf, "%d 0 obj\n", id)
		buf.Write(objects[id].data)
		buf.WriteString("\nendobj\n")
	}

	xrefOffset := buf.Len()
	fmt.Fprintf(&buf, "xref\n0 %d\n", len(objects))
	buf.WriteString("0000000000 65535 f \n")
	for id := 1; id < len(objects); id++ {
		fmt.Fprintf(&buf, "%010d 00000 n \n", offsets[id])
	}

	fmt.Fprintf(&buf, "trailer\n<< /Size %d /Root %d 0 R /Info %d 0 R >>\nstartxref\n%d\n%%%%EOF\n",
		len(objects), catalogID, infoID, xrefOffset)

	if err := os.WriteFile(outputPath, buf.Bytes(), 0o644); err != nil {
		return fmt.Errorf("write pdf: %w", err)
	}

	return nil
}

func extractBookCode(input string) (string, error) {
	trimmed := strings.TrimSpace(input)
	if trimmed == "" {
		return "", errors.New("empty input")
	}

	if simpleBookCode(trimmed) {
		return trimmed, nil
	}

	u, err := url.Parse(trimmed)
	if err != nil {
		return "", fmt.Errorf("parse input: %w", err)
	}

	if code := u.Query().Get("bkcode"); simpleBookCode(code) {
		return code, nil
	}

	parts := strings.Split(strings.Trim(u.Path, "/"), "/")
	for i := 0; i < len(parts)-1; i++ {
		if (parts[i] == "books" || parts[i] == "read") && simpleBookCode(parts[i+1]) {
			return parts[i+1], nil
		}
	}

	last := parts[len(parts)-1]
	if simpleBookCode(last) {
		return last, nil
	}

	return "", fmt.Errorf("could not extract Calameo book code from %q", input)
}

func buildToken(expires, path, signature string) string {
	return "?_token_=exp=" + expires + "~acl=" + path + "~hmac=" + signature
}

func buildSignedPageURL(base, key string, pageNum int, token, ext string) string {
	return strings.TrimRight(base, "/") + "/" + key + "/p" + strconv.Itoa(pageNum) + "." + ext + token
}

func normalizeURL(u string) string {
	u = strings.TrimSpace(u)
	if u == "" {
		return ""
	}
	if strings.HasPrefix(u, "//") {
		return "https:" + u
	}
	return u
}

func parseOutputOptions(input string) (outputOptions, error) {
	var opts outputOptions
	for _, part := range strings.Split(input, ",") {
		switch strings.ToLower(strings.TrimSpace(part)) {
		case "":
		case "pdf", "pvg":
			opts.PDF = true
		case "jpg", "jpeg":
			opts.JPG = true
		case "svg":
			opts.SVG = true
		case "svgz":
			opts.SVGZ = true
		default:
			return outputOptions{}, fmt.Errorf("unsupported format %q; use pdf,jpg,svg,svgz", part)
		}
	}

	if !opts.PDF && !opts.JPG && !opts.SVG && !opts.SVGZ {
		return outputOptions{}, errors.New("no output formats selected")
	}

	return opts, nil
}

func (o outputOptions) needsRawDir() bool {
	return o.JPG || o.SVG || o.SVGZ
}

func sanitizeFilename(name string) string {
	name = strings.TrimSpace(name)
	if name == "" {
		return "calameo-book"
	}

	invalid := regexp.MustCompile(`[<>:"/\\|?*\x00-\x1F]`)
	name = invalid.ReplaceAllString(name, "_")
	name = strings.Join(strings.Fields(name), " ")
	name = strings.Trim(name, " .")
	if name == "" {
		return "calameo-book"
	}
	return name
}

func simpleBookCode(s string) bool {
	if len(s) < 20 || len(s) > 24 {
		return false
	}

	for _, r := range s {
		if (r < '0' || r > '9') && (r < 'a' || r > 'z') && (r < 'A' || r > 'Z') {
			return false
		}
	}
	return true
}

func pdfString(s string) string {
	replacer := strings.NewReplacer(`\`, `\\`, `(`, `\(`, `)`, `\)`)
	return "(" + replacer.Replace(s) + ")"
}

func minFloat(a, b float64) float64 {
	if a < b {
		return a
	}
	return b
}

func exitErr(err error) {
	fmt.Fprintln(os.Stderr, "error:", err)
	os.Exit(1)
}
