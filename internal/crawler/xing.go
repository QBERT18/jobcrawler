package crawler

import (
	"bytes"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"

	"github.com/PuerkitoBio/goquery"
	"github.com/applytude/jobcrawler/internal/domain"
)

const xingBase = "https://www.xing.com"

// xingDetailPath matches Xing job detail URL paths, which look like:
//   /jobs/{city}-{slug-with-dashes}-{numeric-id}
// e.g. /jobs/altenberge-embedded-softwareentwickler-152619439
// The trailing numeric ID is the discriminator vs. category/directory/search
// paths like /jobs/search, /jobs/directory/a, /jobs/jobs-in-berlin, /jobs/remote.
var xingDetailPath = regexp.MustCompile(`^/jobs/[a-z0-9-]+-\d+$`)

// XingCrawler parses Xing job listings and detail pages.
type XingCrawler struct{}

func (x *XingCrawler) Name() domain.JobSource {
	return domain.SourceXing
}

// ParseListing extracts job detail page URLs from a Xing listing page.
// Xing detail hrefs are relative paths matching xingDetailPath; everything
// else on the page (category directories, city landings, search variants)
// shares the /jobs/ prefix and must be filtered out by the trailing numeric ID.
func (x *XingCrawler) ParseListing(html []byte) ([]string, error) {
	doc, err := goquery.NewDocumentFromReader(bytes.NewReader(html))
	if err != nil {
		return nil, fmt.Errorf("xing parse listing: %w", err)
	}

	var urls []string
	seen := make(map[string]bool)

	doc.Find("a[href]").Each(func(_ int, sel *goquery.Selection) {
		href, exists := sel.Attr("href")
		if !exists || href == "" {
			return
		}
		path := href
		if strings.HasPrefix(href, xingBase) {
			path = strings.TrimPrefix(href, xingBase)
		}
		if !xingDetailPath.MatchString(path) {
			return
		}
		abs := xingBase + path
		if !seen[abs] {
			seen[abs] = true
			urls = append(urls, abs)
		}
	})

	return urls, nil
}

// ParseDetail extracts structured job data from a Xing detail page.
//
// Xing embeds a schema.org JobPosting JSON-LD blob on every detail page —
// this is the authoritative source because it's what Xing publishes to
// search engines and rarely changes shape. We parse that first; data-testid
// CSS selectors are only a fallback if the JSON-LD is missing or malformed.
func (x *XingCrawler) ParseDetail(html []byte) (*RawJob, error) {
	doc, err := goquery.NewDocumentFromReader(bytes.NewReader(html))
	if err != nil {
		return nil, fmt.Errorf("xing parse detail: %w", err)
	}

	job := &RawJob{}

	// ── Primary: JSON-LD JobPosting ─────────────────────────────────────────
	if jp := findXingJobPosting(doc); jp != nil {
		job.Title = strings.TrimSpace(jp.Title)
		if jp.HiringOrganization != nil {
			job.Company = strings.TrimSpace(jp.HiringOrganization.Name)
		}
		job.Location = firstXingLocality(jp.JobLocation)
		job.Description = stripHTMLText(jp.Description)
		job.SalaryText = formatXingSalary(jp.BaseSalary)
		job.PublishedAt = jp.DatePosted
	}

	// ── Fallbacks (only fill what's still empty) ────────────────────────────
	if job.Title == "" {
		job.Title = strings.TrimSpace(doc.Find(`[data-testid="job-details-title"]`).First().Text())
	}
	if job.Title == "" {
		job.Title = strings.TrimSpace(doc.Find("h1").First().Text())
	}
	if job.Title == "" {
		job.Title, _ = doc.Find(`meta[property="og:title"]`).Attr("content")
	}

	if job.Company == "" {
		job.Company = strings.TrimSpace(doc.Find(`[data-testid="job-details-company-info-name"]`).First().Text())
	}

	if job.Location == "" {
		job.Location = strings.TrimSpace(doc.Find(`[data-testid="company-card-location"]`).First().Text())
	}

	if job.SalaryText == "" {
		job.SalaryText = strings.TrimSpace(doc.Find(`[data-testid="job-details-salary-card"]`).First().Text())
	}

	if job.Description == "" {
		job.Description = strings.TrimSpace(doc.Find(`[data-testid="expandable-content"]`).First().Text())
	}

	if job.PublishedAt == "" {
		job.PublishedAt, _ = doc.Find(`meta[name="date"]`).Attr("content")
	}

	// Canonical URL and external ID — always from the DOM.
	if canonical, exists := doc.Find(`link[rel="canonical"]`).Attr("href"); exists {
		job.URL = canonical
		job.ExternalID = extractXingID(canonical)
	}

	return job, nil
}

// xingJobPosting mirrors the schema.org JobPosting fields Xing emits in JSON-LD.
// Only the subset we consume is modelled; unknown fields are ignored on unmarshal.
type xingJobPosting struct {
	Type               string                `json:"@type"`
	Title              string                `json:"title"`
	Description        string                `json:"description"`
	DatePosted         string                `json:"datePosted"`
	HiringOrganization *xingHiringOrg        `json:"hiringOrganization"`
	JobLocation        json.RawMessage       `json:"jobLocation"` // object OR array
	BaseSalary         *xingBaseSalary       `json:"baseSalary"`
	EmploymentType     any                   `json:"employmentType"` // unused but noted
	JobLocationType    string                `json:"jobLocationType"`
}

type xingHiringOrg struct {
	Name string `json:"name"`
}

type xingJobLocation struct {
	Address *xingPostalAddress `json:"address"`
}

type xingPostalAddress struct {
	AddressLocality string `json:"addressLocality"`
	AddressRegion   string `json:"addressRegion"`
	AddressCountry  string `json:"addressCountry"`
}

type xingBaseSalary struct {
	Currency string          `json:"currency"`
	Value    json.RawMessage `json:"value"`
}

// findXingJobPosting scans every <script type="application/ld+json"> block
// for a JobPosting object and returns the first match, or nil. Xing usually
// only emits one per page but we don't rely on that.
func findXingJobPosting(doc *goquery.Document) *xingJobPosting {
	var found *xingJobPosting
	doc.Find(`script[type="application/ld+json"]`).EachWithBreak(func(_ int, sel *goquery.Selection) bool {
		raw := strings.TrimSpace(sel.Text())
		if raw == "" {
			return true
		}
		var jp xingJobPosting
		if err := json.Unmarshal([]byte(raw), &jp); err == nil && jp.Type == "JobPosting" {
			found = &jp
			return false
		}
		// Could also be an array of multiple entities; best-effort handle that.
		var arr []xingJobPosting
		if err := json.Unmarshal([]byte(raw), &arr); err == nil {
			for i := range arr {
				if arr[i].Type == "JobPosting" {
					found = &arr[i]
					return false
				}
			}
		}
		return true
	})
	return found
}

// firstXingLocality pulls a human-readable city from JobLocation, which
// schema.org allows to be either a single object or an array of them.
func firstXingLocality(raw json.RawMessage) string {
	if len(raw) == 0 {
		return ""
	}
	var single xingJobLocation
	if err := json.Unmarshal(raw, &single); err == nil && single.Address != nil {
		if c := strings.TrimSpace(single.Address.AddressLocality); c != "" {
			return c
		}
	}
	var many []xingJobLocation
	if err := json.Unmarshal(raw, &many); err == nil {
		for _, l := range many {
			if l.Address != nil {
				if c := strings.TrimSpace(l.Address.AddressLocality); c != "" {
					return c
				}
			}
		}
	}
	return ""
}

// formatXingSalary renders baseSalary for downstream normalization. Xing's
// salary shape varies (QuantitativeValue, MonetaryAmount, etc.), so we just
// marshal it back into the raw text the normalizer can regex over.
func formatXingSalary(s *xingBaseSalary) string {
	if s == nil || len(s.Value) == 0 {
		return ""
	}
	return strings.TrimSpace(s.Currency + " " + string(s.Value))
}

// stripHTMLText renders an HTML fragment as plain text by parsing it through
// goquery. Xing's JSON-LD description is HTML-encoded (<p>, <ul>, <li>, etc.);
// the rest of the pipeline expects plain text.
func stripHTMLText(htmlFrag string) string {
	s := strings.TrimSpace(htmlFrag)
	if s == "" {
		return ""
	}
	doc, err := goquery.NewDocumentFromReader(strings.NewReader(s))
	if err != nil {
		return s
	}
	return strings.TrimSpace(doc.Text())
}

// extractXingID extracts the trailing numeric job ID from a Xing detail URL.
// URL pattern: /jobs/{city}-{slug}-{numeric-id}
// e.g. /jobs/altenberge-embedded-softwareentwickler-152619439 → 152619439
var xingIDTail = regexp.MustCompile(`-(\d+)$`)

func extractXingID(rawURL string) string {
	trimmed := strings.TrimSuffix(rawURL, "/")
	if m := xingIDTail.FindStringSubmatch(trimmed); m != nil {
		return m[1]
	}
	return ""
}