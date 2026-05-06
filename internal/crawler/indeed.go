package crawler

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/url"
	"regexp"
	"strings"

	"github.com/PuerkitoBio/goquery"
	"github.com/applytude/jobcrawler/internal/domain"
)

const indeedBase = "https://de.indeed.com"

// indeedJK is a 16-hex-character job ID that uniquely identifies an Indeed
// posting. Listing pages expose it via `data-jk="..."` on each card.
var indeedJK = regexp.MustCompile(`^[a-f0-9]{8,32}$`)

// IndeedCrawler parses Indeed job listings and detail pages.
//
// Indeed sits behind Cloudflare's managed challenge — fetches only succeed via
// the uTLS-backed transport in pkg/httputil. This parser is therefore only
// useful in concert with that client; against stdlib net/http, every fetch
// returns the security-check page rather than HTML.
type IndeedCrawler struct{}

func (i *IndeedCrawler) Name() domain.JobSource {
	return domain.SourceIndeed
}

// ParseListing extracts detail-page URLs from an Indeed search-results page.
// Indeed embeds the canonical job ID on every card via `data-jk`; we don't
// rely on href patterns because Indeed serves multiple href shapes
// (/viewjob, /rc/clk, /pagead/clk) all pointing at the same job.
func (i *IndeedCrawler) ParseListing(html []byte) ([]string, error) {
	doc, err := goquery.NewDocumentFromReader(bytes.NewReader(html))
	if err != nil {
		return nil, fmt.Errorf("indeed parse listing: %w", err)
	}

	var urls []string
	seen := make(map[string]bool)

	doc.Find("[data-jk]").Each(func(_ int, sel *goquery.Selection) {
		jk, ok := sel.Attr("data-jk")
		if !ok {
			return
		}
		jk = strings.TrimSpace(jk)
		if !indeedJK.MatchString(jk) {
			return
		}
		if isPlaceholderJK(jk) {
			return
		}
		abs := indeedBase + "/viewjob?jk=" + jk
		if !seen[abs] {
			seen[abs] = true
			urls = append(urls, abs)
		}
	})

	return urls, nil
}

// ParseDetail extracts structured job data from an Indeed detail page.
//
// Indeed embeds a schema.org JobPosting JSON-LD blob on every detail page —
// authoritative because it's what Indeed publishes to search engines and
// rarely changes shape. We parse that first; data-testid CSS selectors fill
// gaps when individual fields are missing.
func (i *IndeedCrawler) ParseDetail(html []byte) (*RawJob, error) {
	doc, err := goquery.NewDocumentFromReader(bytes.NewReader(html))
	if err != nil {
		return nil, fmt.Errorf("indeed parse detail: %w", err)
	}

	job := &RawJob{}

	// ── Primary: JSON-LD JobPosting ─────────────────────────────────────────
	if jp := findIndeedJobPosting(doc); jp != nil {
		job.Title = strings.TrimSpace(jp.Title)
		if jp.HiringOrganization != nil {
			job.Company = strings.TrimSpace(jp.HiringOrganization.Name)
		}
		job.Location = firstIndeedLocality(jp.JobLocation)
		job.Description = stripHTMLText(jp.Description)
		job.SalaryText = formatIndeedSalary(jp.BaseSalary)
		job.PublishedAt = jp.DatePosted
	}

	// ── Fallbacks (only fill what's still empty) ────────────────────────────
	if job.Title == "" {
		job.Title = strings.TrimSpace(doc.Find(`[data-testid="jobsearch-JobInfoHeader-title"]`).First().Text())
	}
	if job.Title == "" {
		job.Title = strings.TrimSpace(doc.Find("h1").First().Text())
	}
	if job.Title == "" {
		job.Title, _ = doc.Find(`meta[property="og:title"]`).Attr("content")
	}

	if job.Company == "" {
		job.Company = strings.TrimSpace(doc.Find(`[data-testid="inlineHeader-companyName"]`).First().Text())
	}

	if job.Location == "" {
		job.Location = strings.TrimSpace(doc.Find(`[data-testid="inlineHeader-companyLocation"]`).First().Text())
	}
	if job.Location == "" {
		job.Location = strings.TrimSpace(doc.Find(`[data-testid="jobsearch-JobInfoHeader-companyLocation"]`).First().Text())
	}

	// Canonical URL (preferred — it's the long form Indeed itself uses).
	canonical, _ := doc.Find(`link[rel="canonical"]`).Attr("href")
	if canonical == "" {
		canonical, _ = doc.Find(`meta[property="og:url"]`).Attr("content")
	}
	job.URL = canonical
	job.ExternalID = extractIndeedJK(canonical)

	return job, nil
}

// indeedJobPosting mirrors the schema.org JobPosting fields Indeed emits.
// Only fields we consume are modelled; unknown fields are ignored on unmarshal.
type indeedJobPosting struct {
	Type               string           `json:"@type"`
	Title              string           `json:"title"`
	Description        string           `json:"description"`
	DatePosted         string           `json:"datePosted"`
	HiringOrganization *indeedHiringOrg `json:"hiringOrganization"`
	JobLocation        json.RawMessage  `json:"jobLocation"` // object OR array
	BaseSalary         *indeedBaseSalary `json:"baseSalary"`
}

type indeedHiringOrg struct {
	Name string `json:"name"`
}

type indeedJobLocation struct {
	Address *indeedPostalAddress `json:"address"`
}

type indeedPostalAddress struct {
	AddressLocality string `json:"addressLocality"`
	AddressRegion   string `json:"addressRegion"`
	AddressCountry  any    `json:"addressCountry"` // string OR object
}

type indeedBaseSalary struct {
	Currency string          `json:"currency"`
	Value    json.RawMessage `json:"value"`
}

func findIndeedJobPosting(doc *goquery.Document) *indeedJobPosting {
	var found *indeedJobPosting
	doc.Find(`script[type="application/ld+json"]`).EachWithBreak(func(_ int, sel *goquery.Selection) bool {
		raw := strings.TrimSpace(sel.Text())
		if raw == "" {
			return true
		}
		var jp indeedJobPosting
		if err := json.Unmarshal([]byte(raw), &jp); err == nil && jp.Type == "JobPosting" {
			found = &jp
			return false
		}
		// Schema.org also allows arrays of entities; best-effort handle that.
		var arr []indeedJobPosting
		if err := json.Unmarshal([]byte(raw), &arr); err == nil {
			for k := range arr {
				if arr[k].Type == "JobPosting" {
					found = &arr[k]
					return false
				}
			}
		}
		return true
	})
	return found
}

func firstIndeedLocality(raw json.RawMessage) string {
	if len(raw) == 0 {
		return ""
	}
	var single indeedJobLocation
	if err := json.Unmarshal(raw, &single); err == nil && single.Address != nil {
		if c := strings.TrimSpace(single.Address.AddressLocality); c != "" {
			return c
		}
	}
	var many []indeedJobLocation
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

func formatIndeedSalary(s *indeedBaseSalary) string {
	if s == nil || len(s.Value) == 0 {
		return ""
	}
	return strings.TrimSpace(s.Currency + " " + string(s.Value))
}

// isPlaceholderJK detects the synthetic data-jk Indeed embeds in every listing
// page as a template/sentinel. The placeholder is always a 16-char hex string
// containing all 16 distinct hex digits exactly once — a permutation of
// "0123456789abcdef" (e.g. "f1e2d3c4b5a67890", "789abcdef0123456"). Real jks
// produced by Indeed's job-ID hash always repeat at least one digit.
func isPlaceholderJK(jk string) bool {
	if len(jk) != 16 {
		return false
	}
	seen := make(map[byte]bool, 16)
	for i := 0; i < len(jk); i++ {
		seen[jk[i]] = true
	}
	return len(seen) == 16
}

// extractIndeedJK pulls the jk query parameter from a canonical Indeed URL,
// e.g. https://de.indeed.com/viewjob?jk=daf248c83b67f9bd → "daf248c83b67f9bd".
func extractIndeedJK(rawURL string) string {
	if rawURL == "" {
		return ""
	}
	u, err := url.Parse(rawURL)
	if err != nil {
		return ""
	}
	return u.Query().Get("jk")
}
