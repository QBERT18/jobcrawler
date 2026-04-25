package crawler

import (
	"bytes"
	"fmt"
	"strings"

	"github.com/PuerkitoBio/goquery"
	"github.com/applytude/jobcrawler/internal/domain"
)

const stepstonBase = "https://www.stepstone.de"

// StepstoneCrawler parses Stepstone Germany job listings and detail pages.
type StepstoneCrawler struct{}

func (s *StepstoneCrawler) Name() domain.JobSource {
	return domain.SourceStepstone
}

// ParseListing extracts job detail page URLs from a Stepstone listing page.
// Stepstone renders job cards as <article> elements; each card contains an
// <a> link whose href points to the detail page.
func (s *StepstoneCrawler) ParseListing(html []byte) ([]string, error) {
	doc, err := goquery.NewDocumentFromReader(bytes.NewReader(html))
	if err != nil {
		return nil, fmt.Errorf("stepstone parse listing: %w", err)
	}

	var urls []string
	seen := make(map[string]bool)

	// Primary selector — job card article links.
	doc.Find("article a[href]").Each(func(_ int, sel *goquery.Selection) {
		href, exists := sel.Attr("href")
		if !exists || href == "" {
			return
		}
		// Stepstone hrefs are relative (/stellenangebote--...) or absolute.
		if strings.HasPrefix(href, "/") {
			href = stepstonBase + href
		}
		// Only keep job detail links — filter out navigation and ads.
		if strings.Contains(href, "/stellenangebote--") && !seen[href] {
			seen[href] = true
			urls = append(urls, href)
		}
	})

	// Fallback: any link containing the job detail path pattern.
	if len(urls) == 0 {
		doc.Find("a[href*='/stellenangebote--']").Each(func(_ int, sel *goquery.Selection) {
			href, _ := sel.Attr("href")
			if strings.HasPrefix(href, "/") {
				href = stepstonBase + href
			}
			if !seen[href] {
				seen[href] = true
				urls = append(urls, href)
			}
		})
	}

	return urls, nil
}

// ParseDetail extracts structured job data from a Stepstone detail page.
// Selectors are based on Stepstone's current (2024) HTML structure.
// Add fallback selectors below each primary one to improve resilience.
func (s *StepstoneCrawler) ParseDetail(html []byte) (*RawJob, error) {
	doc, err := goquery.NewDocumentFromReader(bytes.NewReader(html))
	if err != nil {
		return nil, fmt.Errorf("stepstone parse detail: %w", err)
	}

	// Stepstone inlines styled-components CSS as <style> blocks inside the
	// same wrappers that contain the visible text. goquery's .Text() walks
	// every descendant — including <style> — so strip those (and <script>)
	// up front or every field we extract will be prefixed with minified CSS.
	doc.Find("style, script").Remove()

	job := &RawJob{}

	// Title — primary: h1, fallback: og:title meta tag
	job.Title = strings.TrimSpace(doc.Find("h1").First().Text())
	if job.Title == "" {
		job.Title, _ = doc.Find(`meta[property="og:title"]`).Attr("content")
	}

	// Company name
	job.Company = strings.TrimSpace(
		doc.Find("[data-at='header-company-name']").First().Text(),
	)
	if job.Company == "" {
		job.Company = strings.TrimSpace(
			doc.Find(".at-listing__list-icons_company-name").First().Text(),
		)
	}

	// Location
	job.Location = strings.TrimSpace(
		doc.Find("[data-at='job-ad-header-city']").First().Text(),
	)
	if job.Location == "" {
		job.Location = strings.TrimSpace(
			doc.Find(".at-listing__list-icons_location").First().Text(),
		)
	}

	// Salary — not always present
	job.SalaryText = strings.TrimSpace(
		doc.Find("[data-at='salary']").First().Text(),
	)

	// Full description text — strip HTML, keep plain text
	descNode := doc.Find(".at-section-text-description, [data-at='jobad-tasks-section']").First()
	job.Description = strings.TrimSpace(descNode.Text())
	if job.Description == "" {
		// Last resort: take the largest text block on the page
		job.Description = strings.TrimSpace(doc.Find("main").First().Text())
	}

	// External ID from the canonical URL
	if canonical, exists := doc.Find(`link[rel="canonical"]`).Attr("href"); exists {
		job.URL = canonical
		job.ExternalID = extractStepstoneID(canonical)
	}

	// Published date (ISO string if present in meta)
	job.PublishedAt, _ = doc.Find(`meta[name="date"]`).Attr("content")

	return job, nil
}

// extractStepstoneID pulls the numeric job ID from a Stepstone URL.
// URL pattern: /stellenangebote--job-title--{id}.html
func extractStepstoneID(url string) string {
	parts := strings.Split(url, "--")
	if len(parts) < 2 {
		return ""
	}
	last := parts[len(parts)-1]
	return strings.TrimSuffix(last, ".html")
}