package processor

import (
	"regexp"
	"strconv"
	"strings"
	"unicode"

	"github.com/applytude/jobcrawler/internal/domain"
)

// techKeywords is the canonical list of technology tags the system extracts.
// All entries must be lowercase. Order is irrelevant — matching is substring-based.
var techKeywords = []string{
	// Languages
	"golang", "python", "java", "javascript", "typescript", "rust",
	"c++", "c#", "ruby", "scala", "kotlin", "swift", "php", "r",
	// Backend frameworks
	"spring", "django", "fastapi", "flask", "express", "nestjs",
	"gin", "fiber", "echo",
	// Frontend
	"react", "vue", "angular", "svelte", "nextjs", "nuxt",
	// Databases
	"postgresql", "mysql", "mongodb", "redis", "elasticsearch",
	"cassandra", "clickhouse", "sqlite", "dynamodb", "neo4j",
	// Cloud & DevOps
	"aws", "azure", "gcp", "kubernetes", "docker", "terraform",
	"ansible", "helm", "argocd", "jenkins", "github actions",
	// Data & ML
	"kafka", "spark", "airflow", "dbt", "hadoop", "flink",
	"tensorflow", "pytorch", "pandas", "scikit-learn",
	// Other
	"graphql", "grpc", "rest", "microservices", "linux",
}

// remoteFullKeywords triggers RemoteFull when found in job description.
var remoteFullKeywords = []string{
	"fully remote", "100% remote", "vollständig remote", "remote only",
	"anywhere", "work from anywhere", "distributed team",
}

// remotePartialKeywords triggers RemotePartial.
var remotePartialKeywords = []string{
	"hybrid", "home office", "homeoffice", "teilweise remote",
	"remote möglich", "remote possible", "partially remote",
	"flexible working", "work from home", "wfh",
}

// Normalizer transforms raw scraped strings into clean, typed domain values.
// It is stateless — safe to share across goroutines.
type Normalizer struct{}

// NewNormalizer creates a Normalizer.
func NewNormalizer() *Normalizer {
	return &Normalizer{}
}

// NormalizeTitle cleans a raw job title:
//   - Trims whitespace
//   - Removes common trailing noise (" - Remote", " (m/w/d)", " - Hamburg")
//   - Title-cases the result
func (n *Normalizer) NormalizeTitle(raw string) string {
	title := strings.TrimSpace(raw)

	// Remove trailing " - <anything>" patterns often appended by job boards.
	trailingNoise := regexp.MustCompile(`\s*[-–|]\s*(Remote|Hybrid|[\w\s]+)$`)
	title = trailingNoise.ReplaceAllString(title, "")

	// Remove gender notation common in German job postings.
	genderNoise := regexp.MustCompile(`\s*\(m\s*/\s*[wfd]\s*/?\s*[d]?\)`)
	title = genderNoise.ReplaceAllStringFunc(title, func(_ string) string { return "" })

	title = strings.TrimSpace(title)
	return titleCase(title)
}

// NormalizeLocation extracts the city from strings like "Hamburg, Germany"
// or "Berlin (Hybrid)" or "Remote / München".
func (n *Normalizer) NormalizeLocation(raw string) string {
	loc := strings.TrimSpace(raw)

	// Strip parenthetical annotations: "Berlin (Hybrid)" → "Berlin"
	parenRe := regexp.MustCompile(`\s*\([^)]*\)`)
	loc = parenRe.ReplaceAllString(loc, "")

	// Take the first part before comma or slash.
	// "Hamburg, Germany" → "Hamburg"
	// "Remote / München" → keep as-is if "remote" prefix
	lower := strings.ToLower(loc)
	if strings.HasPrefix(lower, "remote") {
		return "Remote"
	}

	parts := regexp.MustCompile(`[,/]`).Split(loc, 2)
	city := strings.TrimSpace(parts[0])

	return city
}

// DetectRemote analyses the full job description text for remote-work signals.
// Returns RemoteFull, RemotePartial, or RemoteNone.
func (n *Normalizer) DetectRemote(description string) domain.RemoteType {
	lower := strings.ToLower(description)

	for _, kw := range remoteFullKeywords {
		if strings.Contains(lower, kw) {
			return domain.RemoteFull
		}
	}
	for _, kw := range remotePartialKeywords {
		if strings.Contains(lower, kw) {
			return domain.RemotePartial
		}
	}
	return domain.RemoteNone
}

// ExtractTags scans the job title and description for known technology keywords.
// Returns a deduplicated, sorted slice of matched tags (all lowercase).
func (n *Normalizer) ExtractTags(title, description string) []string {
	combined := strings.ToLower(title + " " + description)
	seen := make(map[string]bool)

	for _, kw := range techKeywords {
		if strings.Contains(combined, kw) {
			seen[kw] = true
		}
	}

	tags := make([]string, 0, len(seen))
	for kw := range seen {
		tags = append(tags, kw)
	}
	return tags
}

// salaryPatterns lists regex patterns for common salary formats in job ads.
// Each pattern must capture: optional currency prefix, min amount, max amount.
var (
	// "60.000 – 80.000 €" or "60.000 - 80.000 EUR"
	patternDE = regexp.MustCompile(`(\d[\d.]+)\s*[-–]\s*(\d[\d.]+)\s*(€|EUR|CHF|GBP|USD)?`)
	// "€60k - €80k" or "$60k-$80k"
	patternShort = regexp.MustCompile(`([€$£])\s*(\d+)k?\s*[-–]\s*([€$£])?\s*(\d+)k?`)
	// "up to 90.000 €"
	patternUpTo = regexp.MustCompile(`(?i)up\s+to\s+(\d[\d.]+)\s*(€|EUR|USD|GBP)?`)
)

// ParseSalary attempts to extract a salary range from free-form text.
// Returns nil pointers when the salary cannot be determined.
func (n *Normalizer) ParseSalary(text string) (min *int, max *int, currency string) {
	if text == "" {
		return nil, nil, ""
	}

	// Try "60.000 – 80.000 €" pattern first.
	if m := patternDE.FindStringSubmatch(text); len(m) >= 3 {
		minVal := parseAmount(m[1])
		maxVal := parseAmount(m[2])
		cur := "EUR"
		if len(m) >= 4 && m[3] != "" {
			cur = normaliseCurrency(m[3])
		}
		return &minVal, &maxVal, cur
	}

	// Try "€60k - €80k" pattern.
	if m := patternShort.FindStringSubmatch(text); len(m) >= 5 {
		minVal := parseAmount(m[2]) * 1000
		maxVal := parseAmount(m[4]) * 1000
		cur := normaliseCurrency(m[1])
		return &minVal, &maxVal, cur
	}

	// Try "up to 90.000 €" — only a max.
	if m := patternUpTo.FindStringSubmatch(text); len(m) >= 2 {
		maxVal := parseAmount(m[1])
		cur := "EUR"
		if len(m) >= 3 && m[2] != "" {
			cur = normaliseCurrency(m[2])
		}
		return nil, &maxVal, cur
	}

	return nil, nil, ""
}

// parseAmount converts "60.000" or "60000" or "60" to int.
func parseAmount(s string) int {
	// Remove thousands separator (German uses ".")
	s = strings.ReplaceAll(s, ".", "")
	s = strings.ReplaceAll(s, ",", "")
	s = strings.TrimSpace(s)
	v, _ := strconv.Atoi(s)
	return v
}

// normaliseCurrency maps symbol variants to ISO 4217 codes.
func normaliseCurrency(s string) string {
	switch strings.TrimSpace(s) {
	case "€", "EUR":
		return "EUR"
	case "$", "USD":
		return "USD"
	case "£", "GBP":
		return "GBP"
	case "CHF":
		return "CHF"
	default:
		return "EUR"
	}
}

// titleCase converts a string to Title Case, respecting common abbreviations.
func titleCase(s string) string {
	words := strings.Fields(s)
	for i, w := range words {
		if len(w) == 0 {
			continue
		}
		runes := []rune(w)
		runes[0] = unicode.ToUpper(runes[0])
		words[i] = string(runes)
	}
	return strings.Join(words, " ")
}