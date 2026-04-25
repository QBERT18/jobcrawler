package handler

import (
	"net/http"
	"strconv"
	"strings"

	"github.com/applytude/jobcrawler/internal/domain"
	"github.com/applytude/jobcrawler/internal/service"
	"github.com/go-chi/chi/v5"
)

// JobHandler wires HTTP requests to the JobService.
// All state lives in the injected service — the handler itself is stateless.
type JobHandler struct {
	service service.JobService
}

// NewJobHandler creates a JobHandler with the given service dependency.
func NewJobHandler(svc service.JobService) *JobHandler {
	return &JobHandler{service: svc}
}

// Search handles GET /api/v1/jobs
//
// Query parameters:
//
//	q           string       full-text search term
//	location    string       city or region filter
//	remote      string       FULL | PARTIAL | NONE
//	tags        string       comma-separated tech keywords (AND logic)
//	salary_min  int          minimum salary filter
//	source      string       comma-separated: STEPSTONE,INDEED,XING,LINKEDIN
//	page        int          page number, 1-based (default 1)
//	per_page    int          results per page (default 25, max 100)
//	sort        string       field:direction, e.g. published_at:desc
func (h *JobHandler) Search(w http.ResponseWriter, r *http.Request) {
	filter := domain.SearchFilter{
		Query:    strings.TrimSpace(r.URL.Query().Get("q")),
		Location: strings.TrimSpace(r.URL.Query().Get("location")),
		Remote:   domain.RemoteType(r.URL.Query().Get("remote")),
		Sort:     r.URL.Query().Get("sort"),
		Page:     1,
		PerPage:  25,
	}

	if tagsRaw := r.URL.Query().Get("tags"); tagsRaw != "" {
		for _, t := range strings.Split(tagsRaw, ",") {
			if trimmed := strings.TrimSpace(t); trimmed != "" {
				filter.Tags = append(filter.Tags, strings.ToLower(trimmed))
			}
		}
	}

	if sourceRaw := r.URL.Query().Get("source"); sourceRaw != "" {
		for _, s := range strings.Split(sourceRaw, ",") {
			if trimmed := strings.TrimSpace(s); trimmed != "" {
				filter.Sources = append(filter.Sources, domain.JobSource(strings.ToUpper(trimmed)))
			}
		}
	}

	if v := r.URL.Query().Get("salary_min"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 0 {
			filter.SalaryMin = n
		}
	}

	if v := r.URL.Query().Get("page"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 1 {
			filter.Page = n
		}
	}

	if v := r.URL.Query().Get("per_page"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 1 {
			if n > 100 {
				n = 100 // hard cap
			}
			filter.PerPage = n
		}
	}

	result, err := h.service.Search(r.Context(), filter)
	if err != nil {
		respondError(w, err)
		return
	}

	respondJSON(w, http.StatusOK, result)
}

// GetByID handles GET /api/v1/jobs/{id}
func (h *JobHandler) GetByID(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	if id == "" {
		respondError(w, domain.ErrNotFound)
		return
	}

	job, err := h.service.GetByID(r.Context(), id)
	if err != nil {
		respondError(w, err)
		return
	}

	respondJSON(w, http.StatusOK, job)
}

// GetStats handles GET /api/v1/jobs/stats
func (h *JobHandler) GetStats(w http.ResponseWriter, r *http.Request) {
	stats, err := h.service.GetStats(r.Context())
	if err != nil {
		respondError(w, err)
		return
	}

	respondJSON(w, http.StatusOK, stats)
}