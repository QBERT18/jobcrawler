package handler

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"

	"github.com/applytude/jobcrawler/internal/domain"
)

// respondJSON writes a JSON-encoded payload with the given status code.
// Logs a warning if marshalling fails — should never happen in practice.
func respondJSON(w http.ResponseWriter, statusCode int, payload any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(statusCode)

	if payload == nil {
		return
	}

	if err := json.NewEncoder(w).Encode(payload); err != nil {
		slog.Warn("respondJSON: failed to encode payload", slog.String("error", err.Error()))
	}
}

// errorResponse is the canonical error body returned by every endpoint.
type errorResponse struct {
	Error string `json:"error"`
	Code  string `json:"code"`
}

// respondError maps domain sentinel errors to appropriate HTTP status codes
// and writes a structured JSON error body.
func respondError(w http.ResponseWriter, err error) {
	var (
		status int
		code   string
	)

	switch {
	case errors.Is(err, domain.ErrNotFound):
		status = http.StatusNotFound
		code = "NOT_FOUND"
	case errors.Is(err, domain.ErrDuplicate):
		status = http.StatusConflict
		code = "CONFLICT"
	case errors.Is(err, domain.ErrRateLimited):
		status = http.StatusTooManyRequests
		code = "RATE_LIMITED"
	case errors.Is(err, domain.ErrDisallowedByRobots):
		status = http.StatusForbidden
		code = "FORBIDDEN"
	default:
		status = http.StatusInternalServerError
		code = "INTERNAL_ERROR"
	}

	respondJSON(w, status, errorResponse{
		Error: err.Error(),
		Code:  code,
	})
}