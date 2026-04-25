package domain

import "errors"

var (
	// ErrNotFound is returned when a requested resource does not exist.
	ErrNotFound = errors.New("resource not found")

	// ErrDuplicate is returned when an entity with the same fingerprint
	// already exists — used by the Processor to skip re-indexing.
	ErrDuplicate = errors.New("duplicate resource")

	// ErrRateLimited is returned by the crawler HTTP client when the
	// target server responds with HTTP 429. The caller must not retry
	// immediately.
	ErrRateLimited = errors.New("rate limited by target server")

	// ErrDisallowedByRobots is returned when the robots.txt for a source
	// explicitly disallows crawling the requested path.
	ErrDisallowedByRobots = errors.New("path disallowed by robots.txt")
)