package crawler

import (
	"fmt"

	"github.com/applytude/jobcrawler/internal/domain"
)

// SourceRegistry maps a JobSource to its crawler implementation.
// All registered sources are available to the CrawlerWorker at runtime.
type SourceRegistry struct {
	sources map[domain.JobSource]Source
}

// NewRegistry builds a SourceRegistry pre-populated with all crawlers.
// Add new crawlers here — they will be picked up automatically by the worker.
func NewRegistry() *SourceRegistry {
	r := &SourceRegistry{
		sources: make(map[domain.JobSource]Source),
	}
	r.register(&StepstoneCrawler{})
	r.register(&XingCrawler{})
	r.register(&IndeedCrawler{})
	return r
}

// register adds a Source to the registry.
func (r *SourceRegistry) register(s Source) {
	r.sources[s.Name()] = s
}

// GetSource returns the Source for the given JobSource identifier.
// Returns an error if the source is not registered — this prevents silent
// no-ops when a new source is added to the scheduler but not to the registry.
func (r *SourceRegistry) GetSource(source domain.JobSource) (Source, error) {
	s, ok := r.sources[source]
	if !ok {
		return nil, fmt.Errorf("no crawler registered for source %q", source)
	}
	return s, nil
}

// All returns all registered sources — used by the scheduler to iterate.
func (r *SourceRegistry) All() []Source {
	out := make([]Source, 0, len(r.sources))
	for _, s := range r.sources {
		out = append(out, s)
	}
	return out
}