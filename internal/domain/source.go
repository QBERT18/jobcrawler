package domain

// JobSource identifies which platform a job listing was crawled from.
type JobSource string

const (
	SourceStepstone JobSource = "STEPSTONE"
	SourceIndeed    JobSource = "INDEED"
	SourceXing      JobSource = "XING"
	SourceLinkedIn  JobSource = "LINKEDIN"
)

// AllSources lists every supported crawl source.
var AllSources = []JobSource{
	SourceStepstone,
	SourceIndeed,
	SourceXing,
	SourceLinkedIn,
}

// RemoteType describes the remote-work policy of a job listing.
type RemoteType string

const (
	RemoteFull    RemoteType = "FULL"    // 100 % remote
	RemotePartial RemoteType = "PARTIAL" // Hybrid / home-office days
	RemoteNone    RemoteType = "NONE"    // On-site only
)