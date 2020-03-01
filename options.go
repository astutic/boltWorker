package boltworker

import "github.com/boltdb/bolt"

// ConfigOptions are used to configure boltWorker
type ConfigOptions struct {
	Logger         Logger
	Name           string
	MaxConcurrency int
}

type boltDBOptions struct {
	ConfigOptions
	*bolt.Options
	FilePath        string
	CompletedBucket string
	PendingBucket   string
	FailedBucket    string
	PollDBTime      string
}
