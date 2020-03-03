package boltworker

import "github.com/boltdb/bolt"

// Options are used to configure boltWorker
type Options struct {
	BoltOptions      bolt.Options
	Logger           Logger
	Name             string
	MaxConcurrency   int
	FilePath         string
	CompletedBucket  string
	PendingBucket    string
	FailedBucket     string
	PollDBTime       string
	MaxRetryAttempts int
	IdleSleepTime    string
	JobNameHandler   JobNameGenerator
}
