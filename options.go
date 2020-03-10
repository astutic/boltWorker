package boltworker

import (
	"github.com/boltdb/bolt"
	"github.com/gobuffalo/buffalo/worker"
)

// JobNameGenerator function that will be run to determine the key for the
// job which will be saved in boltDB
type JobNameGenerator func(worker.Job) string

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
	DBSyncInterval   string
	MaxRetryAttempts int
	IdleSleepTime    string
	JobNameHandler   JobNameGenerator
}
