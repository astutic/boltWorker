package boltworker

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/gobuffalo/buffalo/worker"
)

// RetryJobError denotes jobs which temporarily failed and should be retried
type RetryJobError struct {
	message string
	RetryIN time.Duration
}

// NewRetryJobError returns an instance of RetryJobError,
// this denotes a temporary failure and the job will be retried
func NewRetryJobError(msg string) RetryJobError {
	return RetryJobError{message: msg}
}

// Error returns the error as a string
func (e RetryJobError) Error() string {
	return fmt.Sprintf("temporary error: %s, retry in %v seconds", e.message, e.RetryIN.Seconds())
}

// SetRetryTime sets the retry time in time.Duration
func (e *RetryJobError) SetRetryTime(t time.Duration) {
	e.RetryIN = t
}

type jobStatusCode uint8

const (
	//JobPending denotes the job is pending
	JobPending jobStatusCode = 1 << iota
	//JobInProcess denotes the job is being worked by a worker
	JobInProcess
	//JobReAttempt denotes the job needs a retry
	JobReAttempt
	//JobFailed denotes the job has failed and no need to reattempt
	JobFailed
	//JobDone denotes the job completed successfully
	JobDone
)

// HasAll checks whether the jobStatus has all the status codes passed set.
func (js jobStatusCode) HasAll(jobStatus ...jobStatusCode) bool {
	var status jobStatusCode
	for _, s := range jobStatus {
		status = s | status
	}
	return status == js
}

// HasAny checks whether the jobStatus has any 1 of the status codes
// passed set.
func (js jobStatusCode) HasAny(jobStatus ...jobStatusCode) bool {
	var status jobStatusCode
	for _, s := range jobStatus {
		status = s | status
	}
	return js&status != 0
}

// Remove removes the status code from jobStatus
func (js *jobStatusCode) Remove(jobStatus jobStatusCode) *jobStatusCode {
	t := *js
	t = t &^ jobStatus
	*js = t
	return js
}

// boltJob is Job to be saved & processed by a Worker
type boltJob struct {
	// Name of the job
	Name string
	// Args that will be passed to the Handler when run
	Args worker.Args
	// Handler that will be run by the worker
	Handler string
	// Status of the job
	Status jobStatusCode
	// Attempts counts the number of retried attempts
	Attempt int
	// WorkAT stores the time the worker has to work at
	WorkAT time.Time
	// Time when the job was first added
	TimeAdded time.Time
	// Time when the job was last worked on
	TimeLastWorkDone time.Time
}

// Returns a json serialized string of boltJob
func (bj boltJob) String() string {
	b, _ := json.Marshal(bj)
	return string(b)
}

// getBoltJob creates a new boltJob from a Worker job
func getBoltJob(job worker.Job, jn JobNameGenerator) *boltJob {
	bj := new(boltJob)
	// Set the Name attribute if you want to use custom IDs instead of auto-increment ids.
	bj.Name = jn(job)
	bj.Args = job.Args
	bj.Handler = job.Handler
	bj.Status = JobPending
	bj.TimeAdded = time.Now()
	return bj
}
