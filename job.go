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
	retryIN time.Duration
}

// Error returns the error as a string
func (e RetryJobError) Error() string {
	return fmt.Sprintf("temporary error: %s, retry in %d seconds", e.message, e.retryIN)
}

// NewRetryJobError creates a new error of type RetryJobError
func NewRetryJobError(message string) RetryJobError {
	return RetryJobError{message: message}
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

// HasAny checks whether the jobStatus has atleast any 1 of the status codes
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
	js = &t
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
}

// Returns a json serialized string of boltJob
func (bj boltJob) String() string {
	b, _ := json.Marshal(bj)
	return string(b)
}

// getBoltJob creates a new boltJob from a Worker job
func getBoltJob(job worker.Job) *boltJob {
	bj := new(boltJob)
	bj.Name = job.Args.String()
	bj.Args = job.Args
	bj.Handler = job.Handler
	bj.Status = JobPending
	return bj
}
