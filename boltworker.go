/*
 *
 * BoltWorker - A very simple worker for go buffalo with persistence via boltDB.
 * This code is a modification of SimpleWorker implemented here
 * https://github.com/gobuffalo/buffalo/blob/master/worker/simple.go
 * to support persistence via boltDB.
 *
 */

package boltworker

import (
	"container/list"
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/markbates/safe"

	"github.com/gobuffalo/x/defaults"
	"github.com/sirupsen/logrus"

	"github.com/gobuffalo/buffalo/worker"
)

// Ensure the Worker adaptor is implemented
var _ worker.Worker = &BoltWorker{}

// BoltWorker is a basic implementation of buffalo worker interface
// which persists job data in boltDB
type BoltWorker struct {
	Logger       Logger
	ctx          context.Context
	cancel       context.CancelFunc
	handlers     map[string]worker.Handler
	handlerLock  *sync.RWMutex
	DB           *boltDB
	PollDBTime   time.Duration
	Concurrency  int
	getWorkChan  chan *boltJob
	pushWorkChan chan *boltJob
	updateDBChan chan *boltJob
	jobQueue     *list.List
	qManagerLock *sync.Mutex
}

// NewBoltWorker creates a buffalo worker interface implementation which
// persists data in boltDB defined in the opts
func NewBoltWorker(opts Options) *BoltWorker {
	return NewBoltWorkerWithContext(context.Background(), opts)
}

// NewBoltWorkerWithContext creates a buffalo worker interface implementation
// which persists data in boltDB defined in opts
func NewBoltWorkerWithContext(ctx context.Context, opts Options) *BoltWorker {
	ctx, cancel := context.WithCancel(ctx)

	// Set defaults if not provided in options
	opts.Name = defaults.String(opts.Name, "buffalo")
	opts.MaxConcurrency = defaults.Int(opts.MaxConcurrency, 10)
	opts.CompletedBucket = defaults.String(opts.CompletedBucket, "completed")
	opts.PendingBucket = defaults.String(opts.PendingBucket, "pending")
	opts.FailedBucket = defaults.String(opts.FailedBucket, "failed")
	opts.PollDBTime = defaults.String(opts.PollDBTime, "5s")

	if opts.Logger == nil {
		logger := logrus.New()
		logger.Formatter = &logrus.TextFormatter{}
		logger.Level = logrus.InfoLevel
		opts.Logger = logger
	}

	bDB := NewBoltDB(&opts)
	pollTime, err := time.ParseDuration(opts.PollDBTime)
	if err != nil {
		panic(err)
	}

	return &BoltWorker{
		Logger:       opts.Logger,
		ctx:          ctx,
		cancel:       cancel,
		handlers:     map[string]worker.Handler{},
		handlerLock:  &sync.RWMutex{},
		DB:           bDB,
		Concurrency:  opts.MaxConcurrency,
		qManagerLock: &sync.Mutex{},
		PollDBTime:   pollTime,
	}
}

//Register a work handler with the name of the work
func (bw *BoltWorker) Register(name string, h worker.Handler) error {
	bw.Logger.Debugf("register called for %s, waiting for lock", name)
	bw.handlerLock.Lock()
	defer bw.handlerLock.Unlock()
	if _, ok := bw.handlers[name]; ok {
		err := fmt.Errorf("handler already exists for %s", name)
		bw.Logger.Errorf(err.Error())
		return err
	}
	bw.handlers[name] = h
	bw.Logger.Debugf("registered handler for %s", name)
	return nil
}

// Start boltWorker
func (bw *BoltWorker) Start(ctx context.Context) error {
	bw.Logger.Info("Starting BoltWorker background worker")
	bw.ctx, bw.cancel = context.WithCancel(ctx)
	err := bw.DB.Init()
	if err != nil {
		err = fmt.Errorf("error initializing boltDB: %s", err)
		bw.Logger.Error(err)
		return err
	}
	bw.jobQueue = list.New()
	bw.getWorkChan = make(chan *boltJob)
	bw.pushWorkChan = make(chan *boltJob)
	bw.updateDBChan = make(chan *boltJob)
	go func() {
		select {
		case <-ctx.Done():
			bw.Stop()
		}
	}()

	go bw.queueManager()

	go bw.SpawnWorkers()

	go bw.LoadPendingJobs()

	return nil
}

// QueueManager is the single thread which is responsible for interfacing with boltDB
// Do not directly access the DB but only via the queueManager
func (bw *BoltWorker) queueManager() {
	bw.qManagerLock.Lock()
	bw.Logger.Info("starting queue manager")
	defer bw.qManagerLock.Unlock()
	var inJob, outJob *boltJob
	var e *list.Element
	for {
		select {
		case inJob = <-bw.pushWorkChan:
			bw.jobQueue.PushBack(inJob)
		case inJob = <-bw.updateDBChan:
			err := bw.DB.Update(inJob)
			if err != nil {
				// TODO Should we panic or re-attempt DB update?
				bw.Logger.Errorf("unable to update DB for job %s, error: %s", inJob, err)
			}
			if inJob.Status.HasAll(JobDone) {
				continue
			}
			if inJob.Status.HasAny(JobFailed) {
				continue
			}
			if inJob.Status.HasAny(JobPending, JobReAttempt) {
				bw.perform(inJob)
			}
		case bw.getWorkChan <- outJob:
			if e != nil && outJob != nil {
				bw.jobQueue.Remove(e)
				outJob = nil
				e = nil
			}
		case <-bw.ctx.Done():
			bw.cancel()
			bw.Logger.Info("queue manager stopping")
			return
		case <-time.After(bw.PollDBTime):
			// TODO sync jobQueue from DB.
			bw.SyncWithDB()
		default:
			if e != nil {
				// None of the workers picked up our job, give them some time
				time.Sleep(500 * time.Millisecond)
				bw.Logger.Debug("All workers are busy...")
				continue
			}
			e = bw.jobQueue.Front()
			if e != nil {
				outJob = e.Value.(*boltJob)
				bw.Logger.Debugf("got job %s from queue", outJob)
				if !outJob.WorkAT.IsZero() && outJob.WorkAT.Sub(time.Now()) > 0 {
					bw.jobQueue.MoveToBack(e)
					outJob = nil
					e = nil
				}
			} else {
				bw.Logger.Debug("Job queue is empty... Wait for some time")
				time.Sleep(500 * time.Millisecond)
			}
		}
	}
}

// LoadPendingJobs loads all the pending jobs from the boltDB to the Job Queue
func (bw *BoltWorker) LoadPendingJobs() {
	jobList, err := bw.DB.GetPendingJobs()
	if err != nil {
		bw.Logger.Error(err)
		bw.cancel()
	}
	for _, job := range jobList {
		bw.Logger.Debugf("loading job %s from DB", job.Name)
		err = bw.perform(job)
		if err != nil {
			bw.Logger.Errorf("error loading job %s: %s", job.Name, err)
		}
	}

}

// SyncWithDB syncs the jobQueue with DB, not threadsafe needs to be called within a mutex Lock
func (bw *BoltWorker) SyncWithDB() error {
	//TODO Not yet implemented
	return nil
}

// SpawnWorkers creates concurrent worker goroutines based on the MaxConcurrency opt provided
func (bw *BoltWorker) SpawnWorkers() {
	bw.Logger.Infof("Spawning %d workers", bw.Concurrency)
	for i := 1; i <= bw.Concurrency; i++ {
		go bw.startWork(i)
	}
}

// startWork is the worker
func (bw *BoltWorker) startWork(worker int) {
	bw.Logger.Infof("starting worker %d", worker)
	var job *boltJob
	for {
		select {
		case job = <-bw.getWorkChan:
			if job == nil {
				//bw.Logger.Debugf("worker %d: nil job received...", worker)
				continue
			}
			bw.Logger.Infof("worker %d: got job %s", worker, job.Name)
			// TODO Fix Possible race condition with JobInProcess
			if job.Status.HasAny(JobPending) && !job.Status.HasAny(JobInProcess) {
				bw.handlerLock.RLock()
				handler, ok := bw.handlers[job.Handler]
				bw.handlerLock.RUnlock()
				if ok {
					err := safe.RunE(func() error {
						job.Status = job.Status | JobInProcess
						defer job.Status.Remove(JobInProcess)
						return handler(job.Args)
					})
					if err != nil {
						if errRetry, ok := err.(RetryJobError); ok {
							job.Status = job.Status | JobReAttempt
							bw.Logger.Infof("worker %d: job %s failed temporarily with error %s, attempts: %d, retrying...", job.Name, job.Attempt, err)
							job.WorkAT = time.Now().Add(errRetry.retryIN)
							job.Attempt++
						} else {
							job.Status = job.Status | JobFailed
						}
					} else {
						job.Status = JobDone
						bw.Logger.Debugf("worker %d: completed job %s", job)
					}
					bw.updateDBChan <- job
				} else {
					bw.Logger.Errorf("worker %d: Handler for %s not found, ignoring job", worker, job.Handler)
					continue
				}
			} else {
				bw.Logger.Debugf("worker %d: Job %s with wrong flag received, ignoring", worker, job)
			}
		case <-bw.ctx.Done():
			bw.cancel()
			bw.Logger.Infof("worker %d stopping", worker)
			return
		}
	}
}

// perform sends the job to the queueManager
func (bw *BoltWorker) perform(job *boltJob) error {
	bw.Logger.Debugf("performing job: %s", job)
	if job.Handler == "" {
		err := fmt.Errorf("no handler associated with job %s", job)
		bw.Logger.Error(err)
		return err
	}
	// TODO check for re-attempts
	go func() {
		if job.WorkAT.IsZero() || job.WorkAT.Sub(time.Now()) <= 0 {
			bw.pushWorkChan <- job
		} else {
			select {
			case <-time.After(time.Until(job.WorkAT)):
				bw.pushWorkChan <- job
			case <-bw.ctx.Done():
				bw.cancel()
			}
		}
	}()
	return nil
}

// Stop boltWorker
func (bw *BoltWorker) Stop() error {
	bw.Logger.Info("Stopping boltWorker")
	bw.cancel()
	return nil
}

// Perform a job, the job is first saved to boltDB and then performed
func (bw *BoltWorker) Perform(job worker.Job) error {
	bJob := getBoltJob(job)
	bw.updateDBChan <- bJob
	return nil
}

// PerformIn performs a job after waiting for a specified time, the job is
// first saved to boltDB
func (bw *BoltWorker) PerformIn(job worker.Job, d time.Duration) error {
	bJob := getBoltJob(job)
	bJob.WorkAT = time.Now().Add(d)
	bw.updateDBChan <- bJob
	return nil
}

// PerformAt perfirms a job at a particular time, the job is first saved to boltDB
func (bw *BoltWorker) PerformAt(job worker.Job, t time.Time) error {
	bJob := getBoltJob(job)
	bJob.WorkAT = t
	bw.updateDBChan <- bJob
	return nil
}
