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

	"github.com/gobuffalo/uuid"

	"github.com/markbates/safe"

	"github.com/gobuffalo/x/defaults"
	"github.com/sirupsen/logrus"

	"github.com/gobuffalo/buffalo/worker"
)

// Ensures the Worker Interface is implemented correctly
var _ worker.Worker = &BoltWorker{}

// BoltWorker is a basic implementation of buffalo worker interface
// which persists job data in boltDB
type BoltWorker struct {
	Logger              Logger
	ctx                 context.Context
	cancel              context.CancelFunc
	handlers            map[string]worker.Handler
	handlerLock         *sync.RWMutex
	DB                  *boltDB
	DBSyncInterval      time.Duration
	IdleSleepTime       time.Duration
	BusyWorkerSleepTime time.Duration
	Concurrency         int
	getWorkChan         chan *boltJob
	pushWorkChan        chan *boltJob
	updateDBChan        chan *boltJob
	jobQueue            *list.List
	qManagerLock        *sync.Mutex
	jobsInWorkers       map[string]bool
	qLock               *sync.Mutex
	qStop               chan bool
	isQManagerRunning   bool
	RetryAttempts       int
	jobNameHandler      JobNameGenerator
}

// NewBoltWorker creates a buffalo worker interface implementation which
// persists data in boltDB defined in the opts
func NewBoltWorker(opts Options) *BoltWorker {
	return NewBoltWorkerWithContext(context.Background(), opts)
}

// DefaultJobNameGenerator is the default job name generator which
// assigns a uuid version 4 id to the job
func DefaultJobNameGenerator(job worker.Job) string {
	return uuid.Must(uuid.NewV4()).String()
}

// NewBoltWorkerWithContext creates a buffalo worker interface implementation
// which persists data in boltDB defined in opts
func NewBoltWorkerWithContext(ctx context.Context, opts Options) *BoltWorker {
	ctx, cancel := context.WithCancel(ctx)

	if opts.FilePath == "" {
		panic(fmt.Errorf("FilePath is required"))
	}
	// Set defaults if not provided in options
	opts.Name = defaults.String(opts.Name, "buffalo")
	opts.MaxConcurrency = defaults.Int(opts.MaxConcurrency, 10)
	opts.CompletedBucket = defaults.String(opts.CompletedBucket, "completed")
	opts.PendingBucket = defaults.String(opts.PendingBucket, "pending")
	opts.FailedBucket = defaults.String(opts.FailedBucket, "failed")
	opts.DBSyncInterval = defaults.String(opts.DBSyncInterval, "30s")
	opts.IdleSleepTime = defaults.String(opts.IdleSleepTime, "5s")
	opts.MaxRetryAttempts = defaults.Int(opts.MaxRetryAttempts, 10)

	if opts.JobNameHandler == nil {
		opts.JobNameHandler = DefaultJobNameGenerator
	}

	if opts.Logger == nil {
		logger := logrus.New()
		logger.Formatter = &logrus.TextFormatter{}
		logger.Level = logrus.InfoLevel
		opts.Logger = logger
	}

	bDB := NewBoltDB(&opts)
	pollTime, err := time.ParseDuration(opts.DBSyncInterval)
	if err != nil {
		panic(err)
	}
	idleTime, err := time.ParseDuration(opts.IdleSleepTime)
	if err != nil {
		panic(err)
	}

	if pollTime <= idleTime {
		panic(fmt.Errorf("DBSyncInterval is configured less than IdleSleepTime," +
			"please have a higher value for DBSyncInterval"))
	}

	return &BoltWorker{
		Logger:         opts.Logger,
		ctx:            ctx,
		cancel:         cancel,
		handlers:       map[string]worker.Handler{},
		handlerLock:    &sync.RWMutex{},
		DB:             bDB,
		Concurrency:    opts.MaxConcurrency,
		qManagerLock:   &sync.Mutex{},
		DBSyncInterval: pollTime,
		IdleSleepTime:  idleTime,
		RetryAttempts:  opts.MaxRetryAttempts,
		jobNameHandler: opts.JobNameHandler,
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
	bw.qLock = &sync.Mutex{}
	bw.jobsInWorkers = make(map[string]bool)
	bw.getWorkChan = make(chan *boltJob)
	bw.pushWorkChan = make(chan *boltJob)
	bw.updateDBChan = make(chan *boltJob)
	bw.qStop = make(chan bool)
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

// queueManager should always be a single thread which is responsible
// for interfacing with boltDB.
// Do not directly access the DB when queueManager is running
func (bw *BoltWorker) queueManager() {
	bw.qManagerLock.Lock()
	bw.Logger.Info("starting queue manager")
	bw.isQManagerRunning = true
	defer bw.qManagerLock.Unlock()
	defer func() {
		bw.isQManagerRunning = false
	}()
	var inJob, outJob *boltJob
	var e *list.Element
	dbSync := time.After(bw.DBSyncInterval)
	// emptyChan is just an empty channel to prevent selection of getWorkChan when queue is empty
	emptyChan := make(chan *boltJob)
	// outJobChan points to either empty or getWorkChan depending on whether outJob is nil
	outJobChan := emptyChan
	for {
		select {
		case inJob = <-bw.pushWorkChan:
			bw.Logger.Debugf("Pushing Job %s to back of queue", inJob.Name)
			bw.checkAndPushQ(inJob)
		case inJob = <-bw.updateDBChan:
			bw.qLock.Lock()
			err := bw.DB.Update(inJob)
			if err != nil {
				// TODO Should we panic or re-attempt DB update?
				bw.Logger.Errorf("unable to update DB for job %s, error: %s", inJob, err)
			}
			delete(bw.jobsInWorkers, inJob.Name)
			bw.qLock.Unlock()
			if inJob.Status.HasAll(JobDone) {
				bw.removeFromQ(inJob)
				continue
			}
			if inJob.Status.HasAny(JobFailed) {
				bw.removeFromQ(inJob)
				continue
			}
			if inJob.Status.HasAny(JobPending, JobReAttempt) {
				//bw.perform(inJob)
				// we directly push to the queue
				bw.checkAndPushQ(inJob)
			}
		case outJobChan <- outJob:
			if e != nil && outJob != nil {
				bw.qLock.Lock()
				bw.jobQueue.Remove(e)
				bw.jobsInWorkers[outJob.Name] = true
				bw.qLock.Unlock()
				outJob = nil
				e = nil
				outJobChan = emptyChan
			}
		case <-bw.qStop:
			bw.Logger.Info("queue manager stopping")
			bw.qStop <- true
			return
		case <-bw.ctx.Done():
			bw.cancel()
			bw.Logger.Info("queue manager stopping")
			return
		case <-dbSync:
			bw.SyncWithDB()
			dbSync = time.After(bw.DBSyncInterval)
		default:
			if e != nil {
				// None of the workers picked up our job, give them some time
				time.Sleep(500 * time.Millisecond)
				bw.Logger.Debug("All workers are busy...")
				continue
			}
			bw.qLock.Lock()
			e = bw.jobQueue.Front()
			bw.qLock.Unlock()
			if e != nil {
				outJob = e.Value.(*boltJob)
				if outJob != nil && !outJob.WorkAT.IsZero() && outJob.WorkAT.Sub(time.Now()) > 0 {
					bw.qLock.Lock()
					bw.jobQueue.MoveToBack(e)
					bw.qLock.Unlock()
					outJob = nil
					e = nil
				}
				if outJob != nil {
					bw.Logger.Debugf("got job %s from queue", outJob)
					outJobChan = bw.getWorkChan
				} else {
					outJobChan = emptyChan
				}
			} else {
				outJobChan = emptyChan
				/*bw.Logger.Debugf("Job queue is empty... Sleep for IdleSleepTime configured: %s",
				bw.IdleSleepTime)
				*/
				time.Sleep(bw.IdleSleepTime)
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

// SyncWithDB syncs the jobQueue with DB,
// not threadsafe and needs to be called within a mutex Lock
// No other concurrent operations with the jobQueue should take place.
func (bw *BoltWorker) SyncWithDB() error {
	bw.Logger.Debug("Sync with DB called...")
	bw.qLock.Lock()
	jobList, err := bw.DB.GetPendingJobs()
	if err != nil {
		err = fmt.Errorf("Error in SyncWithDB, error getting pending jobs from DB: %s", err)
		bw.Logger.Error(err)
		return err
	}
	jobMap := make(map[string]bool)
	// Get jobs from queue
	for e := bw.jobQueue.Front(); e != nil; e = e.Next() {
		job := e.Value.(*boltJob)
		jobMap[job.Name] = true
	}
	// Get jobs in workers
	for job, v := range bw.jobsInWorkers {
		jobMap[job] = v
	}
	bw.qLock.Unlock()

	for _, job := range jobList {
		if _, ok := jobMap[job.Name]; ok {
			bw.Logger.Debugf("job %s already in job queue, not adding it", job.Name)
		} else {
			bw.Logger.Debugf("job %s not in job queue, adding it", job.Name)
			bw.perform(job)
		}
	}
	return nil
}

func (bw *BoltWorker) checkAndPushQ(job *boltJob) {
	var found, inProcess bool
	bw.qLock.Lock()
	defer bw.qLock.Unlock()
	for e := bw.jobQueue.Front(); e != nil; e = e.Next() {
		qJob := e.Value.(*boltJob)
		if qJob.Name == job.Name {
			bw.Logger.Debugf("checkAndPushQ: job %s already in queue", job.Name)
			found = true
			break
		}
	}
	inProcess, _ = bw.jobsInWorkers[job.Name]
	if inProcess {
		bw.Logger.Debugf("checkAndPushQ: job %s is in process", job.Name)
	}
	if !found && !inProcess {
		bw.Logger.Debugf("checkAndPushQ: job %s not in queue, adding it", job.Name)
		bw.jobQueue.PushBack(job)
	}
}

func (bw *BoltWorker) removeFromQ(job *boltJob) {
	var ele *list.Element
	bw.qLock.Lock()
	defer bw.qLock.Unlock()
	for e := bw.jobQueue.Front(); e != nil; e = e.Next() {
		qJob := e.Value.(*boltJob)
		if qJob == job {
			ele = e
			break
		}
	}
	if ele != nil {
		bw.jobQueue.Remove(ele)
	}
}

// SpawnWorkers creates concurrent worker goroutines
// based on the MaxConcurrency option provided
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
				bw.Logger.Debugf("worker %d: nil job received...", worker)
				continue
			}
			bw.Logger.Infof("worker %d: got job %s at %v", worker, job, time.Now())
			// TODO Fix Possible race condition with JobInProcess
			if job.Status.HasAny(JobPending) && !job.Status.HasAny(JobInProcess) {
				bw.handlerLock.RLock()
				handler, ok := bw.handlers[job.Handler]
				bw.handlerLock.RUnlock()
				if ok {
					err := safe.RunE(func() error {
						job.Status = job.Status | JobInProcess
						defer job.Status.Remove(JobInProcess)
						job.Attempt++
						return handler(job.Args)
					})
					if err != nil {
						if errRetry, ok := err.(RetryJobError); ok {
							job.Status = job.Status | JobReAttempt
							bw.Logger.Infof("worker %d: job %s failed temporarily with error %s,"+
								"attempts: %d, retrying...", worker, job, err, job.Attempt)
							job.WorkAT = time.Now().Add(errRetry.RetryIN)
							if job.Attempt >= bw.RetryAttempts {
								bw.Logger.Infof("worker %d: job %s exceeded max retry attempts "+
									"(%d of %d), marking the job failed.",
									worker, job.Name, job.Attempt, bw.RetryAttempts)
								job.Status = JobFailed
							}
						} else {
							job.Status = JobFailed
							bw.Logger.Debugf("worker %d: job failed with err: %s", worker, err)
						}
					} else {
						job.Status = JobDone
						bw.Logger.Debugf("worker %d: completed job %s", worker, job)
					}
					job.TimeLastWorkDone = time.Now()
					bw.updateDBChan <- job
				} else {
					bw.Logger.Errorf("worker %d: Handler for %s not found, ignoring job",
						worker, job.Handler)
					continue
				}
			} else {
				bw.Logger.Debugf("worker %d: Job %s with wrong flag received, ignoring",
					worker, job)
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
		bw.pushWorkChan <- job
		/*
			if job.WorkAT.IsZero() || job.WorkAT.Sub(time.Now()) <= 0 {
				bw.pushWorkChan <- job
			} else {
				bw.qLock.Lock()
				//bw.jobsInWorkers[job.Name] = true
				bw.qLock.Unlock()
				select {
				case <-time.After(time.Until(job.WorkAT)):
					bw.Logger.Debug("adding job to queue after time...")
					bw.pushWorkChan <- job
				case <-bw.ctx.Done():
					bw.cancel()
				}
			}
		*/
	}()
	return nil
}

// Stop boltWorker
func (bw *BoltWorker) Stop() error {
	bw.Logger.Info("Stopping boltWorker")
	bw.qStop <- true
	<-bw.qStop
	bw.cancel()
	return nil
}

// Perform a job, the job is first saved to boltDB and then performed
func (bw *BoltWorker) Perform(job worker.Job) error {
	bJob := getBoltJob(job, bw.jobNameHandler)
	if bw.isQManagerRunning {
		bw.updateDBChan <- bJob
	} else {
		err := bw.DB.Init()
		if err != nil {
			return err
		}
		return bw.DB.Update(bJob)
	}
	return nil
}

// PerformIn performs a job after waiting for a specified time, the job is
// first saved to boltDB
func (bw *BoltWorker) PerformIn(job worker.Job, d time.Duration) error {
	bJob := getBoltJob(job, bw.jobNameHandler)
	bJob.WorkAT = time.Now().Add(d)
	if bw.isQManagerRunning {
		bw.updateDBChan <- bJob
	} else {
		err := bw.DB.Init()
		if err != nil {
			return err
		}
		return bw.DB.Update(bJob)
	}
	return nil
}

// PerformAt perfirms a job at a particular time, the job is first saved to boltDB
func (bw *BoltWorker) PerformAt(job worker.Job, t time.Time) error {
	bJob := getBoltJob(job, bw.jobNameHandler)
	bJob.WorkAT = t
	if bw.isQManagerRunning {
		bw.updateDBChan <- bJob
	} else {
		err := bw.DB.Init()
		if err != nil {
			return err
		}
		return bw.DB.Update(bJob)
	}
	return nil
}
