package boltworker

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/boltdb/bolt"

	"github.com/sirupsen/logrus"

	"github.com/gobuffalo/buffalo/worker"
	"github.com/stretchr/testify/require"
)

/* TODO: test cases which needs to be covered.
1. Configuration and DB creation
2. Creation of all buckets
3. Registering of handle
4. Starting of boltWorker
5. Workers and concurrency
6. Performing job
7. Job is DB
8. PerformIN and job in DB
9. PerformAT and job in DB
10. Loading of jobs from DB
11. PerformAT after loading from DB
12. Job retries
13. Job success and completed bucket
14. Job Fail and failed bucket
15. JobINProcess flag test
16. Stop and all jobs in DB
*/

var bw *BoltWorker
var baseDir string

func TestMain(m *testing.M) {
	var err error
	baseDir, err = ioutil.TempDir("", "testboltWorker_")
	if err != nil {
		panic(err)
	}
	defer os.Remove(baseDir)
	bw = NewBoltWorker(Options{
		FilePath:       filepath.Join(baseDir, "test_bw.db"),
		MaxConcurrency: 5,
		IdleSleepTime:  "100ms",
		DBSyncInterval: "1s",
	})

	logger := logrus.New()
	logger.Formatter = &logrus.TextFormatter{}
	logger.Level = logrus.DebugLevel

	ctx, cancel := context.WithCancel(context.Background())
	ctx, cancel = context.WithTimeout(ctx, 200*time.Second)
	defer cancel()

	bw.Logger = logger

	go func() {
		select {
		case <-ctx.Done():
			cancel()
			log.Fatal(ctx.Err())
		}
	}()

	fmt.Println("Starting Main boltWorker")
	err = bw.Start(ctx)
	if err != nil {
		cancel()
		log.Fatal(ctx.Err())
	}

	code := m.Run()
	fmt.Println("CODE: ", code)

	fmt.Println("Stopping Main boltWorker")

	err = bw.Stop()
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Removing files from MAIN")
	os.Remove(filepath.Join(baseDir, "test_bw.db"))
	os.Exit(code)
}

func TestPerform(t *testing.T) {
	t.Log("Begining TestPerform")
	r := require.New(t)

	var hit bool
	wg := &sync.WaitGroup{}
	wg.Add(1)
	t.Log("calling register")
	bw.Register("performTest", func(worker.Args) error {
		hit = true
		fmt.Println("Calling wg Done inside handler performTest")
		defer wg.Done()
		return nil
	})
	t.Log("calling Perform")
	err := bw.Perform(worker.Job{
		Handler: "performTest",
	})
	fmt.Printf("Before Wait in TestPerform: %v", err)
	wg.Wait()
	fmt.Println("After Wait in TestPerform")
	r.True(hit)
}

func TestPerformAtIn(t *testing.T) {
	t.Log("Begining TestPerformAt")
	r := require.New(t)

	var hitTime time.Time
	timeGap := 500 * time.Millisecond
	wg := &sync.WaitGroup{}
	wg.Add(1)
	bw.Register("performATInTest", func(worker.Args) error {
		hitTime = time.Now()
		defer wg.Done()
		t.Log("handler performATInTest done...")
		return nil
	})
	startTime := time.Now()
	bw.PerformAt(worker.Job{
		Handler: "performATInTest",
	}, startTime.Add(timeGap))
	wg.Wait()
	r.Condition(func() bool {
		t.Log("Time comparision: ", hitTime.Sub(startTime), timeGap)
		if hitTime.Sub(startTime) >= timeGap {
			return true
		}
		return false
	})

	wg = &sync.WaitGroup{}
	wg.Add(1)
	startTime = time.Now()
	bw.PerformIn(worker.Job{
		Handler: "performATInTest",
	}, timeGap)
	wg.Wait()
	r.Condition(func() bool {
		t.Log("Time comparision: ", hitTime.Sub(startTime), timeGap)
		if hitTime.Sub(startTime) >= timeGap {
			return true
		}
		return false
	})
}

func jobNamefromHandler(job worker.Job) string {
	return job.Handler
}

func TestConfig(t *testing.T) {
	defer os.Remove(filepath.Join(baseDir, "test_bw1.db"))
	t.Log("Beginning TestConfig")

	var bw1 = NewBoltWorker(Options{
		FilePath:       filepath.Join(baseDir, "test_bw1.db"),
		MaxConcurrency: 1,
		JobNameHandler: jobNamefromHandler,
		IdleSleepTime:  "1ms",
		DBSyncInterval: "10ms",
	})

	bwTests := make([]*BoltWorker, 0)
	bwTests = append(bwTests, bw1)

	ctx, cancel := context.WithCancel(context.Background())

	// Setting up and starting boltWorkers
	wgStart := &sync.WaitGroup{}

	go func() {
		select {
		case <-ctx.Done():
			cancel()
			log.Fatal(ctx.Err())
		}
	}()

	for i, bwt := range bwTests {
		wgStart.Add(1)
		go func(bwt *BoltWorker, i int) {
			fmt.Println("Starting boltWorker ", i)
			t.Logf("TestConfig: starting boltWorker %d", i)
			err := bwt.Start(ctx)
			if err != nil {
				cancel()
				log.Fatal(ctx.Err())
			}
			wgStart.Done()
		}(bwt, i)
	}

	wgStart.Wait()

	// Test functions

	testDBCreation := func(t *testing.T) {
		//t.Skip()
		r := require.New(t)
		r.FileExists(filepath.Join(baseDir, "test_bw1.db"))
	}

	testBucketCreation := func(t *testing.T) {
		//t.Skip()
		r := require.New(t)
		db, err := bolt.Open(filepath.Join(baseDir, "test_bw1.db"), 0600, nil)
		defer db.Close()
		r.Empty(err)
		db.View(func(tx *bolt.Tx) error {
			cb := tx.Bucket(bw1.DB.completedBucket)
			r.NotEmpty(cb)
			pb := tx.Bucket(bw1.DB.pendingBucket)
			r.NotEmpty(pb)
			fb := tx.Bucket(bw1.DB.failedBucket)
			r.NotEmpty(fb)
			return nil
		})
	}

	testHandleRegistration := func(t *testing.T) {
		//t.Skip()
		r := require.New(t)
		handleError := fmt.Errorf("test handler error")
		testHandle := func(worker.Args) error {
			return handleError
		}
		err := bw1.Register("testHandle", testHandle)
		r.Empty(err)
		handle, ok := bw1.handlers["testHandle"]
		r.True(ok)
		r.EqualValues(handle(nil), testHandle(nil))
		err = bw1.Register("testHandle", testHandle)
		r.NotEmpty(err)
		r.EqualError(err, "handler already exists for testHandle")

	}

	testJobInDB := func(t *testing.T) {
		//t.Skip()
		r := require.New(t)

		// Test empty handler error
		job := getBoltJob(worker.Job{
			Handler: "",
		}, jobNamefromHandler)
		err := bw1.perform(job)
		r.NotEmpty(err)

		wgJobWait := &sync.WaitGroup{}
		wgJobHit := &sync.WaitGroup{}

		// Test whether job is saved in DB before it is worked on
		// and then test when handle gives error the job is marked failed in DB
		handleError := fmt.Errorf("test handler error")
		testJobDB := func(worker.Args) error {
			t.Log("inside testJobDB")
			// Wait inside handle so that we can inspect the DB entry and its state
			wgJobHit.Done()
			fmt.Println("waiting inside job handler")
			wgJobWait.Wait()
			fmt.Println("After wait, will fail now")
			t.Log("returning from testJobDB")
			return handleError
		}

		err = bw1.Register("testJobDB", testJobDB)
		r.Empty(err)

		wgJobWait.Add(1)
		wgJobHit.Add(1)
		t.Log("calling perform testJobDB")
		bw1.Perform(worker.Job{
			Handler: "testJobDB",
		})

		wgJobHit.Wait()

		// Inspect the DB when the worker is waiting
		fmt.Println("opening the DB for checking JOB entry", filepath.Join(baseDir, "test_bw1.db"))
		t.Log("opening the DB for checking entries..")

		/*
			newDB, _ := os.Create(filepath.Join(baseDir, "test_bw1copy.db"))
			inDB, _ := os.Open(filepath.Join(baseDir, "test_bw1.db"))
			io.Copy(newDB, inDB)
			inDB.Close()
			newDB.Close()

			db, err := bolt.Open(filepath.Join(baseDir, "test_bw1copy.db"), 0600, &bolt.Options{
		*/
		db, err := bolt.Open(filepath.Join(baseDir, "test_bw1.db"), 0600, &bolt.Options{
			ReadOnly: true,
		})
		r.Empty(err)

		var fetchedJob boltJob
		dbValue := make([]byte, 0)
		err = db.View(func(tx *bolt.Tx) error {
			pb := tx.Bucket(bw1.DB.pendingBucket)
			if pb == nil {
				return fmt.Errorf("bucket not found")
			}
			val := pb.Get([]byte("testJobDB"))
			dbValue = append(dbValue, val...)
			t.Logf("got val from pending bucket: %v", string(dbValue))
			return nil
		})
		db.Close()
		err = json.Unmarshal(dbValue, &fetchedJob)
		if err != nil {
			t.Logf("json unmarshal err: %v", err)
			fmt.Println("json unmarshal err", err)
		}
		r.Empty(err)

		r.Exactly(fetchedJob.Status, JobPending)
		wgJobWait.Done()

		// Spawn another testJobDB job so when it waits we can check the status of previous job
		wgJobWait = &sync.WaitGroup{}
		wgJobHit = &sync.WaitGroup{}
		wgJobWait.Add(1)
		wgJobHit.Add(1)
		dbValue = make([]byte, 0)
		t.Log("calling perform testJobDB")
		bw1.Perform(worker.Job{
			Handler: "testJobDB",
		})

		wgJobHit.Wait()
		db, err = bolt.Open(filepath.Join(baseDir, "test_bw1.db"), 0600, &bolt.Options{
			ReadOnly: true,
		})
		err = db.View(func(tx *bolt.Tx) error {
			fb := tx.Bucket(bw1.DB.failedBucket)
			val := fb.Get([]byte("testJobDB"))
			dbValue = append(dbValue, val...)
			t.Logf("got val from failed bucket: %v", string(dbValue))
			return nil
		})
		db.Close()
		err = json.Unmarshal(dbValue, &fetchedJob)
		if err != nil {
			t.Logf("json unmarshal err: %v", err)
			fmt.Printf("json unmarshal err: %v", err)
		}
		r.Empty(err)
		r.True(fetchedJob.Status.HasAny(JobFailed))
		wgJobWait.Done()
	}

	testRetryJobInDB := func(t *testing.T) {
		r := require.New(t)
		wgRetry := &sync.WaitGroup{}
		retryError := NewRetryJobError("test retry error")
		retryAttempts := 0
		testRetryJob := func(worker.Args) error {
			t.Log("inside testRetryJob")
			retryAttempts++
			fmt.Println("RetryAttempts", retryAttempts)
			defer wgRetry.Done()
			return retryError
		}

		bw1.Register("testRetryJob", testRetryJob)
		t.Log("calling perform testRetryJob")
		wgRetry.Add(bw1.RetryAttempts)
		bw1.Perform(worker.Job{
			Handler: "testRetryJob",
		})
		wgRetry.Wait()

		r.Condition(func() bool {
			if retryAttempts == bw1.RetryAttempts {
				return true
			}
			return false
		})

		wgRetryIN := &sync.WaitGroup{}
		retryInError := NewRetryJobError("retryIN error")
		errorGap := 10 * time.Millisecond
		retryInError.SetRetryTime(errorGap)
		retryINAttempts := 0
		retryLock := &sync.Mutex{}
		var retryINFirstTime, retryINErrorTime time.Time
		testRetryINJob := func(worker.Args) error {
			retryLock.Lock()
			defer retryLock.Unlock()
			if retryINAttempts <= 0 {
				retryINAttempts++
				retryINFirstTime = time.Now()
				return retryInError
			}
			retryINErrorTime = time.Now()
			defer wgRetryIN.Done()
			return nil
		}
		bw1.Register("testRetryINJob", testRetryINJob)
		wgRetryIN.Add(1)
		bw1.Perform(worker.Job{
			Handler: "testRetryINJob",
		})

		wgRetryIN.Wait()

		retryLock.Lock()
		r.Condition(func() bool {
			t.Logf("retryIN First: %v Next: %v Sub: %s errorGap: %s", retryINFirstTime, retryINErrorTime, retryINErrorTime.Sub(retryINFirstTime), errorGap)
			if retryINErrorTime.Sub(retryINFirstTime) >= errorGap {
				return true
			}
			return false
		})
		retryLock.Unlock()
	}

	testWorkerBusy := func(t *testing.T) {
		r := require.New(t)

		workerTimeLogs := make([]time.Time, 0)
		wg := &sync.WaitGroup{}
		testWorkerBusyJob := func(worker.Args) error {
			t.Log("inside testWorkerBusyJob")
			workerTimeLogs = append(workerTimeLogs, time.Now())
			time.Sleep(10 * time.Millisecond)
			defer wg.Done()
			return nil
		}

		bw1.Register("testWorkerBusy1", testWorkerBusyJob)
		bw1.Register("testWorkerBusy2", testWorkerBusyJob)
		bw1.Register("testWorkerBusy3", testWorkerBusyJob)
		wg.Add(3)
		bw1.Perform(worker.Job{
			Handler: "testWorkerBusy1",
		})
		bw1.Perform(worker.Job{
			Handler: "testWorkerBusy2",
		})
		bw1.Perform(worker.Job{
			Handler: "testWorkerBusy3",
		})
		wg.Wait()

		r.Condition(func() bool {
			t.Logf("worker time Logs :%v", workerTimeLogs)
			if len(workerTimeLogs) != 3 {
				t.Log("worker time logs length is not 3")
				return false
			}
			timeInterval1 := workerTimeLogs[1].Sub(workerTimeLogs[0])
			timeInterval2 := workerTimeLogs[2].Sub(workerTimeLogs[1])
			t.Logf("time intervals 1: %s, 2: %s", timeInterval1, timeInterval2)
			if timeInterval1 >= 500*time.Millisecond && timeInterval2 >= 500*time.Millisecond {
				return true
			}
			return false
		})

	}

	t.Run("testConfiguration", func(t *testing.T) {
		t.Run("testDBcreation", testDBCreation)
		t.Run("testBucketCreation", testBucketCreation)
		t.Run("testHandleRegistration", testHandleRegistration)
		t.Run("testJobInDB", testJobInDB)
		t.Run("testRetryJobInDB", testRetryJobInDB)
		t.Run("testWorkerBusy", testWorkerBusy)
	})

	wgStop := &sync.WaitGroup{}

	for i, bwt := range bwTests {
		wgStop.Add(1)
		func(bwt *BoltWorker, i int) {
			fmt.Println("Stopping boltWorker ", i)
			err := bwt.Stop()
			if err != nil {
				log.Fatal(ctx.Err())
			}
			wgStop.Done()
		}(bwt, i)
	}

	wgStop.Wait()
}

func TestStopped(t *testing.T) {
	defer os.Remove(filepath.Join(baseDir, "test_bw2.db"))
	logger := logrus.New()
	logger.Formatter = &logrus.TextFormatter{}
	logger.Level = logrus.DebugLevel

	var bw2 = NewBoltWorker(Options{
		FilePath:       filepath.Join(baseDir, "test_bw2.db"),
		Logger:         logger,
		MaxConcurrency: 5,
		JobNameHandler: jobNamefromHandler,
		IdleSleepTime:  "1ns",
		DBSyncInterval: "10ms",
	})

	var jobStartTime time.Time
	var timeGap time.Duration
	var retryAttempts int

	testPutJobsInStopped := func(t *testing.T) {
		r := require.New(t)

		job1 := worker.Job{
			Handler: "Completes",
		}
		job2 := worker.Job{
			Handler: "Fails",
		}
		job3 := worker.Job{
			Handler: "RetriesAndSucceeds",
		}
		job4 := worker.Job{
			Handler: "CompletesAt",
		}
		job5 := worker.Job{
			Handler: "CompletesIn",
		}
		bw2.Perform(job1)
		bw2.Perform(job2)
		bw2.Perform(job3)
		jobStartTime = time.Now()
		timeGap = 100 * time.Millisecond
		bw2.PerformAt(job4, jobStartTime.Add(timeGap))
		bw2.PerformIn(job5, timeGap)

		pendingJobs, err := bw2.DB.GetPendingJobs()
		r.Empty(err)
		r.Len(pendingJobs, 5)
	}

	testRunJobsInDB := func(t *testing.T) {
		t.Log("starting boltWorker 2")
		ctx, cancel := context.WithCancel(context.Background())
		wgRun := &sync.WaitGroup{}
		wgRun.Add(5)
		err := bw2.Start(ctx)
		if err != nil {
			fmt.Println("Cancel Called")
			cancel()
			t.Fatal(ctx.Err())
			return
		}

		go func() {
			select {
			case <-ctx.Done():
				cancel()
				log.Fatal(ctx.Err())
			}
		}()

		bw2.Register("Completes", func(worker.Args) error {
			defer wgRun.Done()
			t.Logf("Completes Done: %v", wgRun)
			return nil
		})
		bw2.Register("Fails", func(worker.Args) error {
			defer wgRun.Done()
			t.Logf("Fails Done: %v", wgRun)
			return fmt.Errorf("this is a failure")
		})
		retryError := NewRetryJobError("test retry error")
		bw2.Register("RetriesAndSucceeds", func(worker.Args) error {
			if retryAttempts < bw2.RetryAttempts-1 {
				retryAttempts++
				return retryError
			}
			defer wgRun.Done()
			t.Logf("RetriesAndSucceeds Done: %v", wgRun)
			return nil
		})
		var completedAt, completedIn time.Time
		bw2.Register("CompletesAt", func(worker.Args) error {
			completedAt = time.Now()
			defer wgRun.Done()
			t.Logf("CompletesAt Done: %v", wgRun)
			return nil
		})
		bw2.Register("CompletesIn", func(worker.Args) error {
			completedIn = time.Now()
			defer wgRun.Done()
			t.Logf("CompletesIn Done: %v", wgRun)
			return nil
		})

		wgRun.Wait()

		r := require.New(t)

		r.Condition(func() bool {
			t.Log("Time comparision: ", completedAt.Sub(jobStartTime), timeGap)
			if completedAt.Sub(jobStartTime) >= timeGap {
				return true
			}
			return false
		})

		r.Condition(func() bool {
			t.Log("Time comparision: ", completedIn.Sub(jobStartTime), timeGap)
			if completedIn.Sub(jobStartTime) >= timeGap {
				return true
			}
			return false
		})

		r.Condition(func() bool {
			t.Log("retryattempts", retryAttempts)
			if retryAttempts == bw2.RetryAttempts-1 {
				return true
			}
			return false
		})
		fmt.Println("CHECK DB")
	}

	t.Run("testStoppedBoltWorker", func(t *testing.T) {
		t.Run("testPutJobsInStopped", testPutJobsInStopped)
		t.Run("testRunJobsInDB", testRunJobsInDB)
	})

	bw2.Stop()
}
