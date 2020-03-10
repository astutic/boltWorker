package boltworker

import (
	"bytes"
	"encoding/json"
	"fmt"

	"github.com/boltdb/bolt"
)

type boltDB struct {
	fileName        string
	boltOptions     bolt.Options
	db              *bolt.DB
	completedBucket []byte
	pendingBucket   []byte
	failedBucket    []byte
}

// NewBoltDB creates a new instance for boltDB used for boltWorker
func NewBoltDB(bo *Options) *boltDB {
	b := new(boltDB)
	b.fileName = bo.FilePath
	b.boltOptions = bo.BoltOptions

	b.completedBucket = []byte(bo.CompletedBucket)
	b.pendingBucket = []byte(bo.PendingBucket)
	b.failedBucket = []byte(bo.FailedBucket)

	return b
}

func (b *boltDB) Init() error {
	err := b.open()
	if err != nil {
		return err
	}
	defer b.close()

	// Create necessary buckets if it does not exist
	return b.db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(b.pendingBucket)
		if err != nil {
			return fmt.Errorf("creating bucket %s failed with error: %s", string(b.pendingBucket), err)
		}
		_, err = tx.CreateBucketIfNotExists(b.completedBucket)
		if err != nil {
			return fmt.Errorf("creating bucket %s failed with error: %s", string(b.completedBucket), err)
		}
		_, err = tx.CreateBucketIfNotExists(b.failedBucket)
		if err != nil {
			return fmt.Errorf("creating bucket %s failed with error: %s", string(b.failedBucket), err)
		}
		return nil
	})
}

// Update updates the given job in boltDB
func (b *boltDB) Update(job *boltJob) error {
	err := b.open()
	if err != nil {
		return err
	}
	defer b.close()

	if job.Status.HasAll(JobDone) {
		err = b._moveFromPendingTo(job, b.completedBucket)
	} else if job.Status.HasAny(JobFailed) {
		err = b._moveFromPendingTo(job, b.failedBucket)
	} else if job.Status.HasAny(JobPending, JobReAttempt) {
		err = b._updatePending(job)
	} else {
		err = fmt.Errorf("job with un-updatable status received: %s", job)
	}
	return err
}

// GetPendingJobs gets all the jobs in the pending bucket
func (b *boltDB) GetPendingJobs() ([]*boltJob, error) {
	jobList := make([]*boltJob, 0)

	err := b.open()
	if err != nil {
		return jobList, err
	}
	defer b.close()

	err = b.forEach(b.pendingBucket, func(k, v []byte) error {
		job := new(boltJob)
		err := json.Unmarshal(v, job)
		if err != nil {
			return err
		}
		jobList = append(jobList, job)
		return nil
	})
	return jobList, err
}

func (b *boltDB) open() error {
	var err error
	b.db, err = bolt.Open(b.fileName, 0600, &b.boltOptions)
	return err
}

func (b *boltDB) close() error {
	return b.db.Close()
}

func (b *boltDB) _moveFromPendingTo(job *boltJob, toBucket []byte) error {
	if !bytes.Equal(toBucket, b.completedBucket) && !bytes.Equal(toBucket, b.failedBucket) {
		return fmt.Errorf("toBucket argument can only contain either of completedBucket or failedBucket")
	}
	return b.db.Update(func(tx *bolt.Tx) error {
		pB := tx.Bucket(b.pendingBucket)
		if pB == nil {
			return fmt.Errorf("bucket: %s not found", string(b.pendingBucket))
		}
		err := pB.Delete([]byte(job.Name))
		if err != nil {
			return err
		}
		tB := tx.Bucket(toBucket)
		if tB == nil {
			return fmt.Errorf("bucket: %s not found", string(toBucket))
		}
		err = tB.Put([]byte(job.Name), []byte(job.String()))
		if err != nil {
			return err
		}
		return nil
	})
}

func (b *boltDB) _updatePending(job *boltJob) error {
	return b.db.Update(func(tx *bolt.Tx) error {
		pB := tx.Bucket(b.pendingBucket)
		err := pB.Put([]byte(job.Name), []byte(job.String()))
		return err
	})
}

func (b *boltDB) forEach(bucket []byte, fn func(k, v []byte) error) error {
	return b.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucket)
		if b != nil {
			return b.ForEach(fn)
		}
		return fmt.Errorf("%s bucket not found", string(bucket))
	})
}
