# boltWorker
A Simple boltDB based Worker which can be used with gobuffalo's background worker interface
Suitable for managing background tasks for single standalone binaries.

Works with grift tasks of your buffalo app
```go
w := actions.App().Worker
w.Perform(worker.Job{
	Queue:   "default",
	Handler: "send_mail",
	Args: worker.Args{
		"user_id":  123,
	},
})
```
The task will be performed by the background workers spawned by your buffalo App server

## Setup
Default boltWorker can be configured by providing a DB filepath
```go
import "github.com/astutic/boltWorker"

// ...
buffalo.New(buffalo.Options {
	// ...
	Worker: boltworker.NewBoltWorker(boltworker.Options{
		FilePath:       "worker.db",
		MaxConcurrency: 10,
	}),
	// ...
})
```

## boltWorker Options
The following options can be configured for boltWorker

### **FilePath**         ```string```
```
FilePath is where the boltDB will be created/accessed. Must Have parameter.
```

### **BoltOptions**      ```bolt.Options```
```
Boltoptions are options which can be passed for boltDB
```

### **Logger**           ```Logger```
```
Logger if passed will be used for logging, else a logrus TextFormatter with InfoLevel will be used
```

### **Name**             ```string```
```
Name is for future use if any. Default value is 'buffalo'
```

###	**MaxConcurrency**   ```int```
```
MaxConcurrency determines how many workers will be spawned. Default is 10.
```

###	**CompletedBucket**  ```string```
```
Name of the bucket where completed jobs will be saved. Default 'completed'.
```

###	**PendingBucket**    ```string```
```
Name of the bucket where jobs which are incomplete will be saved. Default 'pending'.
```

###	**FailedBucket**     ```string```
```
Name of the bucket where jobs which fail will be saved. Default 'failed'.
```

###	**PollDBTime**       ```string```
```
A time.Duration string used as in interval to sync boltDB to read jobs from the DB. Default '30s'. 
```

###	**MaxRetryAttempts** ```int```
```
Number of attempts for jobs which return RetryJobError after which the job will be declared failed. Default 10.
```

###	**IdleSleepTime**    ```string```
```
A time.Duration string which determines the sleep time when there are no jobs in the Queue. Default '5s'
```

###	**JobNameHandler**   ```JobNameGenerator```
```go
A function of type ***func(worker.Job) string** which will be called to set the key of the job.
Default is UUID version 4 generator

type JobNameGenerator func(worker.Job) string

func DefaultJobNameGenerator(job worker.Job) string {
	return uuid.Must(uuid.NewV4()).String()
}
```

An example
```go
import "github.com/gobuffalo/uuid"

jn := func(job worker.Job) string {
	return job.Args.String()
}

app = buffalo.New(buffalo.Options{
	// ...
	Worker: boltworker.NewBoltWorker(boltworker.Options{
		FilePath:       "worker.db",
		MaxConcurrency: 5,
		JobNameHandler: jn,
	}),
	//...
})
```

## Disclaimer
- Not developed for performance.
- Not optimized for memory, Holds the entire DB in memory.
- Not suitable for large and/or distributed workloads.
- No Live updates to the boltDB about the working status, the DB is only updated after the job is performed.

