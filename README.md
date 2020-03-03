# boltWorker
A Simple boltDB based Worker which can be used with gobuffalo's background worker interface
Suitable for managing background tasks for single standalone binaries.


Works with grift task of your buffalo app
From your grift tasks, to schedule a task 
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

- Not developed for performance.
- Not optimized for memory, Holds the entire DB in memory.
- Not suitable for large and/or distributed workloads.
- No Live updates to the boltDB about the working status, the DB is only updated after the job is performed.

