# boltWorker
A Simple boltDB based Worker which can be used with gobuffalo's background worker
Suitable for managing background tasks for single standalone binaries.


Not developed for performance.
Not optimized for memory, Holds the entire DB in memory.
Not suitable for large and/or distributed workloads.
No Live updates to the boltDB about the working status, the DB is only updated after the job is performed.

