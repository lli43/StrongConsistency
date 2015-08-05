# StrongConsistency
In a system with N data storage servers, tolerate the failure of up to N-1 nodes. As long as one data storage server is available, a client request will succeed.\r\n

A client will not receive stale data if fresher data is available. If, for example, a client POSTs new data and then performs a GET, the response the client receives must include the most recent POST unless all data storage servers storing the newest data have failed.

Elect a new primary from the remaining secondary replicas. I implement Bully Algorithm to elect a new leader.

Resolve any inconsistencies.  If the primary was in the process of processing a POST when it failed, the new data will be replicated on all secondaries.

Inform the front ends of the IP address of the new replica.  
