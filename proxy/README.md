
Logic:


For each incoming connection we maintain a connection to each of the shards in the cluster.

If any connections are broken we disconnect the client

If router table changes we disconnect the client

We pass bytes from each shard to the client, we only parse the responses in order to 
check for sharding error conditions (bad router table ect).  Even those responses get passed through to the client unchanged. 
