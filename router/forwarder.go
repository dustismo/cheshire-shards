package router

// What the router needs to do:
// Listen on ports
// Shard on partition key
// forward request to proper server.
// Allow for overriding of processing for specific routes
// Manage the router table


// Handles matching the request to the 
// appropriate service router
type Matcher struct {


}


// The router
// Handles matching the route
// Handles forwarding the requests to the target
type Router struct {
    lock sync.RWMutex
    connections *shards.Connections
    
}
