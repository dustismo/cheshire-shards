package router

// What the router needs to do:
// Listen on ports
// Shard on partition key
// forward request to proper server.
// Allow for overriding of processing for specific routes
// Manage the router table


// Handles matching the request to the 
// appropriate service router
// This basically overrides a bunch of standard stuff in the 
// goshire stack, the uri matcher, and controllers.
type Matcher struct { 
    config *goshire.ControllerConfig
    services map[string]*Router 
}
func NewMatcher *Matcher {
    //create the config

    m := &Matcher{
        config : NewControllerConfig("/"),
        services : make(map[string]*Router),
    }
    return m
}

// We return our special controller here.
func (this *Matcher) Match(route string, method string) Controller {
    return this
}

// Necessary to implement the RouteMatcher interface
func (this *Matcher) Register([]string, Controller) {
    //do nothing
    log.Println("Register called on Router Matcher. Invalid!")
}

// Necessary for the Controller interface
func (this *Matcher) Config() *ControllerConfig {
    return this.config
}

// This is our special controller.
func HandleRequest(txn *goshire.Txn) {
    tmp := strings.Split(txn.Request.Uri(), "/", 3)
    if len(tmp) != 3 {
        //ack!
        goshire.SendError(txn, 406, "Uri should be in the form /{service}/{uri}")
        return
    }
    service := tmp[1]
    //set the proper uri
    txn.Request.SetUri(fmt.Sprintf("/%s", tmp[2]))

    router, ok := this.services[service]
    if !ok {
        goshire.SendError(txn, 404, fmt.Sprintf("No service with name %s found", service))
        return   
    }

    router.doReq(txn)
}



// The router
// Handles matching the route
// Handles forwarding the requests to the target
type Router struct {
    connections *shards.Connections   
}

// Do the request
func (this *Router) doReq(txn *goshire.Txn) {

    keyParams := this.connections.RouterTable().PartitionKeys

    partitionFound := false

    vals := make([]string, len(keyParams))
    for i, k := range(keyParams) {
        vals[i], ok := txn.Request.Params().GetString(k)
        if ok {
            partitionFound = true
        }
    }

    if !partitionFound {
        //uhh, return error, or broadcast?

    }

    partitionKey = strings.Join(vals, "|")

    //Now partition
    partition = P.Partition(partitionKey)

    //Add the required params
    txn.Request.Params().Put(shards.P_PARTITION, partition)
    txn.Request.Params().Put(shards.P_ROUTER_TABLE_V, this.connections.RouterTable().Revision)


    //get the connections and send.
    entries := this.connections.Entries(partition)

    //TODO: Queue here for overloading?
    for _, entry := range(clients) {
        c, err := entry.Client()
        if err != nil {
            log.Println("ERR %s", err)
            //TODO: Add to retry queue
            continue
        }

        c.ApiCall(txn.Request)
    }

}


