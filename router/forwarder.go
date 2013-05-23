package router

import (
	"fmt"
	"github.com/trendrr/goshire-shards/shards"
	"github.com/trendrr/goshire/cheshire"
	"github.com/trendrr/goshire/client"
	"log"
	"strings"
	"time"
)

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
	config   *cheshire.ControllerConfig
	services map[string]*Router
}

func NewMatcher() *Matcher {
	//create the config

	m := &Matcher{
		config:   cheshire.NewControllerConfig("/"),
		services: make(map[string]*Router),
	}
	return m
}

// Adds a new service to this router.
// All endpoints matching /{serviceName}/ will be forwarded to that service.
// at least 1 of the seedUrls must be in service 
func (this *Matcher) AddService(seedUrls ...string) error {

	connections, err := shards.ConnectionsFromSeed(seedUrls...)
	if err != nil {
		return err
	}
	router := &Router{
		connections: connections,
	}
	this.services[connections.RouterTable().Service] = router
	return nil
}

// We return our special controller here.
func (this *Matcher) Match(route string, method string) cheshire.Controller {
	return this
}

// Necessary to implement the RouteMatcher interface
func (this *Matcher) Register([]string, cheshire.Controller) {
	//do nothing
	log.Println("Register called on Router Matcher. Invalid!")
}

// Necessary for the Controller interface
func (this *Matcher) Config() *cheshire.ControllerConfig {
	return this.config
}

// This is our special controller.
func (this *Matcher) HandleRequest(txn *cheshire.Txn) {
	tmp := strings.SplitN(txn.Request.Uri(), "/", 3)
	if len(tmp) != 3 {
		//ack!
		cheshire.SendError(txn, 406, "Uri should be in the form /{service}/{uri}")
		return
	}
	service := tmp[1]
	//set the proper uri
	txn.Request.SetUri(fmt.Sprintf("/%s", tmp[2]))

	router, ok := this.services[service]
	if !ok {
		cheshire.SendError(txn, 404, fmt.Sprintf("No service with name %s found", service))
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
func (this *Router) doReq(txn *cheshire.Txn) {

	keyParams := this.connections.RouterTable().PartitionKeys

	partitionFound := false

	vals := make([]string, len(keyParams))
	for i, k := range keyParams {
		v, ok := txn.Request.Params().GetString(k)
		if ok {
			partitionFound = true
		}
		vals[i] = v
	}

	if !partitionFound {
		//uhh, return error, or broadcast?

	}

	partitionKey := strings.Join(vals, "|")

	log.Printf("partition key %s", partitionKey)
	//Now partition
	//TODO
	partition := 0 //P.Partition(partitionKey)

	//Add the required params
	txn.Request.Params().Put(shards.P_PARTITION, partition)
	txn.Request.Params().Put(shards.P_ROUTER_TABLE_V, this.connections.RouterTable().Revision)

	queryType := txn.Request.Params().MustString(shards.P_QUERY_TYPE, "all")

	//Do the damn api call!
	max := 5
	if queryType == "all_q" {
		max = 100
	}

	a := &apiRR{
		partition: partition,
		queryType: queryType,
		responses: make(map[string]*cheshire.Response),
		txn:       txn,
		response:  cheshire.NewResponse(txn),
		count:     0,
		max:       max,
	}

	//send response.
	if len(a.responses) == 0 {
		// set the error response
		a.response.SetStatus(501, "Unable to get a single response.")
	}
	for k, r := range a.responses {
		r.Put("server_id", k)
		a.response.AddToSlice("responses", r)
	}

	//send the response.
	txn.Write(a.response)
}

// wrapper to hold a request and response
type apiRR struct {
	partition int
	queryType string
	responses map[string]*cheshire.Response
	txn       *cheshire.Txn
	response  *cheshire.Response
	count     int
	max       int
}

// Does the actual apiCall
// keeps track of all the responses in the responses map
func (this *Router) apiCall(a *apiRR) {

	if a.count >= a.max {
		log.Printf("Tried %d times, that is our limit!", a.max)
		//TODO: Send what we have
		return
	}
	a.count++

	//get the connections and send.
	entries, err := this.connections.Entries(a.partition)
	if err != nil {
		//TODO, do something here.
		log.Println(err)
	}

	//make sure this txnId is unique to each connection. 
	a.txn.Request.SetTxnId(fmt.Sprintf("%d", client.NewTxnId()))

	//Fuck it, just do the calls serially, this makes life soo much
	//easier then trying to do them in parallel.  
	//
	for _, entry := range entries {
		c, err := entry.Client()
		if err != nil {
			log.Println("ERR %s", err)
			//TODO: Add to retry queue
			continue
		}
		//check if we have already run this id elsewhere
		if _, ok := a.responses[entry.Entry.Id()]; ok {
			continue
		}

		resp, err := c.ApiCallSync(a.txn.Request, 15*time.Second)

		if err != nil {
			continue
		}

		// check for locks, or other problems
		if resp.StatusCode() == shards.E_ROUTER_TABLE_OLD {
			// ouch bad routertable..

			//update router table, and try the request again.
			rt, err := shards.RequestRouterTable(c)
			if err != nil {
				this.connections.SetRouterTable(rt)
				break
			}
		}

		// partition is locked!
		if resp.StatusCode() == shards.E_PARTITION_LOCKED {
			// sleep 5 seconds and try again.
			time.Sleep(5 * time.Second)
			break
		}

		a.responses[entry.Entry.Id()] = resp
		if a.queryType == "single" {
			// we got one!
			return
		}
	}

	if len(a.responses) == len(entries) {
		//we got them all!
		return
	}

	// we did not all responses.  
	// give it a rest, then try again
	time.Sleep(1 * time.Second)
	//Tail Recursion YAY!
	this.apiCall(a)
	return
}
