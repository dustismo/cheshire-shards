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

// This is a client that maintains connections
// to all nodes in the cluster, and routes the request to the appropriate one
// This conforms to the cheshire.Client interface.
type Client struct {
	connections *shards.Connections
	hasher      Hasher
}

// creates a new client from seed urls.
func NewClientFromSeed(seedUrls ...string) (*Client, error) {
	client := &Client{}

	connections := &shards.Connections{}
	connections.SetClientCreator(client)

	err := connections.InitFromSeed(seedUrls...)
	if err != nil {
		return nil, err
	}

	client.connections = connections
	client.hasher = &DefaultHasher{}
	return client, nil
}

// a syncronous api call.
// Currently the timeout is ignored..
func (this *Client) ApiCallSync(req *cheshire.Request, timeout time.Duration) (*cheshire.Response, error) {

	keyParams := this.connections.RouterTable().PartitionKeys

	partitionFound := false

	vals := make([]string, len(keyParams))
	for i, k := range keyParams {
		v, ok := req.Params().GetString(k)
		if ok {
			partitionFound = true
			vals[i] = v
		} else {
			vals[i] = ""
		}
	}

	if !partitionFound {
		//uhh, return error, or broadcast?
		log.Println("No partition key found")
	}

	partitionKey := strings.Join(vals, "|")

	partition, err := this.hasher.Hash(partitionKey, this.connections.RouterTable().TotalPartitions)
	if err != nil {
		return nil, err
	}
	//Add the required params
	req.Params().Put(shards.P_PARTITION, partition)
	req.Params().Put(shards.P_REVISION, this.connections.RouterTable().Revision)

	queryType := req.Params().MustString(shards.P_QUERY_TYPE, "all")

	//Do the damn api call!
	max := 5
	if queryType == "all_q" {
		max = 100
	}

	a := &apiRR{
		partition: partition,
		queryType: queryType,
		responses: make(map[string]*cheshire.Response),
		response:  cheshire.NewResponse(req),
		request:   req,
		count:     0,
		max:       max,
	}

	this.apiCall(a)

	//send response.
	if len(a.responses) == 0 {
		// set the error response
		a.response.SetStatus(501, "Unable to get a single response.")
	}
	for k, r := range a.responses {
		r.Put("server_id", k)
		a.response.AddToSlice("responses", r)
	}

	return a.response, nil
}

// Dont use, this is not implemented
func (this *Client) ApiCall(req *cheshire.Request, responseChan chan *cheshire.Response, errorChan chan error) error {
	log.Println("ERROR ApiCall in router.Client not implemented, use the syncronous one!")
	return fmt.Errorf("ERROR ApiCall in router.Client not implemented, use the syncronous one!")
}

//Closes this client
func (this *Client) Close() {
	//TODO
}

//satisfy the ClientCreator interface for the connections object.
// default to using a large maxinflight and poolsize.
// Do not call this method directly
func (this *Client) Create(entry *shards.RouterEntry) (client.Client, error) {
	c := client.NewJson(entry.Address, entry.JsonPort)
	c.PoolSize = 25
	c.MaxInFlight = 10000
	err := c.Connect()
	return c, err
}

// wrapper to hold a request and response
type apiRR struct {
	partition int
	queryType string
	responses map[string]*cheshire.Response
	response  *cheshire.Response
	request   *cheshire.Request
	count     int
	max       int
}

// Does the actual apiCall
// keeps track of all the responses in the responses map
func (this *Client) apiCall(a *apiRR) {

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
		log.Printf("ERRROR %s", err)
		return
	}

	//make sure this txnId is unique to each connection.
	a.request.SetTxnId(fmt.Sprintf("%d", client.NewTxnId()))

	//Fuck it, just do the calls serially, this makes life soo much
	//easier then trying to do them in parallel.
	//
	for _, entry := range entries {
		c, err := entry.Client()
		if err != nil {
			log.Printf("ERR %s", err)
			//TODO: Add to retry queue
			continue
		}
		//check if we have already run this id elsewhere
		if _, ok := a.responses[entry.Entry.Id()]; ok {
			continue
		}

		resp, err := c.ApiCallSync(a.request, 15*time.Second)

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
