package router

import(
    "github.com/trendrr/goshire/cheshire"
    "github.com/trendrr/goshire/client"
)

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

//struct to match the entry with a specific client connection
type EntryClient struct {
    Entry *shards.RouterEntry
    Client client.Client
}

//creates a client from a router entry
// supplying a custom one makes it easy to 
// change between http/json ect.
type ClientCreator interface {
    Create(entry *RouterEntry) client.Client
}

// The router
// Handles matching the route
// Handles forwarding the requests to the target
type Router struct {
    lock sync.RWMutex
    table *RouterTable
    //connections indexed by partition
    connections [][]*EntryClient
    clientCreator ClientCreator
    //entries organized by entry id
    entries map[string]*EntryClient
}

// Returns the entries associated to this partition.
// the "master" will be at position [0]
func (this *Router) Entries(int partition) ([]*EntryClient, error) {
    this.lock.RLock()
    defer this.lock.RUnlock()
    if partition > this.table.TotalPartitions || partition < 0 {
        return nil, fmt.Errorf("Partition %d is out of range", partition)
    }
    return this.connections[partition]
}

// Creates a new EntryClient
func (this *Router) createEntryClient(entry *RouterEntry) *EntryClient {
    client := this.clientCreator.Create(entry)
    return &EntryClient{
        Entry : entry,
        Client : client,
    }
}

// Sets a new router table.
// will close and remove and client connections that no longer exist. 
// will return an error on any problem, in which case the old router table
// should remain intact.
// Returns the old router table
func (this *Router) SetRouterTable(table *RouterTable) (*RouterTable, error) {
    this.lock.Lock()
    defer this.lock.Unlock()
    if this.table != nil {
        if this.table.Revision >= table.Revision {
            return nil, fmt.Errorf("Trying to set an older revision %d vs %d", this.table.Revision, table.Revision)
        }
    }

    //create a new map for connections
    c := make(map[string]*EntryClient)
    for _,e := range(table.Entries) {
        key := e.Id()
        entry, ok := this.entries[key]
        if !ok {
            entry = this.createEntryClient(e)
        } 
        delete(this.entries, key)
        c[key] = conn
    }


    //now set up the connections to partition mapping
    connections := make([][]*EntryClient, table.TotalPartitions)

    for i := 0; i < table.TotalPartitions; i++ {
        entries := make([]*EntryClient, 0)
        rte, err := table.PartitionEntries(i)
        if err != nil {
            return nil, err
        }
        for _, e := range(rte) {
            val, ok := c[e.Id()]
            if !ok {
                return nil, fmt.Errorf("Could not get connection for entry %d", e.Id())
            }
            entries = append(entries, val)
        }
        connections[i] = entries
    }

    //now close any Clients for removed entries
    for _, e := range(this.entries) {
        e.client.Close()
    }
    oldTable := this.table
    this.entries = c
    this.connections = connections
    this.table = table
    this.save()

    return oldTable, nil
}

// A default client creation.  JSON with poolsize = 10
func (this *Router) Create(entry *RouterEntry) (client.Client) {
    c := cheshire.NewJsonClient(entry.Address, entry.JsonPort)
    c.PoolSize = 5
    c.MaxInFlight = 250
    err := c.Connect()
    if err != nil {
        //TODO: we need some way to deal with this.
        // for instance, if one node happened to be restarting
        // as the router table was set, then this client would be stuck forever
        // 
        // Not sure the best approach...
        log.Println("Error creating client! %s -- %s", err, entry)
    }
    return c
}




