package shards

import(
    "sync"
    "fmt"
    "github.com/trendrr/goshire/client"
    "log"
)


//struct to match the entry with a specific client connection
type EntryClient struct {
    Entry *RouterEntry
    Client client.Client

    //need locking to handle the dead client issue
    lock sync.RWMutex
    dead bool
    lastLookup time.Time



}

//creates a client from a router entry
// supplying a custom one makes it easy to 
// change between http/json ect.
type ClientCreator interface {
    Create(entry *RouterEntry) client.Client
}

// Manages the connections to the different shards
type Connections struct {
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
func (this *Connections) Entries(partition int) ([]*EntryClient, error) {
    this.lock.RLock()
    defer this.lock.RUnlock()
    if partition > this.table.TotalPartitions || partition < 0 {
        return nil, fmt.Errorf("Partition %d is out of range", partition)
    }
    return this.connections[partition], nil
}

// Creates a new EntryClient
func (this *Connections) createEntryClient(entry *RouterEntry) *EntryClient {
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
func (this *Connections) SetRouterTable(table *RouterTable) (*RouterTable, error) {
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
        c[key] = entry
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
        e.Client.Close()
    }
    oldTable := this.table
    this.entries = c
    this.connections = connections
    this.table = table
    return oldTable, nil
}

// A default client creation.  JSON with poolsize = 10
func (this *Connections) Create(entry *RouterEntry) (client.Client) {
    c := client.NewJson(entry.Address, entry.JsonPort)
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
