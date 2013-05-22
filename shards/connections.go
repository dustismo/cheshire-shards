package shards

import (
	"fmt"
	"github.com/trendrr/goshire/client"
	"log"
	"sync"
	"time"
)

//struct to match the entry with a specific client connection
type EntryClient struct {
	Entry  *RouterEntry
	client client.Client

	//need locking to handle the dead client issue
	created       bool
	lock          sync.RWMutex
	lastLookup    time.Time
	clientCreator ClientCreator
}

// Gets the client.
// This will handle reinitialing the client if it is dead for some reason
func (this *EntryClient) Client() (client.Client, error) {
	this.lock.RLock()
	if this.created {
		c := this.client
		this.lock.RUnlock()
		return c, nil
	}
	this.lock.RUnlock()
	//upgrade to a write lock
	this.lock.Lock()
	defer this.lock.Unlock()
	//need to check again because there is a window between giving up the readlock 
	// and aquiring the write lock.
	if this.created {
		//someone else got to it before we did 
		return this.client, nil
	}

	//check that lastlook isnt within 5 seconds.
	t := time.Now().Sub(this.lastLookup) * time.Second
	if t < 5 {
		return this.client, fmt.Errorf("No client available, will try to connect again in a few seconds")
	}
	//now attempt to connect.
	c, err := this.clientCreator.Create(this.Entry)
	this.client = c
	this.lastLookup = time.Now()
	if err == nil {
		this.created = true
	}
	return c, err
}

//creates a client from a router entry
// supplying a custom one makes it easy to 
// change between http/json ect.
type ClientCreator interface {
	Create(entry *RouterEntry) (client.Client, error)
}

// Manages the connections to the different shards
type Connections struct {
	lock  sync.RWMutex
	table *RouterTable
	//connections indexed by partition
	connections   [][]*EntryClient
	clientCreator ClientCreator
	//entries organized by entry id
	entries map[string]*EntryClient
	//a channel you can listen for 
	// router table changes
	// this will send the OLD router table
	RouterTableChange chan *RouterTable
}

// Loads the router table from one or more of the urls 
func ConnectionsFromSeed(urls ...string) (*Connections, error) {
	connections := &Connections{}
	err := connections.InitFromSeed(urls...)
	return connections, err
}

func (this *Connections) InitFromSeed(urls ...string) error {
	var rt *RouterTable
	var err error
	for _, url := range urls {
		c := client.NewHttp(url)
		rt, err = RequestRouterTable(c)
		if err != nil {
			break
		}
	}
	if rt == nil {
		return fmt.Errorf("Unable to get a router table from urls %s ERROR(%s)", urls, err)
	}
	_, err = this.SetRouterTable(rt)
	return err
}

func (this *Connections) RouterTable() *RouterTable {
	return this.table
}

// finds an entry based on the entry Id
func (this *Connections) EntryById(id string) (*EntryClient, bool) {
	this.lock.RLock()
	defer this.lock.RUnlock()
	v, ok := this.entries[id]
	return v, ok
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
	e := &EntryClient{
		Entry:         entry,
		created:       false,
		clientCreator: this.clientCreator,
	}
	return e
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
	for _, e := range table.Entries {
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
		for _, e := range rte {
			val, ok := c[e.Id()]
			if !ok {
				return nil, fmt.Errorf("Could not get connection for entry %d", e.Id())
			}
			entries = append(entries, val)
		}
		connections[i] = entries
	}

	//now close any Clients for removed entries
	for _, e := range this.entries {
		c, err := e.Client()
		if err != nil {
			log.Println(err)
		} else {
			c.Close()
		}
	}
	oldTable := this.table
	this.entries = c
	this.connections = connections
	this.table = table

	//non-blocking channel send.
	select {
	case this.RouterTableChange <- oldTable:
	default:
	}

	return oldTable, nil
}

// A default client creation.  JSON with poolsize = 10
func (this *Connections) Create(entry *RouterEntry) (client.Client, error) {
	c := client.NewJson(entry.Address, entry.JsonPort)
	c.PoolSize = 5
	c.MaxInFlight = 250
	err := c.Connect()
	return c, err
}
