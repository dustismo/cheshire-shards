package shards

import (
	// "time"
	"fmt"
	"github.com/trendrr/goshire/cheshire"
	"github.com/trendrr/goshire/client"
	"github.com/trendrr/goshire/dynmap"
	"io"
	"io/ioutil"
	"log"
	"os"
	"sync"
	"time"
)

// You must implement this interface in order for balancing to work
type Shard interface {

	//Exports all the data for a specific partition
	//should send total # of bytes on the finished chanel when complete
	ExportPartition(partition int, writer io.Writer, finished chan int64, errorChan chan error)

	//Imports data
	ImportPartition(partition int, reader io.Reader, finished chan int64, errorChan chan error)

	//Deletes the requested partition
	DeletePartition(partition int) error
}

// A dummy service
type DummyShard struct {
}

func (this *DummyShard) ExportPartition(partition int, writer io.Writer, finished chan int64, errorChan chan error) {
	log.Printf("Requesting Export from dummy service, ignoring.. (partition: %d)", partition)
	finished <- int64(0)
}

func (this *DummyShard) ImportPartition(partition int, reader io.Reader, finished chan int64, errorChan chan error) {
	log.Printf("Requesting Import from dummy service, ignoring.. (partition: %d),(reader: %s)", partition, reader)
	finished <- int64(0)
}

func (this *DummyShard) DeletePartition(partition int) error {
	log.Printf("Requesting DeletePartiton from dummy service, ignoring.. (partition: %d)", partition)
	return nil
}

type EventType string

type Event struct {
	EventType string
}

// Manages the router table and connections and things
type Manager struct {
	lock        sync.RWMutex
	connections *Connections
	ServiceName string
	DataDir     string
	//my entry id.  TODO: need a good way to autodiscover this..
	MyEntryId        string
	shard            Shard
	lockedPartitions map[int]bool
}

// Creates a new manager.  Uses the one or more seed urls to download the
// routing table.
func NewManagerSeed(shard Shard, serviceName, dataDir, myEntryId string, seedHttpUrls ...string) (*Manager, error) {
	//TODO: can we get the servicename from the routing table?
	manager := NewManager(shard, serviceName, dataDir, myEntryId)
	err := manager.connections.InitFromSeed(seedHttpUrls...)
	//we still return the manager since it is usable just doesnt have a routing table.
	return manager, err
}

//Creates a new manager.  will load the routing table from disk if
//it exists
func NewManager(shard Shard, serviceName, dataDir, myEntryId string) *Manager {
	rtchange := make(chan *RouterTable)

	manager := &Manager{
		connections:      &Connections{RouterTableChange: rtchange},
		DataDir:          dataDir,
		ServiceName:      serviceName,
		MyEntryId:        myEntryId,
		shard:            shard,
		lockedPartitions: make(map[int]bool),
	}
	//attempt to load from disk
	err := manager.load()
	if err != nil {
		log.Println(err)
		log.Println("Unable to load router table, setting dummy routertable")
		manager.SetRouterTable(NewRouterTable(serviceName))
	}
	// Save whenever the routertable is changed.
	go func() {
		for {
			<-rtchange
			err := manager.save()
			if err != nil {
				log.Printf("ERROR Trying to save router table : %s", err)
			}
		}
	}()
	return manager
}

// Registers all the necessary controllers for partitioning.
func (this *Manager) RegisterControllers() error {
	RegisterServiceControllers(this)
	return nil
}

// Puts a lock on the specified partition (locally only)
func (this *Manager) LockPartition(partition int) error {
	this.lock.Lock()
	defer this.lock.Unlock()
	this.lockedPartitions[partition] = true
	return nil
}

func (this *Manager) UnlockPartition(partition int) error {
	this.lock.Lock()
	defer this.lock.Unlock()
	delete(this.lockedPartitions, partition)
	return nil
}

// Returns the list of partitions I am responsible for
// returns an empty list if I am not responsible for any
func (this *Manager) MyPartitions() []int {
	this.lock.RLock()
	defer this.lock.RUnlock()
	if this.connections == nil {
		return make([]int, 0)
	}

	e, ok := this.connections.EntryById(this.MyEntryId)
	if !ok {
		return make([]int, 0)
	}
	return e.Entry.Partitions
}

// Checks if this partition is my responsibility.
// This is also how we test for locked partitions.
//
// returns responsibility, locked
//
func (this *Manager) MyResponsibility(partition int) (bool, bool) {
	this.lock.RLock()
	defer this.lock.RUnlock()
	isMine := false
	if this.connections != nil {
		e, ok := this.connections.EntryById(this.MyEntryId)
		if ok {
			isMine, _ = e.Entry.PartitionsMap[partition]
		}
	}
	locked, ok := this.lockedPartitions[partition]
	if !ok {
		locked = false
	}
	return isMine, locked
}

//Sets the service for this manager
//this should only be called once at initialization.  it is not threadsafe
func (this *Manager) SetShard(par Shard) {
	this.shard = par
}

// Does a checkin with the requested client.  returns the
// router table revision of the connection.
func (this *Manager) Checkin(client client.Client) (int64, error) {
	response, err := client.ApiCallSync(cheshire.NewRequest(CHECKIN, "GET"), 10*time.Second)
	if err != nil {
		return int64(0), err
	}
	revision := response.MustInt64("rt_revision", int64(0))
	return revision, nil
}

//loads the stored version
func (this *Manager) load() error {
	bytes, err := ioutil.ReadFile(this.filename())
	if err != nil {
		return err
	}
	mp := dynmap.NewDynMap()
	err = mp.UnmarshalJSON(bytes)
	if err != nil {
		return err
	}
	table, err := ToRouterTable(mp)
	if err != nil {
		return err
	}
	this.connections.SetRouterTable(table)
	return nil
}

func (this *Manager) save() error {
	rt, err := this.RouterTable()
	if err != nil {
		return err
	}
	mp := rt.ToDynMap()
	bytes, err := mp.MarshalJSON()
	if err != nil {
		return err
	}
	err = ioutil.WriteFile(this.filename(), bytes, 0644)
	return err
}

func (this *Manager) filename() string {
	if this.DataDir == "" {
		return fmt.Sprintf("%s.routertable", this.ServiceName)
	}
	return fmt.Sprintf("%s%c%s.routertable", this.DataDir, os.PathSeparator, this.ServiceName)
}

func (this *Manager) Clients(partition int) ([]*EntryClient, error) {
	this.lock.RLock()
	defer this.lock.RUnlock()

	c, err := this.connections.Entries(partition)
	return c, err
}

//returns the current router table
func (this *Manager) RouterTable() (*RouterTable, error) {
	this.lock.RLock()
	defer this.lock.RUnlock()
	if this.connections == nil {
		return nil, fmt.Errorf("No Router Table available")
	}
	rt := this.connections.RouterTable()
	if rt == nil {
		return rt, fmt.Errorf("No RouterTable available")
	}

	return rt, nil
}

// Sets a new router table, returns the old one
func (this *Manager) SetRouterTable(rt *RouterTable) (*RouterTable, error) {
	if rt.Service != this.ServiceName {
		return nil, fmt.Errorf("Error cannot set router table for service %s, should be service %s", rt.Service, this.ServiceName)
	}
	old, err := this.connections.SetRouterTable(rt)
	return old, err
}
