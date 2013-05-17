package shards

import (
    // "time"
    "github.com/trendrr/goshire/dynmap"
    "fmt"
    "time"
    // "log"
)


// A router table.
// The table is considered generally immutable.  If any changes occur a new table should
// be generated and propagated.
type RouterTable struct {

    //The name of the service. should be unique.  (Example: "trendrrdb")
    Service string

    //The revision # of the router table
    //this should be always increasing so greater revision means more upto date router table
    //this should typically be a timestamp 
    Revision int64

    //total # of partitions
    TotalPartitions int

    //Replication Factory
    ReplicationFactor int

    //This is the param that we partition by
    PartitionKey string

    //This is me
    MyEntry *RouterEntry

    //entries organized by partition
    //index in the array is the partition
    EntriesPartition [][]*RouterEntry

    //The unique entries
    Entries []*RouterEntry

    //serialized dynmap
    DynMap *dynmap.DynMap
}

func NewRouterTable(service string) *RouterTable {
    return &RouterTable {
        Service : service,
        DynMap : dynmap.NewDynMap(),
        Revision : time.Now().Unix(),
        TotalPartitions : 0,
        ReplicationFactor : 2,
    }
}

// updates the revision to now
func (this *RouterTable) UpdateRevision() (previous, current int64) {
    prev := this.Revision
    this.Revision = time.Now().Unix()
    //reset the dynmap
    this.DynMap = dynmap.NewDynMap()
    return prev, this.Revision
}

//Rebuilds this router table, if you changed anything, you should call this and use the newly built 
//table
func (this *RouterTable) Rebuild() (*RouterTable, error) {
    total := 0
    //set TotalPartitions
    for _,e := range(this.Entries) {
        total += len(e.Partitions)
    }
    this.TotalPartitions = total
    mp := this.toDynMap()
    table,err := ToRouterTable(mp)
    if err != nil {
        return nil, err
    }
    return table, nil 
}

// Adds one or more new entries
// These entries will replace any entries in the current routertable.
// a new router table is returned.
func (this *RouterTable) AddEntries(entry ...*RouterEntry) (*RouterTable, error) {
    //copy the router table
    routerTable, err := this.Rebuild()
    if err != nil {
        return routerTable, err
    }

    entries := make([]*RouterEntry, 0)

    for _, e := range(routerTable.Entries) {
        //check if this entry matches any of the entries we are adding.
        found := false
        for _, en := range(entry) {
            if e.Id() == en.Id() {
                found = true
            }
        }
        if !found {
            entries = append(entries, e)
        }
    }

    //now add the new ones
    for _, en := range(entry) {
        entries = append(entries, en)
    }
    routerTable.Entries = entries
    routerTable.UpdateRevision()
    routerTable, err = routerTable.Rebuild()
    return routerTable, err
}

func (this *RouterTable) FindEntry(id string) (*RouterEntry, bool) {
    for _, e := range(this.Entries) {
        if e.Id() == id {
            return e, true
        }
    }
    return nil, false
}

// Creates a new router table from the dynmap passed in
func ToRouterTable(mp *dynmap.DynMap) (*RouterTable, error) {
    t := &RouterTable{}

    var ok bool
    t.Service, ok = mp.GetString("service")
    if !ok {
        return nil, fmt.Errorf("No Service in the table %s", mp)
    }

    t.Revision, ok = mp.GetInt64("revision")
    if !ok {
        return nil, fmt.Errorf("No Revision in the table %s", mp)   
    }

    t.TotalPartitions, ok = mp.GetInt("total_partitions")
    if !ok {
        return nil, fmt.Errorf("No total_partitions in the table %s", mp)
    }

    t.ReplicationFactor, ok = mp.GetInt("replication_factor")
    if !ok {
        return nil, fmt.Errorf("No replication_factor in the table %s", mp)
    }

    t.PartitionKey, ok = mp.GetString("partition_key")
    if !ok {
        //do nothing, right?
    }

    //fill the entries
    t.Entries = make([]*RouterEntry, 0)
    entryMaps, ok := mp.GetDynMapSlice("entries")
    if !ok {
        return nil, fmt.Errorf("Bad entries in the table %s", mp)      
    }
    for _, em := range(entryMaps) {
        routerEntry, err := ToRouterEntry(em)
        if err != nil {
            return nil, err
        }
        t.Entries = append(t.Entries, routerEntry)
    }


    // set up the partition to entry mapping
    partitionCount := 0
    entriesPartition := make([]*RouterEntry, t.TotalPartitions)
    for _, e := range(t.Entries) {
        for _,p := range(e.Partitions) {
            if p >= t.TotalPartitions {
                return nil, fmt.Errorf("Bad Partition entry (greater then total partitions): %s", e)
            }
            entriesPartition[p] = e
            partitionCount++
        }
    }

    if partitionCount != t.TotalPartitions {
        return nil, fmt.Errorf("Bad table, some partitions un accounted for")
    }

    t.DynMap = t.toDynMap()

    t.EntriesPartition = make([][]*RouterEntry, t.TotalPartitions)
    //Now setup the replication partitions. 
    for _, e := range(t.Entries) {
        
        for _,p := range(e.Partitions) {
            pRep, err := t.repPartitions(p, e)
            if err != nil {
                return nil, fmt.Errorf("Bad table (%s)", err)
            }
            entries := make([]*RouterEntry, len(pRep)+1)
            entries[0] = e
            for i :=1; i < len(entries); i++ {
                entries[i] = entriesPartition[pRep[i-1]]
                e.PartitionsMap[pRep[i-1]] = false
            }
            t.EntriesPartition[p] = entries
        }
    }

    return t, nil
}

//internal use.
//skips the cached version
func (this *RouterTable) toDynMap() *dynmap.DynMap {
    mp := dynmap.NewDynMap()
    mp.Put("service", this.Service)
    mp.Put("revision", this.Revision)
    mp.Put("total_partitions", this.TotalPartitions)
    mp.Put("replication_factor", this.ReplicationFactor)
    mp.Put("partition_key", this.PartitionKey)
    
    entries := make([]*dynmap.DynMap, 0)
    for _,e := range(this.Entries) {
        entries = append(entries, e.ToDynMap())
    }
    mp.Put("entries", entries)
    this.DynMap = mp
    return mp
}

// Translate to a DynMap of the form:
// {
//     "service" : "trendrrdb",
//     "revision" : 898775762309309,
//     "total_partitions" : 256,
//     "entries" : [
//         {/*router entry 1*/},
//         {/*router entry 2*/}
//     ]
// }
func (this *RouterTable) ToDynMap() *dynmap.DynMap {
    if this.DynMap != nil && len(this.DynMap.Map) > 0 {
        return this.DynMap
    }
   return this.toDynMap()
}

//gets the partitions that should replicate this master.
func (this *RouterTable) repPartitions(partition int, entry *RouterEntry) ([]int, error){
    //This method could be much better optimized, but 
    //it is fairly rare, so we wont worry about it..
    entries := make([]int, 0)
    if partition >= this.TotalPartitions {
        return entries, fmt.Errorf("Requested partition %d is out of bounds (%d) ", partition, this.TotalPartitions )
    }

    for i := 1; i < this.TotalPartitions; i++ {
        par := (i+partition) % this.TotalPartitions
        v, ok := entry.PartitionsMap[par]
        if ok && v {
            //this is master.  skip to next one
            continue
        }
        entries = append(entries, par)
        //we check if len < repfactor -1 (minus one because we still need the current partition)
        if len(entries) == this.ReplicationFactor-1 {
            return entries, nil
        }
    } 
    return entries, nil

}


// Gets the entries associated with the given partition
// [0] should be the master entry, and there should be 
// table.ReplicationFactor number of entries
func (this *RouterTable) PartitionEntries(partition int) ([]*RouterEntry, error) {
    if partition >= this.TotalPartitions {
        return make([]*RouterEntry, 0), fmt.Errorf("Requested partition %d is out of bounds (%d) ", partition, this.TotalPartitions )
    }
    return this.EntriesPartition[partition], nil
}


type RouterEntry struct {
    //The address of this entry
    Address string
    JsonPort int
    HttpPort int

    //Is this entry me?
    Self bool
    
    //set for last ping time
    LastSeenAt time.Time    

    //list of partitions this entry is responsible for (master only)
    Partitions []int

    //Map of all the partitions this entry is responsible for.  true indicates master, false otherwise
    PartitionsMap map[int]bool 

    //this entry serialized as a DynMap
    DynMap *dynmap.DynMap
}

// Creates a new router entry from the dynmap passed in
func ToRouterEntry(mp *dynmap.DynMap) (*RouterEntry, error) {
    e := &RouterEntry{
        Self : false,
        PartitionsMap : make(map[int]bool),
    }
    var ok bool
    e.Address, ok = mp.GetString("address")
    if !ok {
        return nil, fmt.Errorf("No Address in Entry: %s", mp)
    }

    e.JsonPort = mp.MustInt("ports.json", 0)
    e.HttpPort = mp.MustInt("ports.http", 0)
    e.LastSeenAt = mp.MustTime("last_seen_at", *new(time.Time))
    e.Partitions, ok = mp.GetIntSlice("partitions")
    if !ok {
        e.Partitions = make([]int, 0)
    }
    for _, p := range(e.Partitions) {
        e.PartitionsMap[p] = true
    }
    e.DynMap = e.ToDynMap()
    return e, nil
}

//Id for this entry.  current is address:jsonport
func (this *RouterEntry) Id() string {
    return fmt.Sprintf("%s:%d", this.Address, this.JsonPort)
}

// Translate to a DynMap of the form:
// {
//     "id" : "localhost:8009"
//     "address" : "localhost",
//     "ports" : {
//         "json" : 8009,
//         "http" : 8010
//     }
//     "partitions" : [1,2,3,4,5,6,7,8,9]
// }
func (this *RouterEntry) ToDynMap() *dynmap.DynMap {
    if this.DynMap != nil && len(this.DynMap.Map) > 0 {
        return this.DynMap
    }

    mp := dynmap.NewDynMap()
    mp.Put("address", this.Address)
    if this.JsonPort > 0 {
        mp.PutWithDot("ports.json", this.JsonPort)
    }

    if this.HttpPort > 0 {
        mp.PutWithDot("ports.http", this.HttpPort)
    }
    mp.Put("id", this.Id())
    mp.Put("partitions", this.Partitions)
    mp.Put("last_seen_at", this.LastSeenAt)
    this.DynMap = mp
    return mp
}
