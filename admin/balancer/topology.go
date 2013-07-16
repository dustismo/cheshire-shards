package balancer


import (
    // "log"
    "github.com/trendrr/goshire/cheshire"
    "github.com/trendrr/goshire/client"
    "github.com/trendrr/goshire-shards/shards"
    "fmt"
    // "io/ioutil"
    "time"
    "math/rand"
    "log"
)

// All the operations necessary for rebalance and topology changes


// Locks the specified partition.
// Note, this does NOT update the routertable 
// it is assumed the router table is up to date.
func LockPartition(services *Services, routerTable *shards.RouterTable, partition int) error {
    return locking(shards.PARTITION_LOCK, services, routerTable, partition)
}


// Unlocks the specified partition.
// Note, this does NOT update the routertable 
// it is assumed the router table is up to date.
func UnlockPartition(services *Services, routerTable *shards.RouterTable, partition int) error {
    return locking(shards.PARTITION_UNLOCK, services, routerTable, partition)
}


// internal lock code shared by unlock and lock. (different endpoints)
func locking(endpoint string, services *Services, routerTable *shards.RouterTable, partition int) error {
    request := cheshire.NewRequest(endpoint, "POST")
    request.Params().Put("partition", partition)
    // Lock All partitions
    for _, e := range(routerTable.Entries) {
        response, err := client.HttpApiCallSync(
            fmt.Sprintf("%s:%d", e.Address, e.HttpPort),
            request,
            5 * time.Second)
        if err != nil {
            services.Logger.Printf("ERROR While contacting %s -- %s", e.Id(), err)
            //TODO: retry?
            continue
        }
        if response.StatusCode() != 200 {
            //TODO: retry?
            services.Logger.Printf("ERROR While locking partition %s -- %s", e.Id(), response.StatusMessage())
        }
    }
    return nil
}


// Delete the requested partition from the entry.
// this does not lock, and does not update the router table
func DeletePartition(services *Services, entry *shards.RouterEntry, partition int) error {
    
    services.Logger.Printf("DELETING Partition %d From %s", partition, entry.Id())
    request := cheshire.NewRequest(shards.PARTITION_DELETE, "DELETE")    
    request.Params().Put("partition", partition)

    response, err := client.HttpApiCallSync(
            fmt.Sprintf("%s:%d", entry.Address, entry.HttpPort),
            request,
            300 * time.Second)

    if err != nil {
        return err
    }

    if response.StatusCode() != 200 {
        return fmt.Errorf("ERROR While deleting partition: %s", response.StatusMessage())
    }
    return nil
}

// tests that this entry is contactable, and is a proper service
func EntryContact(entry *shards.RouterEntry) error {
    response, err := client.HttpApiCallSync(
            fmt.Sprintf("%s:%d", entry.Address, entry.HttpPort),
            cheshire.NewRequest(shards.CHECKIN, "GET"),
            5 * time.Second)
    if err != nil {
        return fmt.Errorf("ERROR While contacting %s -- %s", entry.Address, err)
    }
    _, ok := response.GetInt64("rt_revision")

    if !ok {
        return fmt.Errorf("ERROR No rt_revision in response from %s -- Status(%s)", entry.Address, response.StatusMessage())
    }
    return nil
}

// Checkin to an entry.  will update their router table if it is out of date.  will update our router table if out of date.
// returns the updated router table, updated, error
// return rt, self updated, remote updated, error
func EntryCheckin(routerTable *shards.RouterTable, entry *shards.RouterEntry) (*shards.RouterTable, bool, bool, error) {
    rt, self, remote, err := shards.RouterTableSync(routerTable, entry)
    return rt, self, remote, err
} 

// Checks with entries to see if an updated router table is available.
// Will update router table on server if local is newer
// returns true if local was updated, or at least one server was updated.
func RouterTableUpdate(services *Services, routerTable *shards.RouterTable, maxChecks int) (*shards.RouterTable, bool) {
    checks := 0
    updated := false
    for _,e := range(routerTable.Entries) {
        if checks >= maxChecks {
            break
        }

        checks++

        rt, updatelocal, updateremote, err := EntryCheckin(routerTable, e)
        if err != nil {
            services.Logger.Printf("%s", err)
            continue
        }
        if updatelocal {
            updated = true
            routerTable = rt
        }
        if updateremote {
            updated = true
        }
    }
    return routerTable, updated
}


// Copies the partition data from one server to another.
// This does not lock the partition, that should happen 
func CopyData(services *Services, routerTable *shards.RouterTable, partition int, from, to *shards.RouterEntry) (int, error) { 
    //Move the data!
    moved := 0
    
    toClient := client.NewJson(to.Address, to.JsonPort)
    err := toClient.Connect()
    if err != nil {
        return moved, err
    }
    defer toClient.Close()

    request := cheshire.NewRequest(shards.PARTITION_IMPORT, "POST")
    request.Params().Put("partition", partition)
    request.Params().Put("source", fmt.Sprintf("http://%s:%d", from.Address, from.HttpPort))
    
    responseChan := make(chan *cheshire.Response, 10)
    errorChan := make(chan error)

    toClient.ApiCall(
        request,
        responseChan,
        errorChan,
    )


    for {
        select {
        case response := <- responseChan :
            bytes := response.MustInt("bytes", 0)
            services.Logger.Printf("Moving partition %d... Moved %d bytes", partition, bytes)

            //check for completion
            if response.TxnComplete() {
                // FINISHED!
                services.Logger.Printf("SUCCESSFULLY Moved partition %d. Moved %d bytes!", partition, bytes)
                return bytes, nil
            }
        case err := <- errorChan :
            services.Logger.Printf("ERROR While Moving data from %s -- %s", from.Address, err)
            return moved, err
        }
    }

    return moved, err
}

// Moves data from one server to another
// Does the following:
// 1. Lock all necessary clients 
// 2. Move Data 
// 3. Update router table on servers
// 4. Delete partion from origin
// 5. Unlock
func MovePartition(services *Services, routerTable *shards.RouterTable, partition int, from, to *shards.RouterEntry) error {
    
    err := LockPartition(services, routerTable, partition)
    if err != nil {
        return err
    }
    //Unlock partition no matter what
    defer UnlockPartition(services, routerTable, partition)

    //copy the data
    _, err = CopyData(services, routerTable, partition, from, to)

    log.Println("Back from copy data!")
    if err != nil {
        log.Println(err)
        return err
    }
    services.Logger.Printf("Now updating the router table")
    //update the router table.
    parts := make([]int, 0)
    for _,p := range(from.Partitions) {
        if p != partition {
            parts = append(parts, p)
        }
    }
    from.Partitions = parts
    to.Partitions = append(to.Partitions, partition)
    
    routerTable, err = routerTable.AddEntries(from, to)

    services.SetRouterTable(routerTable)

    routerTable, ok := RouterTableUpdate(services, routerTable, len(routerTable.Entries)) 
    if !ok {
        services.Logger.Printf("Uh oh, Didnt update any router tables")
    }

    //Delete the data on the from server.
    err = DeletePartition(services, from, partition)
    if err != nil {
        return err
    }
    return nil
}

// Moves a single partition from the largest entry to the smallest.  only
// if the smallest entry is smaller then the rest.
// if all entries are the same size, then a random entry is chosen
func RebalanceSingle(services *Services, routerTable *shards.RouterTable) error {

    var smallest *shards.RouterEntry = nil
    var largest *shards.RouterEntry = nil

    min := int(routerTable.TotalPartitions / len(routerTable.Entries))
    services.Logger.Printf("Looking for entries with more then %d partitions", min)

    //shuffle the entries array
    entries := make([]*shards.RouterEntry, len(routerTable.Entries))
    perm := rand.Perm(len(routerTable.Entries))
    for i, v := range perm {
        entries[v] = routerTable.Entries[i]
    }
    for _, entry := range(entries) {
        services.Logger.Printf("Entry %s has %d partitions", entry.Id(), len(entry.Partitions))

        if len(entry.Partitions) < min {
            if smallest == nil {
                smallest = entry
            } else if len(entry.Partitions) < len(smallest.Partitions) {
                smallest = entry
            }
        } else if len(entry.Partitions) > min {
            if largest == nil {
                largest = entry
            } else if len(entry.Partitions) > len(largest.Partitions) {
                largest = entry
            }
        }

    }
    if smallest == nil || largest == nil || smallest == largest {
        services.Logger.Printf("Cluster appears to be balanced")
        return nil
    }
    partition := largest.Partitions[0]
    services.Logger.Printf("Moving partition %d from %s to %s", partition, largest.Id(), smallest.Id())
    err := MovePartition(services, routerTable, partition, largest, smallest)
    if err != nil {
        services.Logger.Printf("ERROR During move %s", err)    
    }
    
    return err
}




