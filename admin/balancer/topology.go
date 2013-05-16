package balancer


import (
    // "log"
    "github.com/trendrr/cheshire-golang/cheshire"
    shards "github.com/dustismo/cheshire-shards/shards"
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

    // Lock All partitions
    for _, e := range(routerTable.Entries) {
        response, err := cheshire.HttpApiCallSync(
            fmt.Sprintf("%s:%d", e.Address, e.HttpPort),
            cheshire.NewRequest(endpoint, "GET"),
            5 * time.Second)
        if err != nil {
            services.Logger.Printf("ERROR While contacting %s -- %s", e.Address, err)
            //TODO: retry?
            continue
        }
        if response.StatusCode() != 200 {
            //TODO: retry?
            services.Logger.Printf("ERROR While locking partition %s -- %s", e.Address, response.StatusMessage())
        }
    }
    return nil
}


// Delete the requested partition from the entry.
// this does not lock, and does not update the router table
func DeletePartition(services *Services, entry *shards.RouterEntry, partition int) error {
    
    services.Logger.Printf("DELETING Partition %d From %s", partition, entry.Address)
    request := cheshire.NewRequest(shards.PARTITION_DELETE, "DELETE")    
    request.Params().Put("partition", partition)

    services.Logger.Printf("NOW Deleting the partition from the origin server %s", entry.Address)

    response, err := cheshire.HttpApiCallSync(
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

// Checkin to an entry.  will update their router table if it is out of date.  will update our router table if out of date.
func EntryCheckin(routerTable *shards.RouterTable, entry *shards.RouterEntry) (*shards.RouterTable, bool, error) {
        // make sure our routertable is up to date.
        response, err := cheshire.HttpApiCallSync(
            fmt.Sprintf("%s:%d", entry.Address, entry.HttpPort),
            cheshire.NewRequest(shards.CHECKIN, "GET"),
            5 * time.Second)
        if err != nil {
            return routerTable, false, fmt.Errorf("ERROR While contacting %s -- %s", entry.Address, err)
        }

        entry.LastSeenAt = time.Now()
        
        rev := response.MustInt64("rt_revision", 0)
        if rev == routerTable.Revision {
            return routerTable, false, nil
        }

        if rev < routerTable.Revision {
            //updating server.
            //set the new routertable.
            req := cheshire.NewRequest(shards.ROUTERTABLE_SET, "POST")
            req.Params().Put("router_table", req.ToDynMap())

            response, err = cheshire.HttpApiCallSync(
                fmt.Sprintf("%s:%d", entry.Address, entry.HttpPort),
                req,
                5 * time.Second)
            if err != nil {
                return routerTable, false, fmt.Errorf("ERROR While contacting for router table update %s -- %s", entry.Address, err)
            }
            if response.StatusCode() != 200 {
                return routerTable, false, fmt.Errorf("Error trying to Set router table %s -- %s", entry.Address, response.StatusMessage())
            }
        } else {
            //updating local 

            log.Printf("Found updated router table at: %s", entry.Address)
            //get the new routertable.
            response, err = cheshire.HttpApiCallSync(
                fmt.Sprintf("%s:%d", entry.Address, entry.HttpPort),
                cheshire.NewRequest(shards.ROUTERTABLE_GET, "GET"),
                5 * time.Second)
            if err != nil {
                return routerTable, false, fmt.Errorf("ERROR While contacting %s -- %s", entry.Address, err)
            }
            mp, ok := response.GetDynMap("router_table")
            if !ok {
                return routerTable, false, fmt.Errorf("ERROR from %s -- BAD ROUTER TABLE RESPONSE %s", entry.Address, response)
            }

            rt, err := shards.ToRouterTable(mp)
            if err != nil {
                return routerTable, false, fmt.Errorf("ERROR While parsing router table %s -- %s", entry.Address, err)
            }

            log.Printf("SUCCESSFULLY update router table to revision %d", rt.Revision)
            routerTable = rt
            return routerTable, true, nil

        } 
        return routerTable, false, nil
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

        rt, ud, err := EntryCheckin(routerTable, e)
        if err != nil {
            services.Logger.Printf("%s", err)
            continue
        }
        if ud {
            updated = true
            routerTable = rt
        }
    }
    return routerTable, updated
}


// Copies the partition data from one server to another.
// This does not lock the partition, that should happen 
func CopyData(services *Services, routerTable *shards.RouterTable, partition int, from, to *shards.RouterEntry) (int, error) { 
    //Move the data!
     moved := 0

    //create a new json connection
    fromClient := cheshire.NewJsonClient(from.Address, from.JsonPort)
    err := fromClient.Connect()
    if err != nil {
        return moved, err
    }
    defer fromClient.Close()
    
    toClient := cheshire.NewJsonClient(to.Address, to.JsonPort)
    err = toClient.Connect()
    if err != nil {
        return moved, err
    }
    defer toClient.Close()

    request := cheshire.NewRequest(shards.DATA_PULL, "GET")
    request.Params().Put("partition", partition)
    
    responseChan := make(chan *cheshire.Response, 10)
    errorChan := make(chan error)

    fromClient.ApiCall(
        request,
        responseChan,
        errorChan,
    )

   
    toResponseChan := make(chan *cheshire.Response, 10)
    toErrorChan := make(chan error)

    for {
        select {
        case response := <- responseChan :
            //TODO: send the data to the toClient
            request := cheshire.NewRequest(shards.DATA_PUSH, "PUT")
            d, ok := response.GetDynMap("data")
            if !ok {
                services.Logger.Printf("ERROR: packet missing data :: %s ", request)
                continue
            }
            request.Params().Put("data", d)
            request.Params().Put("partition", partition)
            toClient.ApiCall(
                request,
                toResponseChan,
                toErrorChan,
            )

            // keep a log of items moved
            moved++
            if moved % 100 == 0 {
                services.Logger.Printf("Moving partition %d... Moved %d objects so far", partition, moved)
            }

            //check for completion
            if response.TxnStatus() == "complete" {
                // FINISHED!
                services.Logger.Printf("SUCCESSFULLY Moved partition %d. Moved %d objects!", partition, moved)
                break
            }

        case err := <- errorChan :
            services.Logger.Printf("ERROR While Moving data from %s -- %s", from.Address, err)
            return moved, err

        case response := <- toResponseChan :
            if response.StatusCode() != 200 {
                services.Logger.Printf("ERROR While Moving data from %s -- %s.  \n Continuing...", from.Address, response.StatusMessage())
            }

            //do nothing, 
        case err := <- toErrorChan :
            services.Logger.Printf("ERROR While Moving data to %s -- %s", to.Address, err)
            return moved, err
        }
    }

    // Now make sure we got responses for all the PUT data ops
    count := 0
    for toClient.CurrentInFlight() > 1 {
        services.Logger.Printf("NOW Waiting for success messages to complete")
        select {
            case response := <- toResponseChan :
            //do nothing, 
                if response.StatusCode() != 200 {
                    services.Logger.Printf("ERROR While Moving data to %s -- %s.  \n Continuing...", to.Address, response.StatusMessage())
                }

            case err := <- toErrorChan :
                services.Logger.Printf("ERROR While Moving data to %s -- %s", to.Address, err)
                return moved, err
            default :
                if count > 30 {
                   services.Logger.Printf("GAH. Waited 30 seconds for completion.  there seems to be a problem,.")
                   return moved, fmt.Errorf("GAH. Waited 30 seconds for completion.  there seems to be a problem.") 
                }
                time.Sleep(1*time.Second) 
        }
        count++
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
    if err != nil {
        return err
    }

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
        if len(entry.Partitions) < min {
            if smallest == nil {
                smallest = entry
            } else if len(entry.Partitions) < len(smallest.Partitions) {
                smallest = entry
            }

            if largest == nil {
                largest = entry
            } else if len(entry.Partitions) > len(largest.Partitions) {
                largest = entry
            }
        }
    }
    if smallest == nil || largest == nil {
        services.Logger.Printf("Cluster appears to be balanced")
        return nil
    }

    services.Logger.Printf("Moving from %s to %s", largest.Id(), smallest.Id())


    return nil
}




