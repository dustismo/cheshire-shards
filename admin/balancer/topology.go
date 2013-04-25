package balancer


import (
    // "log"
    "github.com/trendrr/cheshire-golang/cheshire"
    shards "github.com/dustismo/cheshire-shards/shards"
    "fmt"
    // "io/ioutil"
    "time"
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
    entries, err := routerTable.PartitionEntries(partition)
    if err != nil {
        return err
    }

    // Lock partitions
    for _, e := range(entries) {
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



// Moves data from one server to another
// Does the following:
// 0. Update router table
// 1. Lock all necessary clients 
// 2. Move Data 
// 3. Update router table on servers
// 4. Unlock
func MovePartition(services *Services, routerTable *shards.RouterTable, partition int, from, to *shards.RouterEntry) error {
    



    //update router table if needed
    routerTable, _ = RouterTableUpdate(services, routerTable, 3)

    err := LockPartition(services, routerTable, partition)
    if err != nil {
        return err
    }

    //Unlock partition no matter what
    defer UnlockPartition(services, routerTable, partition)

    //Move the data!

    //create a new json connection
    fromClient := cheshire.NewJsonClient(from.Address, from.JsonPort)
    err = fromClient.Connect()
    if err != nil {
        return err
    }
    defer fromClient.Close()
    
    toClient := cheshire.NewJsonClient(to.Address, to.JsonPort)
    err = toClient.Connect()
    if err != nil {
        return err
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

    moved := 0

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
            return err

        case response := <- toResponseChan :
            if response.StatusCode() != 200 {
                services.Logger.Printf("ERROR While Moving data from %s -- %s.  \n Continuing...", from.Address, response.StatusMessage())
            }

            //do nothing, 
        case err := <- toErrorChan :
            services.Logger.Printf("ERROR While Moving data to %s -- %s", to.Address, err)
            return err
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
                return err
            default :
                if count > 30 {
                   services.Logger.Printf("GAH. Waited 30 seconds for completion.  there seems to be a problem,.")
                   return fmt.Errorf("GAH. Waited 30 seconds for completion.  there seems to be a problem.") 
                }
                time.Sleep(1*time.Second) 
        }
        count++
    }


    //update the router table.




    //Delete the data on the from server.
    request = cheshire.NewRequest(shards.PARTITION_DELETE, "DELETE")    
    request.Params().Put("partition", partition)

    services.Logger.Printf("NOW Deleting the partition from the origin server %s", from.Address)
    response, err := fromClient.ApiCallSync(request, 300*time.Second)
    if response.StatusCode() != 200 {
        services.Logger.Printf("ERROR While deleting partition: %s", response.StatusMessage())
    }




    return nil
}

// Checks with entries to see if an updated router table is available.
func RouterTableUpdate(services *Services, routerTable *shards.RouterTable, maxChecks int) (*shards.RouterTable, bool) {
    checks := 0
    updated := false
    for _,e := range(routerTable.Entries) {
        if checks >= maxChecks {
            break
        }

        checks++
        // make sure our routertable is up to date.
        response, err := cheshire.HttpApiCallSync(
            fmt.Sprintf("%s:%d", e.Address, e.HttpPort),
            cheshire.NewRequest(shards.CHECKIN, "GET"),
            5 * time.Second)
        if err != nil {
            services.Logger.Printf("ERROR While contacting %s -- %s", e.Address, err)
            continue
        }

        rev := response.MustInt64("rt_revision", 0)
        if rev <= routerTable.Revision {
            continue
        }
        services.Logger.Printf("Found updated router table at: %s", e.Address)
        //get the new routertable.
        response, err = cheshire.HttpApiCallSync(
            fmt.Sprintf("%s:%d", e.Address, e.HttpPort),
            cheshire.NewRequest(shards.ROUTERTABLE_GET, "GET"),
            5 * time.Second)
        if err != nil {
            services.Logger.Printf("ERROR While contacting %s -- %s", e.Address, err)
            continue
        }
        mp, ok := response.GetDynMap("router_table")
        if !ok {
            services.Logger.Printf("ERROR from %s -- BAD ROUTER TABLE RESPONSE %s", e.Address, response)
            continue   
        }

        rt, err := shards.ToRouterTable(mp)
        if err != nil {
            services.Logger.Printf("ERROR While parsing router table %s -- %s", e.Address, err)
            continue
        }

        services.Logger.Printf("SUCCESSFULLY update router table to revision %d", rt.Revision)
        updated = true
        routerTable = rt
    }
    return routerTable, updated
}


