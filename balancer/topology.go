package balancer


// All the operations necessary for rebalance and topology changes



// Moves data from one server to another
// Does the following:
// 0. Update router table
// 1. Lock all necessary clients 
// 2. Move Data 
// 3. Update router table on servers
// 4. Unlock
func MovePartition(services *Services, routerTable *RouterTable, partition int, from, to *RouterEntry) error {
    

    entries := routerTable.PartitionEntries(partition)
    
    //update router table if needed
    routerTable, _ := RouterTableUpdate(routerTable, 3)

    // Lock partitions
    for e := range(routerTable.PartitionEntries(partition)) {
        response, err := cheshire.HttpApiCallSync(
            fmt.Sprintf("%s:%d", e.Address, e.HttpPort),
            cheshire.NewRequest(partition.PARTITION_LOCK, "GET"),
            5 * time.Second)
        if err != nil {
            services.Logger.Printf("ERROR While contacting %s -- %s", e.Address, err)
            continue
        }
    }


    //Move the data!

    //create a new json connection
    fromClient, err := cheshire.NewJsonClient(from.Address, from.JsonPort)
    defer fromClient.Close()
    if err != nil {
        return err
    }

    toClient, err := cheshire.NewJsonClient(to.Address, to.JsonPort)
    defer toClient.Close()
    if err != nil {
        return err
    }

    request := cheshire.NewRequest(partition.DATA_PULL, "GET")
    request.Params().Put("partition", partition)
    
    responseChan := make(chan *Response, 10)
    errorChan := make(chan error)

    fromClient.ApiCall(
        request,
        responseChan,
        errorChan        
    )
    moved := 0

    toResponseChan := make(chan *Response, 10)
    toErrorChan := make(chan error)

    for {
        select {
        case response := <- responseChan :
            //TODO: send the data to the toClient
            request := cheshire.NewRequest(partition.DATA_PUSH, "PUT")
            d, ok := request.GetDynMap("data")
            if !ok {
                services.Logger.Printf("ERROR: packet missing data :: %s ", request)
                continue
            }
            request.Params().put("data", d)
            request.Params().put("partition", partition)
            toClient.ApiCall(
                request,
                toResponseChan,
                toErrorChan
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
            //do nothing, 
        case err := <- toErrorChan :
            services.Logger.Printf("ERROR While Moving data to %s -- %s", to.Address, err)
            return err
        }
    }

    // Now delete the data



}

// Checks with entries to see if an updated router table is available.
func RouterTableUpdate(routerTable *RouterTable, maxChecks int) (*RouterTable, bool) {
    checks := 0
    updated = false
    for e := range(routerTable.Entries) {
        if checks >= maxChecks {
            break
        }

        checks++
        // make sure our routertable is up to date.
        response, err := cheshire.HttpApiCallSync(
            fmt.Sprintf("%s:%d", e.Address, e.HttpPort),
            cheshire.NewRequest(partition.CHECKIN, "GET"),
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
        response, err := cheshire.HttpApiCallSync(
            fmt.Sprintf("%s:%d", e.Address, e.HttpPort),
            cheshire.NewRequest(partition.ROUTERTABLE_GET, "GET"),
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

        rt, err := partition.ToRouterTable(mp)
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


