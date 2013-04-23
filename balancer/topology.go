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
    if err != nil {
        return err
    }

    toClient, err := cheshire.NewJsonClient(to.Address, to.JsonPort)

    request := cheshire.NewRequest(partition.DATA_PULL, "GET")
    request.Params().Put("partition", partition)
    
    responseChan := make(chan *Response)
    errorChan := make(chan error)

    req *Request, responseChan chan *Response, errorChan chan error

    fromClient.ApiCall(
        request,
        responseChan,
        errorChan        
    )

    response, err := cheshire.HttpApiCallSync(
        fmt.Sprintf("%s:%d", e.Address, e.HttpPort),
        cheshire.NewRequest(partition.PARTITION_LOCK, "GET"),
        5 * time.Second)
    if err != nil {
        services.Logger.Printf("ERROR While contacting %s -- %s", e.Address, err)
        continue
    }


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


