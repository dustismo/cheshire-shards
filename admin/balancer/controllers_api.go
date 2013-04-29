package balancer

import (
    "github.com/trendrr/cheshire-golang/cheshire"
    clog "github.com/trendrr/cheshire-golang/log"
    shards "github.com/dustismo/cheshire-shards/shards"
    "log"
    "fmt"
)

func init() {
    cheshire.RegisterApi("/api/log", "GET", ConsoleLog)
}

// Gets any logging messages from the Servs.Events
// sends to client
func ConsoleLog(txn *cheshire.Txn) {
    msgChan := make(chan clog.LoggerEvent, 10)
    Servs.Logger.Listen(msgChan)
    defer Servs.Logger.Unlisten(msgChan)
    log.Println("Log Listener Registered")
    for {
        msg := <- msgChan
        res := cheshire.NewResponse(txn)
        res.SetTxnStatus("continue")
        res.PutWithDot("event.type", msg.Type)
        res.PutWithDot("event.message", msg.Message)
        _, err := txn.Write(res)
        if err != nil {
            //try and write an error response
            cheshire.SendError(txn, 510, fmt.Sprintf("%s",err))
            break   
        }
    }
    close(msgChan)
    log.Println("Log Listener unregistered")
}

// Creates a new shard.  Does not register any partitions to it, unless the router table has no entries. in which case this
// gets all the partitions
func NewShard(txn *cheshire.Txn) {
    routerTable, ok := Servs.RouterTable(txn.Params().MustString("service", ""))
    if !ok {
        cheshire.SendError(txn, 406, "Service param missing or service not found")
        return
    }

    address, ok := txn.Params().GetString("address")
    if !ok {
        cheshire.SendError(txn, 406, "address param missing")
        return
    }

    jsonPort, ok := txn.Params().GetInt("json_port")
    if !ok {
        cheshire.SendError(txn, 406, "json_port param missing")
        return
    }

    httpPort, ok := txn.Params().GetInt("http_port")
    if !ok {
        cheshire.SendError(txn, 406, "http_port param missing")
        return
    }

    entry := &shards.RouterEntry{
        Address : address,
        JsonPort : jsonPort,
        HttpPort : httpPort,
        Partitions : make([]int, 0),
    }





    if len(routerTable.Entries) == 0 {
        //first entry, giving it all the partitions
        partitions := make([]int, 0)
        for p := 0; p < routerTable.TotalPartitions; p++ {
            partitions = append(partitions, p)
        }
        entry.Partitions = partitions
    } else {
        // not the first entry, 
    }
    routerTable, err := routerTable.AddEntries(entry)
    if err != nil {
        cheshire.SendError(txn, 501, fmt.Sprintf("Error on add entry %s", err))
        return
    }


    Servs.SetRouterTable(routerTable)

    res := cheshire.NewResponse(txn)
    res.Put("router_table", routerTable.ToDynMap())
    txn.Write(res)
}

