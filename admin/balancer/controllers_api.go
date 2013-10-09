package balancer

import (
	"fmt"
	"github.com/trendrr/goshire-shards/shards"
	"github.com/trendrr/goshire/cheshire"
	clog "github.com/trendrr/goshire/log"
	"log"
	"time"
)

func init() {
	cheshire.RegisterApi("/api/log", "GET", ConsoleLog)
	cheshire.RegisterApi("/api/service", "GET", ServiceGet)
	cheshire.RegisterApi("/api/service/update", "GET", ServiceUpdate)
	cheshire.RegisterApi("/api/service/rebalance", "POST", ServiceRebalance)
	cheshire.RegisterApi("/api/service/sub/checkins", "GET", ServiceCheckins)
	cheshire.RegisterApi("/api/shard/new", "PUT", ShardNew)
}

func ServiceGet(txn *cheshire.Txn) {
	log.Println("Service get!")
	routerTable, ok := Servs.RouterTable(txn.Params().MustString("service", ""))
	if !ok {
		cheshire.SendError(txn, 406, "Service param missing or service not found")
		return
	}
	res := cheshire.NewResponse(txn)
	res.Put("router_table", routerTable.ToDynMap())
	txn.Write(res)
}

// Updates the router table on all entries
func ServiceUpdate(txn *cheshire.Txn) {
	log.Println("Service checkin registered")
	routerTable, ok := Servs.RouterTable(txn.Params().MustString("service", ""))
	if !ok {
		cheshire.SendError(txn, 406, "Service param missing or service not found")
		return
	}

	for _, e := range routerTable.Entries {
		_, updatedlocal, updatedremote, err := EntryCheckin(routerTable, e)
		if err != nil {
			Servs.Logger.Printf("Error contacting %s -- %s", e.Id(), err)
			continue
		}
		if updatedremote {
			Servs.Logger.Printf("Updated router table on %s", e.Id())
		} else if updatedlocal {
			Servs.Logger.Printf("Updated local router table from %s", e.Id())

		} else {
			Servs.Logger.Printf("Router table upto date on %s", e.Id())
		}
	}
	//send a router table update, in case anything changed
	res := cheshire.NewResponse(txn)
	routerTable, _ = Servs.RouterTable(routerTable.Service)
	res.Put("router_table", routerTable.ToDynMap())
	txn.Write(res)
}

// checks into all the services every 30 seconds, sends an updated rt every time
func ServiceCheckins(txn *cheshire.Txn) {
	log.Println("Service checkin registered")
	routerTable, ok := Servs.RouterTable(txn.Params().MustString("service", ""))
	if !ok {
		cheshire.SendError(txn, 406, "Service param missing or service not found")
		return
	}
	for {

		for _, e := range routerTable.Entries {
			err := EntryContact(e)
			if err != nil {
				Servs.Logger.Printf("Error contacting %s -- %s", e.Id(), err)
				continue
			}
			Servs.Logger.Printf("Successfully Pinged %s", e.Id())

		}
		//send a router table update, in case anything changed
		res := cheshire.NewResponse(txn)
		res.SetTxnStatus("continue")

		routerTable, _ = Servs.RouterTable(routerTable.Service)
		res.Put("router_table", routerTable.ToDynMap())
		_, err := txn.Write(res)
		if err != nil {
			//try and write an error response
			cheshire.SendError(txn, 510, fmt.Sprintf("%s", err))
			return
		}

		time.Sleep(45 * time.Second)
		//refresh the router table on every pass
		routerTable, ok = Servs.RouterTable(txn.Params().MustString("service", ""))
		if !ok {
			cheshire.SendError(txn, 406, "Problem finding router table")
			return
		}
	}

}

// Handles the rebalance operation.
// will push an updated router table everytime it changes.
func ServiceRebalance(txn *cheshire.Txn) {
	routerTable, ok := Servs.RouterTable(txn.Params().MustString("service", ""))
	if !ok {
		cheshire.SendError(txn, 406, "Service param missing or service not found")
		return
	}

	maxPartition := txn.Params().MustInt("max", 1)
	for i := 0; i < maxPartition; i++ {
		err := RebalanceSingle(Servs, routerTable)
		if err != nil {
			Servs.Logger.Printf("ERROR %s", err)
			cheshire.SendError(txn, 501, "Problem rebalancing")
			return
		}
		res := cheshire.NewResponse(txn)
		res.SetTxnContinue()
		//Write the new router table.
		res.Put("router_table", routerTable.ToDynMap())

		txn.Write(res)
		time.Sleep(3 * time.Second)
	}
	
	
}

// Gets any logging messages from the Servs.Events
// sends to client
func ConsoleLog(txn *cheshire.Txn) {
	msgChan := make(chan clog.LoggerEvent, 10)
	Servs.Logger.Listen(msgChan)
	defer Servs.Logger.Unlisten(msgChan)
	log.Println("Log Listener Registered")
	for {
		msg := <-msgChan
		res := cheshire.NewResponse(txn)
		res.SetTxnStatus("continue")
		res.PutWithDot("event.type", msg.Type)
		res.PutWithDot("event.message", msg.Message)
		_, err := txn.Write(res)
		if err != nil {
			//try and write an error response
			cheshire.SendError(txn, 510, fmt.Sprintf("%s", err))
			break
		}
	}
	log.Println("Log Listener unregistered")
}

// Creates a new shard.  Does not register any partitions to it, unless the router table has no entries. in which case this
// gets all the partitions
func ShardNew(txn *cheshire.Txn) {
	routerTable, ok := Servs.RouterTable(txn.Params().MustString("service", ""))

	if !ok {
		cheshire.SendError(txn, 406, "Service param missing or service not found")
		return
	}

	Servs.Logger.Printf("Creating new shard for service: %s", routerTable.Service)

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

	binPort, ok := txn.Params().GetInt("bin_port")
	if !ok {
		cheshire.SendError(txn, 406, "bin_port param missing")
		return
	}

	entry := &shards.RouterEntry{
		Address:    address,
		JsonPort:   jsonPort,
		HttpPort:   httpPort,
		BinPort: binPort,
		Partitions: make([]int, 0),
	}

	//check if we can connect!
	Servs.Logger.Printf("Attempting to connect to new entry...")
	err := EntryContact(entry)
	if err != nil {
		Servs.Logger.Printf("ERROR: ", err)
		cheshire.SendError(txn, 406, fmt.Sprintf("Unable to contact %s:%d Error(%s)", entry.Address, entry.HttpPort, err))
		return
	}
	Servs.Logger.Printf("Success!")

	if len(routerTable.Entries) == 0 {
		totalPartitions := routerTable.TotalPartitions

		//first entry, giving it all the partitions
		Servs.Logger.Printf("First Entry! giving it all %d partitions", totalPartitions)
		partitions := make([]int, 0)
		for p := 0; p < totalPartitions; p++ {
			partitions = append(partitions, p)
		}
		entry.Partitions = partitions
	} else {
		// not the first entry,
	}

	routerTable, err = routerTable.AddEntries(entry)
	if err != nil {
		cheshire.SendError(txn, 501, fmt.Sprintf("Error on add entry %s", err))
		return
	}

	Servs.Logger.Printf("Successfully created new entry: %s", entry.Id())

	Servs.SetRouterTable(routerTable)

	Servs.Logger.Printf("Attempting to sending new router table to entry")
	_, _, _, err = EntryCheckin(routerTable, entry)

	if err != nil {
		Servs.Logger.Printf("ERROR %s", err)
		cheshire.SendError(txn, 501, fmt.Sprintf("Error on entry checkin %s", err))
		return
	}

	res := cheshire.NewResponse(txn)
	res.Put("router_table", routerTable.ToDynMap())
	txn.Write(res)
}
