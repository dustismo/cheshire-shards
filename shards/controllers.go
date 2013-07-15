package shards

import (
	"fmt"
	"github.com/trendrr/goshire/cheshire"
	"log"
	"time"
	"net/http"
)

//the global shard manager
var sm *Manager

// This is the global shard manager.
// it is set up when you call shards.RegisterServiceControllers
func SM() *Manager {
	return sm
}

// Sets the partitioner and registers the necessary
// controllers
func RegisterServiceControllers(man *Manager) {
	sm = man
	//register the controllers.
	cheshire.RegisterApi(ROUTERTABLE_GET, "GET", GetRouterTable)
	cheshire.RegisterApi(ROUTERTABLE_SET, "POST", SetRouterTable)
	cheshire.RegisterApi(PARTITION_LOCK, "POST", Lock)
	cheshire.RegisterApi(PARTITION_UNLOCK, "POST", Unlock)
	cheshire.RegisterApi(CHECKIN, "GET", Checkin)
	cheshire.RegisterApi(PARTITION_IMPORT, "POST", PartitionImport)
	cheshire.RegisterApi(PARTITION_EXPORT, "GET", PartitionExport)
	cheshire.RegisterApi(PARTITION_DELETE, "DELETE", PartitionDelete)
}

func Checkin(txn *cheshire.Txn) {
	table, err := SM().RouterTable()

	revision := int64(0)
	if err == nil {
		revision = table.Revision
	}
	response := cheshire.NewResponse(txn)
	response.Put("rt_revision", revision)
	response.Put("ts", time.Now())
	txn.Write(response)
}

func GetRouterTable(txn *cheshire.Txn) {
	log.Println("GetRouterTable")
	tble, err := SM().RouterTable()
	if err != nil {
		cheshire.SendError(txn, 506, fmt.Sprintf("Error: %s", err))
		return
	}
	response := cheshire.NewResponse(txn)
	response.Put("router_table", tble.ToDynMap())
	txn.Write(response)
}

func SetRouterTable(txn *cheshire.Txn) {
	rtmap, ok := txn.Params().GetDynMap("router_table")
	if !ok {
		cheshire.SendError(txn, 406, "No router_table")
		return
	}

	rt, err := ToRouterTable(rtmap)
	if err != nil {
		cheshire.SendError(txn, 406, fmt.Sprintf("Unparsable router table (%s)", err))
		return
	}

	_, err = SM().SetRouterTable(rt)
	if err != nil {
		cheshire.SendError(txn, 406, fmt.Sprintf("Unable to set router table (%s)", err))
		return
	}
	response := cheshire.NewResponse(txn)
	txn.Write(response)
}

func Lock(txn *cheshire.Txn) {

	partition, ok := txn.Params().GetInt("partition")
	if !ok {
		cheshire.SendError(txn, 406, fmt.Sprintf("partition param missing"))
		return
	}

	err := SM().LockPartition(partition)
	if err != nil {
		//now send back an error
		cheshire.SendError(txn, 406, fmt.Sprintf("Unable to lock partitions (%s)", err))
		return
	}
	response := cheshire.NewResponse(txn)
	txn.Write(response)
}

func Unlock(txn *cheshire.Txn) {
	partition, ok := txn.Params().GetInt("partition")
	if !ok {
		cheshire.SendError(txn, 406, fmt.Sprintf("partition param missing"))
		return
	}

	err := SM().UnlockPartition(partition)
	if err != nil {
		//now send back an error
		cheshire.SendError(txn, 406, fmt.Sprintf("Unable to lock partitions (%s)", err))
		return
	}
	response := cheshire.NewResponse(txn)
	txn.Write(response)
}

func PartitionDelete(txn *cheshire.Txn) {
	partition, ok := txn.Params().GetInt("partition")
	if !ok {
		cheshire.SendError(txn, 406, fmt.Sprintf("partition param is manditory"))
		return
	}

	err := SM().shard.DeletePartition(partition)	
	if err == nil {
		cheshire.SendSuccess(txn)
	} else {
		cheshire.SendError(txn, 501, fmt.Sprintf("Error during delete %s", err))
	}
}

func PartitionExport(txn *cheshire.Txn) {
	// make sure this is an http request.
	hw, ok := txn.Writer.(*cheshire.HttpWriter)
	if !ok {
		cheshire.SendError(txn, 406, fmt.Sprintf("Partition Export is only available as an http request"))
		return
	}
	writer := hw.Writer

	partition, ok := txn.Params().GetInt("partition")
	if !ok {
		cheshire.SendError(txn, 406, fmt.Sprintf("partition param is manditory"))
		return
	}
	
	finishedChan := make(chan int64)
	errorChan := make(chan error)

	go SM().shard.ExportPartition(partition, writer, finishedChan, errorChan)
	select {
	case bytes := <-finishedChan:
		log.Println("Successfully exported %d bytes for partition %d", bytes, partition)
		return
	case err := <-errorChan:
		log.Println("ERROR exporting bytes for partition %d -- %s", partition, err)
		return
	}
}

// Requests that this shard import a partition from the given source
// Controller will issue the import request.  Will send back the
// total number of bytes imported, and will close the txn once the data completes or an
// error is thrown
// Requires params:
// partition => The partition to import
// source => the http address to import from.  in the form http://address:port
func PartitionImport(txn *cheshire.Txn) {
	partition, ok := txn.Params().GetInt("partition")
	if !ok {
		cheshire.SendError(txn, 406, fmt.Sprintf("partition param is manditory"))
		return
	}

	//The http source address
	source, ok := txn.Params().GetString("source")
	if !ok {
		cheshire.SendError(txn, 406, fmt.Sprintf("source param is manditory"))
		return
	}	


	//issue the import request..
	address := fmt.Sprintf("%s%s?partition=%d", source, PARTITION_EXPORT, partition)
	log.Println("Attempting to import partition %d from %s",partition, address)

	resp, err := http.Get(address)
	if err != nil {
		// handle error
		cheshire.SendError(txn, resp.StatusCode, resp.Status)
		return
	}

	finishedChan := make(chan int64)
	errorChan := make(chan error)
	
	defer resp.Body.Close()

	go SM().shard.ImportPartition(partition, resp.Body, finishedChan, errorChan)
	select {
	case bytes := <-finishedChan:
		log.Println("Successfully imported %d bytes for partition %d", bytes, partition)
		response := cheshire.NewResponse(txn)
		response.Put("bytes", bytes)
		txn.Write(response)
	case err := <-errorChan:
		str := fmt.Sprintf("ERROR importing bytes for partition %d -- %s", partition, err)
		cheshire.SendError(txn, 501, str)
	}

}