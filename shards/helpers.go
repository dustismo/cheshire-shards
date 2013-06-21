package shards

import (
	"fmt"
	"github.com/trendrr/goshire/cheshire"
	"github.com/trendrr/goshire/client"
	"log"
	"time"
)

// Finds the RouterTable from the given client
//
func RequestRouterTable(c client.Client) (*RouterTable, error) {
	response, err := c.ApiCallSync(cheshire.NewRequest(ROUTERTABLE_GET, "GET"), 10*time.Second)
	if err != nil {
		return nil, err
	}
	if response.StatusCode() != 200 {
		return nil, fmt.Errorf("Error from server %d %s", response.StatusCode(), response.StatusMessage())
	}

	mp, ok := response.GetDynMap("router_table")
	if !ok {
		return nil, fmt.Errorf("No router_table in response : %s", response)
	}

	table, err := ToRouterTable(mp)
	if err != nil {
		return nil, err
	}
	return table, nil
}

// Finds the partition in the params, then
//
// Checks the validity of the partition, and checks that the current node is responsible
// Also checks whether the partition is locked.
//
// This will send the appropriate response on error
func PartitionParam(txn *cheshire.Txn) (int, bool) {
	partition, ok := txn.Params().GetInt(P_PARTITION)
	if !ok {
		cheshire.SendError(txn, 406, fmt.Sprintf("partition param is manditory"))
		return 0, false
	}

	//check the partition is my responsibility
	ok, locked := SM().MyResponsibility(partition)
	if locked {
		cheshire.SendError(txn, E_PARTITION_LOCKED, fmt.Sprintf("partition is locked"))
		return 0, false
	}
	if !ok {
		cheshire.SendError(txn, E_NOT_MY_PARTITION, fmt.Sprintf("Not my partition"))
		return 0, false
	}

	return partition, true
}

// Will check the router revision param
// will send appropriate response if revision doesnt match ours
// returns paramExists, and OK
// if not OK response will be sent
// if not paramExists, no response is sent and ok is true (revision is not manditory)
func RouterRevisionParam(txn *cheshire.Txn) (bool, bool) {

	revision, ok := txn.Params().GetInt64(P_REVISION)
	if !ok {
		return false, true
	}

	rt, err := SM().RouterTable()
	if err != nil {
		log.Println(err)
		// Uhh, I think we say this is ok
		return true, true
	}

	if rt.Revision == revision {
		//great
		return true, true
	}

	if rt.Revision < revision {
		cheshire.SendError(txn, E_SEND_ROUTER_TABLE, fmt.Sprintf("My Router Table is out of date, please send"))
	}

	if rt.Revision > revision {
		cheshire.SendError(txn, E_ROUTER_TABLE_OLD, fmt.Sprintf("Your Router Table is out of date, please update"))
	}
	return true, false
}
