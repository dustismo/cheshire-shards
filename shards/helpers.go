package shards

import (
	"fmt"
	"github.com/trendrr/goshire/cheshire"
	"github.com/trendrr/goshire/client"
	"log"
	"time"
)

// requests the router table via http from the router table entry
func RequestRouterTableEntry(entry *RouterEntry) (*RouterTable, error) {
	c := client.NewHttp(fmt.Sprintf("http://%s:%d",entry.Address, entry.HttpPort))
	rt, err := RequestRouterTable(c)
	return rt, err
}

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

// Finds the partition in the request, then
//
// Checks the validity of the partition, and checks that the current node is responsible
// Also checks whether the partition is locked.
//
// This will send the appropriate response on error
func PartitionParam(txn *cheshire.Txn) (int, bool) {
	partition := 0

	if txn.Request.Shard != nil && txn.Request.Shard.Partition >= 0 {
		partition = txn.Request.Shard.Partition
	} else {
		p, ok := txn.Params().GetInt(P_PARTITION)
		if !ok {
			log.Println("Partition not found1")
			cheshire.SendError(txn, 406, fmt.Sprintf("partition param (%s) is manditory", P_PARTITION))
			return 0, false
		}
		partition = p
	}

	//check the partition is my responsibility
	ok, locked := SM().MyResponsibility(partition)
	if locked {

		log.Println("Partition locked")
		cheshire.SendError(txn, E_PARTITION_LOCKED, fmt.Sprintf("partition is locked"))
		return 0, false
	}
	if !ok {

		str := fmt.Sprintf("Partition %d is not my Partition", partition)
		log.Println(str)
		cheshire.SendError(txn, E_NOT_MY_PARTITION, str)
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

	revision := int64(0)

	if txn.Request.Shard != nil && txn.Request.Shard.Revision > int64(0) {
		revision = txn.Request.Shard.Revision
	} else {
		r, ok := txn.Params().GetInt64(P_REVISION)
		if !ok {
			return false, true
		}
		revision = r
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



// Checkin to an entry.  will update their router table if it is out of date.  will update our router table if out of date.
// returns the updated router table, updated, error
// return rt, self updated, remote updated, error
func RouterTableSync(routerTable *RouterTable, entry *RouterEntry) (*RouterTable, bool, bool, error) {
        log.Println("ENTRY router table sync, %s", entry.Id())
        // make sure our routertable is up to date.
        response, err := client.HttpApiCallSync(
            fmt.Sprintf("%s:%d", entry.Address, entry.HttpPort),
            cheshire.NewRequest(CHECKIN, "GET"),
            5 * time.Second)
        if err != nil {
            return routerTable, false, false, fmt.Errorf("ERROR While contacting %s -- %s", entry.Address, err)
        }

        rev := response.MustInt64("rt_revision", 0)
        log.Printf("Checkin revision from %s Revision %d", entry.Id(), rev)

        if rev == routerTable.Revision {
            return routerTable, false, false, nil
        }

        if rev < routerTable.Revision {
            //updating remote.
            //set the new routertable.

            log.Printf("UPDATING router table on %s", entry.Id())
            req := cheshire.NewRequest(ROUTERTABLE_SET, "POST")
            req.Params().Put("router_table", routerTable.ToDynMap())

            response, err = client.HttpApiCallSync(
                fmt.Sprintf("%s:%d", entry.Address, entry.HttpPort),
                req,
                5 * time.Second)
            if err != nil {
                return routerTable, false, false, fmt.Errorf("ERROR While contacting for router table update %s -- %s", entry.Address, err)
            }
            if response.StatusCode() != 200 {
                return routerTable, false, false, fmt.Errorf("Error trying to Set router table %s -- %s", entry.Address, response.StatusMessage())
            }
            return routerTable, false, true, nil
        } else {
            //updating local 

            log.Printf("Found updated router table at: %s", entry.Id)

            rt, err := RequestRouterTableEntry(entry)
            if err != nil {
                return routerTable, false, false, err
            }

            // //get the new routertable.
            // response, err = client.HttpApiCallSync(
            //     fmt.Sprintf("%s:%d", entry.Address, entry.HttpPort),
            //     cheshire.NewRequest(shards.ROUTERTABLE_GET, "GET"),
            //     5 * time.Second)
            // if err != nil {
            //     return routerTable, false, false, fmt.Errorf("ERROR While contacting %s -- %s", entry.Address, err)
            // }
            // mp, ok := response.GetDynMap("router_table")
            // if !ok {
            //     return routerTable, false, false, fmt.Errorf("ERROR from %s -- BAD ROUTER TABLE RESPONSE %s", entry.Address, response)
            // }

            // rt, err := shards.ToRouterTable(mp)
            // if err != nil {
            //     return routerTable, false, false, fmt.Errorf("ERROR While parsing router table %s -- %s", entry.Address, err)
            // }

            log.Printf("SUCCESSFULLY update router table to revision %d", rt.Revision)
            routerTable = rt
            return routerTable, true, false, nil
        } 
} 