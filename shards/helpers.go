package shards

import (
	"fmt"
	"github.com/trendrr/goshire/cheshire"
	"github.com/trendrr/goshire/client"
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
