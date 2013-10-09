package main

import (
	"github.com/trendrr/goshire/cheshire"
	"log"
	// "github.com/trendrr/goshire-shards/shards"
	"flag"
	"github.com/trendrr/goshire-shards/admin/balancer"
	"github.com/trendrr/goshire/cheshire/impl/gocache"
	// "time"
	// "fmt"
)

//command line args
var (
	configFilename = flag.String("config", "goshire_admin.yaml", "filename of the config")
	dataDir        = flag.String("data-dir", "data", "The local directory where data should be stored")
)

func main() {
	flag.Parse()
	bootstrap := cheshire.NewBootstrapFile(*configFilename)

	//Setup our cache.  this uses the local cache
	cache := gocache.New(10, 10)
	bootstrap.AddFilters(cheshire.NewSession(cache, 3600))

	balancer.Servs.DataDir = *dataDir
	balancer.Servs.Load()

	// testrt := shards.NewRouterTable("Test")
	// balancer.Servs.SetRouterTable(testrt)

	// //try creating entry
	// entry := &shards.RouterEntry{
	//     Address : "localhost",
	//     JsonPort : 8009,
	//     HttpPort : 8010,
	//     Partitions : make([]int, 0),
	// }

	// log.Println("********************* ADD ENTRY")
	// testrt.AddEntries(entry)

	//
	log.Println("Starting")
	// go func() {
	//     c := time.Tick(5 * time.Second)
	//     for now := range c {
	//         str := fmt.Sprintf("%v %s\n", now, "Something something")
	//         balancer.Servs.Logger.Emit("test", str)
	//         balancer.Servs.Logger.Println("TESTING LOG")
	//     }
	// }()

	//starts listening on all configured interfaces
	bootstrap.Start()

}
