package main

import (
    "log"
    "github.com/trendrr/cheshire-golang/cheshire"
        "github.com/trendrr/cheshire-golang/partition"
    "github.com/dustismo/cheshire-balancer/balancer"
    "github.com/trendrr/cheshire-golang/cheshire/impl/gocache"
    "flag"

)


//command line args
var (
    configFilename = flag.String("config-filename", "config.yaml", "filename of the config")
    dataDir = flag.String("data-dir", "data", "The local directory where data should be stored")
)



func main() {
    flag.Parse()
    bootstrap := cheshire.NewBootstrapFile(*configFilename)
    
    //Setup our cache.  this uses the local cache 
    cache := gocache.New(10, 10)
    bootstrap.AddFilters(cheshire.NewSession(cache, 3600))

    balancer.Servs.DataDir = *dataDir
    balancer.Servs.Load()

    balancer.Servs.SetRouterTable(partition.NewRouterTable("Test"))

    //
    log.Println("Starting")
    //starts listening on all configured interfaces
    bootstrap.Start()
    

}