package main

import (
    "log"
    "github.com/trendrr/cheshire-golang/cheshire"
        "github.com/trendrr/cheshire-golang/partition"
    "github.com/dustismo/cheshire-balancer/balancer"
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
    
    balancer.Servs.DataDir = *dataDir
    balancer.Servs.Load()

    balancer.Servs.SetRouterTable(partition.NewRouterTable("Test"))

    //
    log.Println("Starting")
    //starts listening on all configured interfaces
    bootstrap.Start()
    

}