package main

//
// An example main for the router process.
//
import (
    "log"
    // "github.com/trendrr/goshire/cheshire"
    "github.com/trendrr/goshire-shards/router"
    // "os"
)



func main() {

    // lf, err := os.Create("router.log")
    // if err != nil {
    //     log.Panicf("Coulding open log file -- %s", err)
    // }
    // //log to a file
    // log.SetOutput(lf)

    r := router.NewServerFile("router_config.yaml")



    //now add the services.
    err := r.AddService("http://localhost:8010")
    if err != nil {
        log.Panic(err)
    }


    log.Println("Starting")
    //starts listening on all configured interfaces
    r.Start()
}
