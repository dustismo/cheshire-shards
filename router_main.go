package main

//
// An example main for the router process.
//
import (
    "log"
    // "github.com/trendrr/goshire/cheshire"
    "github.com/trendrr/goshire-shards/router"
    // "os"
    "flag"
    "strings"
    "fmt"
)


var (
    config = flag.String("config", "router_config.yaml", "path to the yaml config file")
    seedAddress = flag.String("seed-http-address", "", "http address to seed a service. comma delimit multiple.")
)

func main() {

    // lf, err := os.Create("router.log")
    // if err != nil {
    //     log.Panicf("Coulding open log file -- %s", err)
    // }
    // //log to a file
    // log.SetOutput(lf)
    flag.Parse()

    r := router.NewServerFile("router_config.yaml")

    log.Println(*seedAddress)
    addresses := make([]string, 0)
    for _,add := range(strings.Split(*seedAddress, ",")) {
        addresses = append(addresses, fmt.Sprintf("http://%s",add))
    }
    //now add the services.
    err := r.AddService(addresses ...)
    if err != nil {
        log.Panic(err)
    }


    log.Println("Starting")
    //starts listening on all configured interfaces
    r.Start()
}
