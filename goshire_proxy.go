package main

//
// An example main for the router process.
//
import (
    "log"
    // "github.com/trendrr/goshire/cheshire"
    "github.com/trendrr/goshire-shards/proxy"
    // "os"
    "flag"
    "strings"
    "fmt"
)


var (
    config = flag.String("config", "proxy_config.yaml", "path to the yaml config file")
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

    r := proxy.NewServerFile(*config)

    log.Println(*seedAddress)
    addresses := make([]string, 0)
    for _,add := range(strings.Split(*seedAddress, ",")) {
        if !strings.HasPrefix(add, "http://") {
            add = fmt.Sprintf("http://%s",add)
        }
        addresses = append(addresses, add)
    }
    //now add the services.
    r.Seeds(addresses ...)
    

    log.Println("Starting")
    //starts listening on all configured interfaces
    r.Start()
}
