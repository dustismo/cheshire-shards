package main

//
// An example main for the router process.
//
import (
    "log"
    "github.com/trendrr/goshire/cheshire"
    "github.com/trendrr/goshire-shards/router"
       
)



func main() {
    
    bootstrap := cheshire.NewBootstrapFile("router_config.yaml")

    matcher := router.NewMatcher()
    bootstrap.Conf.Router = matcher


    //now add the services.
    matcher.AddService("http://localhost:8010")



    log.Println("Starting")
    //starts listening on all configured interfaces
    bootstrap.Start()
}
