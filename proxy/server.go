package proxy

import (
    "fmt"
    // "github.com/trendrr/goshire-shards/shards"
    "github.com/trendrr/goshire/cheshire"
    "log"
    "strings"
    "time"
    "sync"
)

// What the proxy needs to do:
// Listen on ports
// Shard on partition key
// forward request to proper server.
// Allow for overriding of processing for specific routes
// Manage the router table


type Server struct {
    //the bootstrap
    Bootstrap *cheshire.Bootstrap
    services  map[string]*RouterTable
    Config *cheshire.ServerConfig
    lock sync.RWMutex
}


func NewServerFile(configPath string) *Server {
    conf := cheshire.NewServerConfigFile(configPath)
    return NewServer(conf)
}

func NewServer(config *cheshire.ServerConfig) *Server {
    //create the config
    s := &Server{
        Bootstrap: cheshire.NewBootstrap(config),
        services:  make(map[string]*RouterTable),
        Config: config,
    }
    return s
}

// Sets the router tables via these seed urls 
func (this *Server) Seeds(urls ...string) {
    for _, url := range urls {
        c := client.NewHttp(url)
        rt, err := shards.RequestRouterTable(c)
        if err == nil {
            this.SetRouterTable(rt)
        }
    }
}

func (this *Server) SetRouterTable(rt *RouterTable) {
    this.lock.Lock()
    defer this.lock.Unlock()
    old, ok := this.services[rt.Service]
    if ok {
        if rt.Revision <= old.Revision {
            log.Println("Attempted to set an equal or older router table for service %s -- skipping", rt.Service)
            return
        }
    }
    this.services[rt.Service] = rt
}

// REturns the router table for the specified service
func (this *Server) RouterTable(service string) (*RouterTable, error) {
    this.lock.RLock()
    defer this.lock.RUnlock()
    rt, ok := this.services[service]
    if ok {
        return rt, nil
    }
    if len(service) == 0 && len(this.services) == 1 {
        for _, v := range(this.services) {
            return v, nil
        }
    }
    return nil, fmt.Errorf("No service with name: %s found", service)
}

//Start listening
func (this *Server) Start() {

}

