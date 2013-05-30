package router


import (
    "fmt"
    // "github.com/trendrr/goshire-shards/shards"
    "github.com/trendrr/goshire/cheshire"
    "log"
    "strings"
    "time"
)




// What the router needs to do:
// Listen on ports
// Shard on partition key
// forward request to proper server.
// Allow for overriding of processing for specific routes
// Manage the router table

// Handles matching the request to the 
// appropriate service router
// This basically overrides a bunch of standard stuff in the 
// goshire stack, the uri matcher, and controllers.
type Server struct {
    //the bootstrap
    Bootstrap *cheshire.Bootstrap
    conConfig   *cheshire.ControllerConfig
    services map[string]*Client
}

func NewServerFile(configPath string) *Server {
    conf := cheshire.NewServerConfigFile(configPath)
    return NewServer(conf)
}

func NewServer(config *cheshire.ServerConfig) *Server {
    //create the config
    s := &Server{
        Bootstrap: cheshire.NewBootstrap(config),
        conConfig:   cheshire.NewControllerConfig("/"),
        services: make(map[string]*Client),
    }
    s.Bootstrap.Conf.Router = s
    return s
}

//Start listening
func (this *Server) Start() {
    this.Bootstrap.Start()
}



// Adds a new service to this router.
// All endpoints matching /{serviceName}/ will be forwarded to that service.
// at least 1 of the seedUrls must be in service 
func (this *Server) AddService(seedUrls ...string) error {

    client, err := NewClientFromSeed(seedUrls...)
    if err != nil {
        return err
    }

    this.services[client.connections.RouterTable().Service] = client
    return nil
}

// We return our special controller here.
func (this *Server) Match(route string, method string) cheshire.Controller {
    return this
}

// Necessary to implement the RouteMatcher interface
func (this *Server) Register([]string, cheshire.Controller) {
    //do nothing
    // log.Println("Register called on Router Matcher. Invalid!")
}

// Necessary for the Controller interface
func (this *Server) Config() *cheshire.ControllerConfig {
    return this.conConfig
}

// This is our special controller.
func (this *Server) HandleRequest(txn *cheshire.Txn) {
    tmp := strings.SplitN(txn.Request.Uri(), "/", 3)
    if len(tmp) != 3 {
        //ack!
        cheshire.SendError(txn, 406, "Uri should be in the form /{service}/{uri}")
        return
    }
    service := tmp[1]
    //set the proper uri
    txn.Request.SetUri(fmt.Sprintf("/%s", tmp[2]))

    client, ok := this.services[service]
    if !ok {
        cheshire.SendError(txn, 404, fmt.Sprintf("No service with name %s found", service))
        return
    }

    response, err := client.ApiCallSync(txn.Request, 30 * time.Second)
    if err != nil {
        log.Println(err)
        cheshire.SendError(txn, 501, fmt.Sprintf("%s", err))
    } else {
        txn.Write(response)
    }

}
