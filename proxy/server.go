package proxy

import (
	"fmt"
	"github.com/trendrr/goshire-shards/shards"
	"github.com/trendrr/goshire/cheshire"
	"github.com/trendrr/goshire/client"
	"log"
	"net"
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
	services  map[string]*Service
	Config    *cheshire.ServerConfig
}

func NewServerFile(configPath string) *Server {
	conf := cheshire.NewServerConfigFile(configPath)
	return NewServer(conf)
}

func NewServer(config *cheshire.ServerConfig) *Server {
	//create the config
	s := &Server{
		Bootstrap: cheshire.NewBootstrap(config),
		services:  make(map[string]*Service),
		Config:    config,
	}
	return s
}

// Sets the router tables via these seed urls
func (this *Server) Seeds(urls ...string) {
	for _, url := range urls {
		c := client.NewHttp(url)
		rt, err := shards.RequestRouterTable(c)
		if err == nil {
			this.RegisterService(rt)
		}
	}
}

// Adding a new service.  Note this is NOT threadsafe, should be done during
// initialization
func (this *Server) RegisterService(rt *shards.RouterTable) error {
	if _, ok := this.services[rt.Service]; ok {
		return fmt.Errorf("Service already registered %s", rt.Service)
	}

	service, err := NewService(rt)
	if err != nil {
		return err
	}
	this.services[rt.Service] = service
	return nil
}

// REturns the router table for the specified service
// If this proxy has only one service registered then
// it will use that one
func (this *Server) Service(service string) (*Service, error) {
	rt, ok := this.services[service]
	if ok {
		return rt, nil
	}
	if len(service) == 0 && len(this.services) == 1 {
		for _, v := range this.services {
			return v, nil
		}
	}
	return nil, fmt.Errorf("No service with name: %s found", service)
}

//Start listening
func (this *Server) Start() {

	this.Bootstrap.InitProcs()
	//TODO: any other bootstrapping we need?

	log.Println("********** Starting Cheshire Shard Proxy **************")
	// //now start listening.
	// if this.Conf.Exists("http.port") {
	//     port, ok := this.Conf.GetInt("http.port")
	//     if !ok {
	//         log.Println("ERROR: Couldn't start http listener ", port)
	//     } else {
	//         go HttpListen(port, this.Conf)
	//     }
	// }

	if this.Config.Exists("json.port") {
		port, ok := this.Config.GetInt("json.port")
		if !ok {
			log.Println("ERROR: Couldn't start binary listener")
		} else {
			go protocollisten(cheshire.JSON, port, this)
		}
	}

	if this.Config.Exists("bin.port") {
		port, ok := this.Config.GetInt("bin.port")
		if !ok {
			log.Println("ERROR: Couldn't start binary listener")
		} else {
			go binarylisten(port, this)
		}
	}

	//this just makes the current thread sleep.  kinda stupid currently.
	//but we should update to get messages from the listeners, like when a listener quites
	channel := make(chan string)
	val := <-channel
	log.Println(val)
}

func binarylisten(port int, server *Server) {
	ln, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	defer ln.Close()
	if err != nil {
		// handle error
		log.Println(err)
		return
	}
	proxy := &BinProxy{

	}

	log.Printf("Binary Proxy Listener on port: %d", port)
	for {
		conn, err := ln.Accept()

		log.Printf("ACCEPT! %s", conn)
		if err != nil {
			log.Print(err)
			// handle error
			continue
		}


		//TODO: handle hello, and associate to correct service.
		go proxy.StartProxy(conn, server)
	}
}

func protocollisten(protocol cheshire.Protocol, port int, server *Server) error {
	ln, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	defer ln.Close()
	if err != nil {
		// handle error
		log.Println(err)
		return err
	}
	log.Printf("%s Listener on port: %d", protocol.Type(), port)
	for {
		conn, err := ln.Accept()

		log.Printf("ACCEPT! %s", conn)
		if err != nil {
			log.Print(err)
			// handle error
			continue
		}
		//TODO: handle hello, and associate to correct service.
		go HandleShardConns(conn, protocol, server)
	}
	return nil
}
