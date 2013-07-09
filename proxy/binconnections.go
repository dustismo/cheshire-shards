package proxy

import (
	"bufio"
	"fmt"
	"github.com/trendrr/goshire-shards/shards"
	"github.com/trendrr/goshire/cheshire"
	"io"
	"log"
	"net"
	"time"
)

//
// Handles a set of connections to all the shards in the cluster
//
type ShardConns struct {
	//Shard Connections indexed by Partition
	Partitions []*ShardConn
	//set of the available unique connections
	Conns []*ShardConn

	//Sending a flag on this channel will kill this connection
	KillChan chan bool

	responseChan chan *cheshire.Response
	requestChan  chan *cheshire.Request

	protocol cheshire.Protocol

	service *Service

	conn net.Conn
}

// creates and handles a binary connection
// does not return
func HandleShardConns(conn net.Conn, protocol cheshire.Protocol, server *Server) error {
	defer conn.Close()

	reader := bufio.NewReader(conn)
	decoder := protocol.NewDecoder(reader)
	err := decoder.DecodeHello()
	if err != nil {
		return err
	}

	//TODO Find service..
	service, err := server.Service("")
	if err != nil {
		log.Println(err)
		return err
	}

	sc, err := NewShardConns(service, protocol)
	if err != nil {
		log.Println(err)
		return err
	}

	sc.conn = conn
	//now listen for incoming.
	for {
		req, err := decoder.DecodeRequest()
		if err == io.EOF {
			log.Print(err)
			break
		} else if err != nil {
			log.Print(err)
			break
		}
		sc.requestChan <- req
	}
	log.Println("RETURNING!")
	return nil
}

//Create the shard connection and connect to all the shards in the cluster.
func NewShardConns(service *Service, protocol cheshire.Protocol) (*ShardConns, error) {
	rt := service.RouterTable()

	log.Println("NEW SHARD CONNS")
	sc := &ShardConns{
		Partitions:   make([]*ShardConn, rt.TotalPartitions),
		Conns:        make([]*ShardConn, 0),
		KillChan:     make(chan bool, 5),
		responseChan: make(chan *cheshire.Response),
		requestChan:  make(chan *cheshire.Request),
		service:      service,
		protocol:     protocol,
	}
	for _, e := range rt.Entries {
		con, err := NewShardConn(e, sc)
		if err != nil {
			log.Println(err)
			//TODO: fail or keep going?
			continue
		}
		sc.Conns = append(sc.Conns, con)

		for _, p := range e.Partitions {
			sc.Partitions[p] = con
		}
	}

	go sc.proxy()

	return sc, nil
}

// Does the actual proxying
//
func (this *ShardConns) proxy() {
	defer this.Close()
	for {

		select {
		case <-this.KillChan:
			break
		case resp := <-this.responseChan:
			// log.Println("Got response!")
			this.protocol.WriteResponse(resp, this.conn)
			//check result code for bad router table ect.
			// check for locks, or other problems
			if resp.StatusCode() == shards.E_ROUTER_TABLE_OLD {
				// ouch bad routertable..
				log.Println("TODO! BAD ROUTER TABLE")
				//TODO!
			}
			// partition is locked!
			if resp.StatusCode() == shards.E_PARTITION_LOCKED {
				//TODO: ?
			}

		case req := <-this.requestChan:
			//TODO: We should be able to take the original byte array and
			// just pull out the partition key and send.  would be faster, but
			// more annoying.

			if req.Shard.Partition < 0 {
				partition, err := this.service.Partition(req.Shard.Key)
				if err != nil {
					log.Print(err)
					//TODO: send error
					break
				}
				req.Shard.Partition = partition
				//ready to send upstream
			}
			con := this.Partitions[req.Shard.Partition]

			if con == nil {
				log.Print("Error no connection for partition %d", req.Shard.Partition)
				//TODO: send error
				break
			}

			_, err := this.protocol.WriteRequest(req, con.Writer)
			if err != nil {
				log.Print(err)
				break
			}
		}
	}
}

//closes all the connections
func (this *ShardConns) Close() {
	this.KillChan <- true
}

func (this *ShardConns) close() {
	this.conn.Close()
	for _, c := range this.Conns {
		c.Close()
	}
}

// A single connection
type ShardConn struct {
	Reader   io.Reader
	Writer   io.Writer
	conn     net.Conn
	Entry    *shards.RouterEntry
	parent   *ShardConns
	response *cheshire.Response
}

func NewShardConn(entry *shards.RouterEntry, parent *ShardConns) (*ShardConn, error) {
	port := 0
	if parent.protocol.Type() == "json" {
		port = entry.JsonPort
	} else if parent.protocol.Type() == "bin" {
		port = entry.BinPort
		port = 8011 //TODO remove me!
	}
	if port == 0 {
		return nil, fmt.Errorf("ERROR No port found for entry %s", entry)
	}

	conn, err := net.DialTimeout("tcp", fmt.Sprintf("%s:%d", entry.Address, port), 5*time.Second)
	if err != nil {
		return nil, err
	}
	parent.protocol.WriteHello(conn)
	sc := &ShardConn{
		Reader: bufio.NewReader(conn),
		Writer: conn,
		conn:   conn,
		Entry:  entry,
		parent: parent,
	}

	go sc.listen()
	return sc, nil
}

func (this *ShardConn) listen() {
	defer this.parent.Close()
	log.Println("LISTEN!")
	decoder := this.parent.protocol.NewDecoder(this.Reader)

	for {
		res, err := decoder.DecodeResponse()
		if err == io.EOF {
			log.Print(err)
			break
		} else if err != nil {
			log.Print(err)
			break
		}
		this.parent.responseChan <- res
	}
}

//closes all the connections
func (this *ShardConn) Close() {
	this.conn.Close()
}
