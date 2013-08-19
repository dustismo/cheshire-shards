package proxy

import(
    "github.com/trendrr/goshire/cheshire"
    "io"
    // "github.com/trendrr/goshire/dynmap"
    "fmt"
    "log"
    "github.com/trendrr/goshire-shards/shards"
    "time"
)
// New proxy implementation


type resp struct {
    //The response.  in most cases only a few of the fields will be 
    //filled in (txnid and txnstatus)
    //
    response *cheshire.Response

    // the bytes, this is either the 
    // full response or a direct connection to the endpoint
    reader io.Reader

    //once a response is created, the decoder can optionally wait on this channel for
    // the response to be pushed upstream
    // a true response indecates, success.  false indecates failure 
    continueChan chan bool
}

func NewResp(response *cheshire.Response, reader io.Reader) *resp {
    return &resp{
        response : response,
        reader : reader,
        continueChan : make(chan bool, 1),
    }
}

type Protocol interface {
    NewDecoder(io.Reader) decoder

    // We need to buffer the entire response to be sure not to interleave responses from 
    // different connections.
    WriteResponse(*resp, io.Writer) error
    
    // Creates a new proxy from the passed in connection
    // Should look up the proper service.
    // this is expected to be executed in a go reutine and only exit when the
    // connection is complete
    StartProxy(connection io.ReadWriteCloser, server *Server)

    //Create a new connection based on the router entry.
    NewConn(*Proxy, *shards.RouterEntry) (*Conn, error)
}

type decoder interface {
    DecodeResponse() (*resp, error)
}

type Proxy struct {
    service *Service
    protocol Protocol 

    //the outgoing connection (to client)
    clientConn io.ReadWriteCloser
    KillChan chan bool
    responseChan chan *resp

    //Shard Connections indexed by Partition
    Partitions []*Conn
    //set of the available unique connections
    Conns []*Conn
}

// Returns the correct connection for the specified 
// partition
func (this *Proxy) Conn(partition int) (*Conn, error) {
    if partition >= len(this.Partitions) || partition < 0 {
        return nil, fmt.Errorf("Partition out of range!")
    }
    return this.Partitions[partition], nil
}

func (this *Proxy) Partition(req cheshire.ShardRequest) (int, error) {
    if req.Partition >= 0 {
        if req.Partition >= len(this.Partitions) {
            return -1, fmt.Errorf("Partition out of range")
        }
        return req.Partition, nil
    }
    partition, err := this.service.Partition(req.Key)
    return partition, err
}

//Create the shard connection and connect to all the shards in the cluster.
func NewProxy(service *Service, protocol Protocol) (*Proxy, error) {
    rt := service.RouterTable()

    log.Println("NEW PROXY")
    px := &Proxy{
        Partitions:   make([]*Conn, rt.TotalPartitions),
        Conns:        make([]*Conn, 0),
        KillChan:     make(chan bool, 5),
        responseChan: make(chan *resp, 5),
        service:      service,
        protocol:     protocol,
    }

    for _, e := range rt.Entries {
        con, err := protocol.NewConn(px, e)
        if err != nil {
            log.Println(err)
            //TODO: fail or keep going?
            continue
        }
        go con.start()
        px.Conns = append(px.Conns, con)

        for _, p := range e.Partitions {
            px.Partitions[p] = con
        }
    }
    go px.start()
    return px, nil
}


// Does the actual proxying
//
func (this *Proxy) start() {
    defer this.Close()
    for {

        select {
        case <-this.KillChan:
            break
        case resp := <-this.responseChan:
            err := this.protocol.WriteResponse(resp, this.clientConn)
            if err != nil {
                log.Printf("Error in proxy %s", err)
                break
            }

            //check result code for bad router table ect.
            // check for locks, or other problems
            if resp.response.StatusCode() == shards.E_ROUTER_TABLE_OLD {
                // ouch bad routertable..
                log.Println("BAD ROUTER TABLE")
                this.routerTableRequest()
                log.Println("Updated router table.  Closing..")
                break
            }

            if resp.response.StatusCode() == shards.E_SEND_ROUTER_TABLE {
                //The server has an older router table then us.  
                // we need to send to them 
                // TODO
            }

            // partition is locked!
            if resp.response.StatusCode() == shards.E_PARTITION_LOCKED {
                //TODO: ?
            }

            //alert the resp that it can continue reading.
            resp.continueChan <- true
        }
    }
}


// looks through all the connections and tries to obtain an updated routertable.
func (this *Proxy) routerTableRequest() {
    for _, conn := range this.Conns {
        routerTable := this.service.RouterTable()

        rt, local, remote, err := shards.RouterTableSync(routerTable, conn.Entry)
        if err != nil {
            log.Printf("Error getting router table from %s -- %s", conn.Entry.Id(), err)
            continue
        }
        if local {
            this.service.connections.SetRouterTable(rt)
            return
        }
        if remote {
            log.Printf("Updated the router table on %s", conn.Entry.Id())
        }
    }
}

//closes all the connections
func (this *Proxy) Close() {
    select {
    case this.KillChan <- true:
    default:
    }
    
}

func (this *Proxy) close() {
    this.clientConn.Close()
    for _, c := range this.Conns {
        c.Close()
    }
}




// Single connection to the 
type Conn struct {
    Connection   io.ReadWriteCloser
    Entry    *shards.RouterEntry
    Port    int 
    proxy    *Proxy
}

func (this *Conn) start() {
    defer this.proxy.Close()
    decoder := this.proxy.protocol.NewDecoder(this.Connection)
    for {
        res, err := decoder.DecodeResponse()
        if err == io.EOF {
            log.Print(err)
            return
        } else if err != nil {
            log.Print(err)
            return
        }

        this.proxy.responseChan <- res
        select {
        case <-res.continueChan:
        case <- time.After(10 * time.Second):
            log.Println("Took more then 10 seconds to parse response, #dies")
            return
        }
    }
}

func (this *Conn) Close() {
    this.Connection.Close()
}


