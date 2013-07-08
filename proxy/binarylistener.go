package proxy

import (
    "bufio"
    "fmt"
    "log"
    "net"
    "sync"
    "io"
        // 
)




func (this *BinaryWriter) Type() string {
    return BIN.Type()
}

func BinaryListen(port int, server *Server) error {
    ln, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
    defer ln.Close()
    if err != nil {
        // handle error
        log.Println(err)
        return err
    }
    log.Println("Binary Listener on port: ", port)
    for {
        conn, err := ln.Accept()
        if err != nil {
            log.Print(err)
            // handle error
            continue
        }
        bincon := &BinaryConnection{
            server: server, 
            conn: conn,
            writer: bufio.NewWriter(conn),
        }

        go bincon.listen()
    }
    return nil
}



type BinaryConnection struct {
    server *Server
    conn         net.Conn
    writer  *bufio.Writer
    writerLock   sync.Mutex
    service *Service
    requests map[string]chan *ClientResponse
}

func (this *BinaryWriter) Write(response *Response) (int, error) {
    defer this.writerLock.Unlock()
    this.writerLock.Lock()
    // log.Printf("Write response %s", response)
    bytes, err := BIN.WriteResponse(response, this.writer)
    this.writer.Flush()
    return bytes, err
}


// a request. 
func (this *BinaryConnection) request(request *cheshire.Request) {
    //TODO: We should be able to take the original byte array and
    // just pull out the partition key and send.  would be faster, but
    // more annoying.

    partition, err := this.service.Partition(request.Shard.Key)
    if err != nil {
        log.Print(err)
        //TODO: send error
        return
    }
    request.Shard.Partition = partition
    //ready to send upstream


    entries, err := service.Entries(partition)
    if err != nil {
        //TODO, do something here.
        log.Printf("ERRROR %s", err)
        return
    }

    resultChan := make(chan *ClientResponse, len(entries))
    //TODO: lock that shit
    requests[request.TxnId()] = resultChan

    //# of expected responses
    expected := len(entries)
    for i, e := range(entries) {
        //write the requests
        err := e.Client().WriteRequest(request)
        if err != nil {
            //retry
            e.Reconnect()
            err := e.Client().WriteRequest(request)
            if err != nil {
                log.Println(err)
                expected--
            }    
        }
    }

    results := make([]*cheshire.Response, 0)
    for results < expected {
        select {
        case res := <- resultChan :
            log.Println("Got response!")
            results = append(results, res.response)
            //check result code for bad router table ect.
            // check for locks, or other problems
            if res.response.StatusCode() == shards.E_ROUTER_TABLE_OLD {
                // ouch bad routertable..

                //update router table, and try the request again.
                rt, err := shards.RequestRouterTable(res.client)
                if err != nil {
                    this.service.SetRouterTable(rt)
                }
            }

            // partition is locked!
            if resp.StatusCode() == shards.E_PARTITION_LOCKED {
                //TODO: ?
            }

        case <- time.After(20 * time.Second) :
            break
        }
    }
    //now serialize the responses..
    this.Write(cheshire.NewResponse(request))
}



func (this *BinaryConnection) listen() {
    defer this.conn.Close()

    reader := bufio.NewReader(this.conn)
    decoder := BIN.NewDecoder(reader)
    err := decoder.DecodeHello()
    if err != nil {
        log.Print(err)
        return
    }

    //create the service
    rt, err := this.server.RouterTable(decoder.Hello.MustString("service", ""))
    if err != nil {
        log.Print(err)
        return
    }
    this.service, err = NewService(rt)
    if err != nil {
        log.Print(err)
        return
    }
    defer this.service.Close()
    
    for {
        req, err := decoder.DecodeRequest()
        if err == io.EOF {
            log.Print(err)
            break
        } else if err != nil {
            log.Print(err)
            break
        }
        go this.request(req)
    }

    log.Print("DISCONNECT!")
}
