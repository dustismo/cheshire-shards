package proxy

// the binary protocol proxy implementation

import(
    "encoding/binary"
    "io"
    "fmt"
    "log"
    "github.com/trendrr/goshire/cheshire"
    "github.com/trendrr/goshire/dynmap"

    "github.com/trendrr/goshire-shards/shards"
    "net"
    "time"
)


type BinProxy struct {

}

func (this *BinProxy) NewDecoder(reader io.Reader) decoder {
    return &BinProxyDecoder{
        reader : reader,
    }
}

type BinProxyDecoder struct {
    reader io.Reader
}

func (this *BinProxyDecoder) DecodeResponse() (*resp, error) {
    txnId, err := cheshire.ReadString(this.reader)
    if err != nil {
        return nil, err
    }

    // log.Printf("txn %s", txnId)
    txnStatus := int8(0)
    err = binary.Read(this.reader, binary.BigEndian, &txnStatus)
    if err != nil {
        return nil, err
    }
    if int(txnStatus) >= len(cheshire.TXN_STATUS) {
        return nil, fmt.Errorf("TxnStatus too large %d", txnStatus)
    }

    statusCode := int16(0)
    err = binary.Read(this.reader, binary.BigEndian, &statusCode)
    if err != nil {
        return nil, err
    }

    res := &cheshire.Response{}
    res.SetTxnId(txnId)
    // log.Println("TXN ID ",  txnId)
    // log.Println("status ", txnStatus)
    res.SetTxnStatus(cheshire.TXN_STATUS[int(txnStatus)])
    res.SetStatusCode(int(statusCode))
    return NewResp(res, this.reader), nil
}


// Writes the response from a connection
func (this *BinProxy) WriteResponse(response *resp, writer io.Writer) error {
//     [length][txn_id (string)]
// [txn_status(int8)]
// [status (int16)]
    //we have already read up to here! see decoder

// [length][status_message (string)]
// [param_encoding (int8)]
// [length (int32)][params (array)]
// [content_encoding (int8)]
// [content_length (int32)][content (array)]
    cheshire.WriteString(writer, response.response.TxnId())
    //txn status
    txnStatus, ok := cheshire.BINCONST.TxnStatus[response.response.TxnStatus()]
    if !ok {
        return fmt.Errorf("Bad TXN Status %s", response.response.TxnStatus())
    }
    err := binary.Write(writer, binary.BigEndian, txnStatus)
    if err != nil {
        return err
    }
    err = binary.Write(writer, binary.BigEndian, int16(response.response.StatusCode()))
    if err != nil {
        return err
    }

    //status message
    err = cheshire.CopyByteArray(writer, response.reader)
    if err != nil {
        return err
    }

    //param encoding
    _, err = io.CopyN(writer, response.reader, 1)
    if err != nil {
        return err
    }

    //params 
    err = cheshire.CopyByteArray32(writer, response.reader)
    if err != nil {
        return err
    }
    
    //content encoding
    _, err = io.CopyN(writer, response.reader, 1)
    if err != nil {
        return err
    }

    //content
    err = cheshire.CopyByteArray32(writer, response.reader)
    if err != nil {
        return err
    }
    return nil
}

    
func (this *BinProxy) StartProxy(connection io.ReadWriteCloser, server *Server) {
    decoder := cheshire.BIN.NewDecoder(connection)
    mp, err := decoder.DecodeHello()
    if err != nil {
        log.Printf("Error in start proxy %s", err)
        return
    }
    service, err := server.Service(mp.MustString("service", ""))
    if err != nil {
        log.Printf("Error in start proxy %s", err)
        return
    }
    px, err := NewProxy(service, this)
    px.clientConn = connection

    if err != nil {
        log.Printf("Error in start proxy %s", err)
        return
    }
    this.Listen(px)
}


// This is called on connection.  this should handle
// the incoming requests from the client, and pass them to the
// appropriate Conn 
func (this *BinProxy) Listen(proxy *Proxy) {
    defer proxy.Close()
    decoder := cheshire.BIN.NewDecoder(proxy.clientConn).(*cheshire.BinDecoder)
    //now listen for incoming.
    for {
        shardReq, err := decoder.DecodeShardRequest()
        if err == io.EOF {
            log.Print(err)
            break
        } else if err != nil {
            log.Print(err)
            break
        }

        //get the partition
        partition, err := proxy.Partition(*shardReq)
        if err != nil {
            log.Print(err)
            break
        }
        shardReq.Partition = partition
        // log.Printf("Partition %d", partition)
        //find the connection
        con, err := proxy.Conn(partition)
        if err != nil {
            log.Print(err)
            break
        }

        cheshire.BIN.WriteShardRequest(shardReq, con.Connection)
        //now write and copy bytes.
        
        txnId, err := cheshire.ReadString(proxy.clientConn)
        cheshire.WriteString(con.Connection, txnId)

        //txn id 
        // method
        _, err = io.CopyN(con.Connection, proxy.clientConn, 2)
        if err != nil {
            log.Print(err)
            break 
        }

        //uri
        err = cheshire.CopyByteArray(con.Connection, proxy.clientConn)
        if err != nil {
            log.Print(err)
            break 
        }

        //param encoding
        _, err = io.CopyN(con.Connection, proxy.clientConn, 1)
        if err != nil {
            log.Print(err)
            break 
        }

        //params array
        err = cheshire.CopyByteArray(con.Connection, proxy.clientConn)
        if err != nil {
            log.Print(err)
            break 
        }


        //content encoding
        _, err = io.CopyN(con.Connection, proxy.clientConn, 1)
        if err != nil {
            log.Print(err)
            break 
        }

        //content array
        err = cheshire.CopyByteArray32(con.Connection, proxy.clientConn)
        if err != nil {
            log.Print(err)
            break 
        }

        //SUCCESS!
    }
    return 
}

    //Create a new connection based on the router entry.
func (this *BinProxy) NewConn(proxy *Proxy, entry *shards.RouterEntry) (*Conn, error) {
    //connect.
    port := entry.BinPort
    conn, err := net.DialTimeout("tcp", fmt.Sprintf("%s:%d", entry.Address, port), 5*time.Second)
    if err != nil {
        return nil, err
    }
    cheshire.BIN.WriteHello(conn, dynmap.New())
    return &Conn {
        Connection : conn, 
        Entry : entry,
        Port   : port,
        proxy  : proxy,
    }, nil
}