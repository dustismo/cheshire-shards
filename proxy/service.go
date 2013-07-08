package proxy

// Handles the connections for a single service. 

type Service struct {
    connections *shards.Connections
    hasher      Hasher
}
// creates a new client from seed urls.
func NewService(rt *RouterTable) (*Service, error) {
    service := &Service{}

    connections := &shards.Connections{}
    connections.SetClientCreator(service)
    connections.SetRouterTable(rt)

    service.connections = connections
    service.hasher = &DefaultHasher{}
    return service, nil
}

func (this *Service) Partition(key string) (int, error) {
    partition, err := this.hasher.Hash(partitionKey, this.connections.RouterTable().TotalPartitions)
    return partition, err
}

//gets the entries for a partition.
func (this *Service) Entries(partition int) ([]*EntryClient, error) {
    v, err := this.connections.Entries(partition)
    return v,err
}

func (this *Service) Close() {
    this.connections.Close()
}

// to satisify the clientcreator interface
func (this *Service) Create(entry *RouterEntry) (client.Client, error) {
    c := &Client{
        HttpClient: client.NewHttp(fmt.Sprintf("http://%s:%d", entry.Address, entry.HttpPort)),
    }
    addr := fmt.Sprintf("%s:%d", entry.Address, entry.BinPort)
    conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
    if err != nil {
        return nil, err
    }
    c.Conn = conn
    c.entry = entry
    return c, err
}

//we have a dummy client that uses http for the 
// regular api calls but allows us access to the an underlying binary socket.
type Client struct {
    client.HttpClient
    conn net.Conn
    entry *shard.RouterEntry 
    responseChan chan *ClientResponse
    lock sync.Mutex
}

type ClientResponse struct {
    client *Client
    response *cheshire.Response
}

func (this *Client) Start() {
    
    //say hello
    BIN.WriteHello(this.conn)

    //3. listen for responses
    go this.listen()


}

func (this *Client) WriteBytes(bytes []byte) error {
    return nil
}

//writes a request
func (this *Client) WriteRequest(req *cheshire.Request) error {
    lock.Lock()
    defer lock.Unlock()
    err := BIN.WriteRequest(this.conn, req)
    return err 
}

func (this *Client) listen() {
    defer this.Close()
    reader := bufio.NewReader(this.Conn)
    decoder := BIN.NewDecoder(reader)
    err := decoder.DecodeHello()
    if err != nil {
        return
    }
    for {
        res, err := decoder.DecodeResponse()
        if err == io.EOF {
            log.Print(err)
            break
        } else if err != nil {
            log.Print(err)
            break
        }
        this.responseChan <- &ClientResponse{client : this, response : res}
    }


}

func (this *Client) Close() {
    log.Println("Closing router client")
    //close the bin socket..
    this.conn.Close()
}