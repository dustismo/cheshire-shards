package balancer

import (
    "github.com/trendrr/goshire/dynmap"
    // "github.com/trendrr/goshire/cheshire"
    "github.com/trendrr/goshire-shards/shards"
    clog "github.com/trendrr/goshire/log"
    "sort"
    "fmt"
    "io/ioutil"
    "log"
    "sync"
)

type Services struct {
    DataDir string
    services map[string]*shards.RouterTable
    Logger *clog.Logger
    lock sync.Mutex
}

var Servs = &Services{
    services : make(map[string]*shards.RouterTable),
    Logger : clog.NewLogger(),
}

func (this *Services) Load() error {
    this.lock.Lock()
    defer this.lock.Unlock()
    //attempt to load from datadir
    filename := fmt.Sprintf("%s/%s",this.DataDir, "services.json")
    bytes, err := ioutil.ReadFile(filename)
    if err != nil {
        return err
    }
    mp := dynmap.NewDynMap()
    err = mp.UnmarshalJSON(bytes)
    if err != nil {
        return err
    }


    for k,_ := range(mp.Map) {
        rt,ok := mp.GetDynMap(k)
        if !ok {
            continue
        }
        table, err := shards.ToRouterTable(rt)
        if err != nil {
            log.Println(err)
            continue
        }
        this.services[k] = table
    }
    return nil
}

func (this *Services) Save() error {
    this.lock.Lock()
    defer this.lock.Unlock()
    
    mp := dynmap.NewDynMap()
    
    for k,v := range(this.services) {
        mp.Put(k, v.ToDynMap())
    }

    bytes,err := mp.MarshalJSON()
    if err != nil {
        this.Logger.Printf("Error marshalling services table -- %s", err)
        return err
    }
    err = ioutil.WriteFile(fmt.Sprintf("%s/%s",this.DataDir, "services.json"), bytes, 0644)
    return err
}

//Will create and save a new router table. 
func (this *Services) NewRouterTable(service string, totalshards int, repFactor int, partitionKeys []string) error {
    
    _, ok := this.RouterTable(service)
    if ok {
        //already exists
        return fmt.Errorf("Router Table %s already exists!", service)
    }

    rt := shards.NewRouterTable(service)
    rt.TotalPartitions = totalshards
    rt.ReplicationFactor= repFactor
    rt.PartitionKeys = partitionKeys
    this.SetRouterTable(rt)
    return nil
}

func (this *Services) Remove(service string) {
    //defers are executed in the reverse order
    defer this.Save()
    this.lock.Lock()
    defer this.lock.Unlock()
    delete(this.services, service)
}

func (this *Services) SetRouterTable(table *shards.RouterTable) {
    this.lock.Lock()
    this.services[table.Service] = table
    this.lock.Unlock()
    err := this.Save()
    if err != nil {
        log.Printf("Error saving %s", err)
    }
}

func (this *Services) RouterTable(service string) (*shards.RouterTable, bool) {
    this.lock.Lock()
    defer this.lock.Unlock()
    t, ok := this.services[service]
    return t,ok
}

// Returns a sorted list of the available router tables.
func (this *Services) RouterTables() ([]*shards.RouterTable) {
    this.lock.Lock()
    defer this.lock.Unlock()

    tables := make([]*shards.RouterTable, 0)
    var keys []string
    for k,_ := range(this.services) {
        keys = append(keys, k)
    }
    sort.Strings(keys)
    for _,k := range(keys) {
        v, ok := this.services[k]
        if !ok {
            continue
        }
        tables = append(tables, v)
    }
    return tables

}
