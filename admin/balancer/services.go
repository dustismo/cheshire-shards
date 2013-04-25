package balancer

import (
    // "log"
    "github.com/trendrr/cheshire-golang/dynmap"
    "github.com/trendrr/cheshire-golang/partition"
    clog "github.com/trendrr/cheshire-golang/log"
    "sort"
    "fmt"
    "io/ioutil"
    "log"
    "sync"
)

type Services struct {
    DataDir string
    services map[string]*partition.RouterTable
    Logger *clog.Logger
    lock sync.Mutex
}

var Servs = &Services{
    services : make(map[string]*partition.RouterTable),
    Logger : cheshire.NewLogger(),
}

func (this *Services) Load() error {
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
        table, err := partition.ToRouterTable(rt)
        if err != nil {
            log.Println(err)
            continue
        }
        this.services[k] = table
    }
    return nil
}

func (this *Services) Save() error {
    mp := dynmap.NewDynMap()
    
    for k,v := range(this.services) {
        mp.Put(k, v.ToDynMap())
    }

    bytes,err := mp.MarshalJSON()
    if err != nil {
        return err
    }
    err = ioutil.WriteFile(fmt.Sprintf("%s/%s",this.DataDir, "services.json"), bytes, 0644)
    return err
}

//Will create and save a new router table. 
func (this *Services) NewRouterTable(service string, totalPartitions int, repFactor int) error {
    
    _, ok := this.RouterTable(service)
    if ok {
        //already exists
        return fmt.Errorf("Router Table %s already exists!", service)
    }

    rt := partition.NewRouterTable(service)
    rt.TotalPartitions = totalPartitions
    rt.ReplicationFactor= repFactor
    this.SetRouterTable(rt)
    return nil
}

func (this *Services) SetRouterTable(table *partition.RouterTable) {
    this.services[table.Service] = table
    this.Save()
}

func (this *Services) RouterTable(service string) (*partition.RouterTable, bool) {
    t, ok := this.services[service]
    return t,ok
}

// Returns a sorted list of the available router tables.
func (this *Services) RouterTables() ([]*partition.RouterTable) {
    tables := make([]*partition.RouterTable, 0)
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
