package balancer

import (
    // "log"
    "github.com/trendrr/cheshire-golang/dynmap"
    "github.com/trendrr/cheshire-golang/partition"
    "sort"
    "fmt"
    "io/ioutil"
)


type Services struct {
    DataDir string
    services map[string]*partition.RouterTable
}

var Servs = &Services{
    services : make(map[string]*partition.RouterTable),
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
        table, _ := partition.ToRouterTable(rt)
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
