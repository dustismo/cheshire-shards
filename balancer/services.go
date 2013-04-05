package balancer

import (
    // "log"
    "github.com/trendrr/cheshire-golang/dynmap"
    "github.com/trendrr/cheshire-golang/partition"

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
