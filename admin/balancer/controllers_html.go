package balancer

import (
    "github.com/trendrr/cheshire-golang/cheshire"
    "log"
    "fmt"
)

func init() {
    cheshire.RegisterHtml("/", "GET", Index)
    cheshire.RegisterHtml("/new", "POST", NewService)
    cheshire.RegisterHtml("/rt/edit", "GET", Service)
    cheshire.RegisterHtml("/log", "GET", Log)
}

//an example html page
func Index(txn *cheshire.Txn) {
    //create a context map to be passed to the template
    context := make(map[string]interface{})
    context["services"] = Servs.RouterTables()
    cheshire.RenderInLayout(txn, "/index.html", "/template.html", context)
}

func Log(txn *cheshire.Txn) {
    cheshire.RenderInLayout(txn, "/log.html", "/template.html", nil)
}

func NewService(txn *cheshire.Txn) {
    log.Println(txn.Params())

    name, ok := txn.Params().GetString("service-name")
    if !ok {
        cheshire.Flash(txn, "error", "Service Name is manditory")
    }

    err := Servs.NewRouterTable(name, 512, 2)
    if err != nil {
        cheshire.Flash(txn, "error", fmt.Sprintf("%s",err))
    } else {
        cheshire.Flash(txn, "success", "successfully created router table")    
    }
    cheshire.Redirect(txn, "/index")
}

func Service(txn *cheshire.Txn) {
    //create a context map to be passed to the template
    context := make(map[string]interface{})

    service, ok := Servs.RouterTable(txn.Params().MustString("service", ""))
    context["service"] = service 
    if !ok {
        cheshire.Flash(txn, "error", fmt.Sprintf("Cant find service"))
        cheshire.Redirect(txn, "/index")
        return
    }
    cheshire.RenderInLayout(txn, "/router_table.html", "/template.html", context)
}