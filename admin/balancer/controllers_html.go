package balancer

import (
	"fmt"
	"github.com/trendrr/goshire/cheshire"
	"log"
)

func init() {
	cheshire.RegisterHtml("/", "GET", Index)
	cheshire.RegisterHtml("/service/delete", "POST", DeleteService)
	cheshire.RegisterHtml("/service/new", "POST", NewService)
	cheshire.RegisterHtml("/service", "GET", Service)
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

func DeleteService(txn *cheshire.Txn) {
	//TODO: remove the router table from the servs
	name, ok := txn.Params().GetString("service-name")
	if !ok {
		cheshire.Flash(txn, "error", "Service Name is manditory")
	}

	Servs.Remove(name)
	cheshire.Redirect(txn, "/index")
}

func NewService(txn *cheshire.Txn) {
	log.Println(txn.Params())

	name, ok := txn.Params().GetString("service-name")
	if !ok {
		cheshire.Flash(txn, "error", "Service Name is manditory")
	}

	replication := txn.Params().MustInt("replication", 2)
	partitions := txn.Params().MustInt("total-partitions", 512)
	partitionKeys, ok := txn.Params().GetStringSliceSplit("partition-keys", ",")
	if !ok {
		cheshire.Flash(txn, "error", "Partition keys is manditory")
	}
	log.Println(partitionKeys)
	err := Servs.NewRouterTable(name, partitions, replication, partitionKeys)
	if err != nil {
		cheshire.Flash(txn, "error", fmt.Sprintf("%s", err))
		cheshire.Redirect(txn, "/index")
	} else {
		cheshire.Flash(txn, "success", "successfully created router table")
		cheshire.Redirect(txn, fmt.Sprintf("/service?name=%s", name))
	}

}

func Service(txn *cheshire.Txn) {
	//create a context map to be passed to the template
	context := make(map[string]interface{})

	service, ok := Servs.RouterTable(txn.Params().MustString("name", ""))
	if !ok {
		cheshire.Flash(txn, "error", fmt.Sprintf("Cant find service"))
		cheshire.Redirect(txn, "/index")
		return
	}
	context["service"] = service.Service
	cheshire.RenderInLayout(txn, "/service.html", "/template.html", context)
}
