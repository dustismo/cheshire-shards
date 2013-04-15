package balancer

import (
    "github.com/trendrr/cheshire-golang/cheshire"
)

func init() {
    cheshire.RegisterHtml("/", "GET", Index)
    cheshire.RegisterHtml("/new", "POST", NewService)
}

//an example html page
func Index(request *cheshire.Request, conn *cheshire.HtmlConnection) {
    //create a context map to be passed to the template
    context := make(map[string]interface{})
    context["services"] = Servs.RouterTables()
    conn.RenderInLayout("/index.html", "/template.html", context)
}

func NewService(request *cheshire.Request, conn *cheshire.HtmlConnection) {


}