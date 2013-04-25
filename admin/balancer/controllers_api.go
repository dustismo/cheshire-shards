package balancer

import (
    "github.com/trendrr/cheshire-golang/cheshire"
    clog "github.com/trendrr/cheshire-golang/log"
    "log"
    "fmt"
)

func init() {
    cheshire.RegisterApi("/api/log", "GET", ConsoleLog)
}

// Gets any logging messages from the Servs.Events
// sends to client
func ConsoleLog(txn *cheshire.Txn) {
    msgChan := make(chan clog.LoggerEvent, 10)
    Servs.Logger.Listen(msgChan)
    defer Servs.Logger.Unlisten(msgChan)
    log.Println("Log Listener Registered")
    for {
        msg := <- msgChan
        res := cheshire.NewResponse(txn)
        res.SetTxnStatus("continue")
        res.PutWithDot("event.type", msg.Type)
        res.PutWithDot("event.message", msg.Message)
        _, err := txn.Write(res)
        if err != nil {
            //try and write an error response
            cheshire.SendError(txn, 510, fmt.Sprintf("%s",err))
            break   
        }
    }
    log.Println("Log Listener unregistered")
}
