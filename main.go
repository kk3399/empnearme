package main

import (
	_ "net/http/pprof"

	"github.com/kk3399/empnearme/http"
	logWriter "github.com/kk3399/empnearme/log"
	"github.com/kk3399/empnearme/store"
)

const dbFileName = "data.gob"

func main() {

	logWriter.Init()
	logger := logWriter.Writer{}
	repo := store.Init(logger)

	logger.Info("db is open")

	lcaHandler := http.LcaHandler{LcaRepo: repo, Log: logger}
	empListHandler := http.EmpListHandler{LcaRepo: repo}
	httpHandler := http.Handler{LcaHandler: lcaHandler, EmpListHandler: empListHandler}
	httpHandler.StartProfiling()
	logger.Write(http.Serve(httpHandler))
}
