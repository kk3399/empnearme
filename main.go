package main

import (
	_ "net/http/pprof"
	"runtime"

	"github.com/kdamarla/empnearme/http"
	logWriter "github.com/kdamarla/empnearme/log"
	"github.com/kdamarla/empnearme/store"
)

const dbFileName = "data.gob"

func main() {

	runtime.GOMAXPROCS(1)

	logWriter.Init()
	logger := logWriter.Writer{}
	repo := store.Init(logger)

	logger.Info("db is open")

	lcaHandler := http.LcaHandler{LcaRepo: repo, Log: logger}
	httpHandler := http.Handler{LcaHandler: lcaHandler}
	httpHandler.StartProfiling()
	logger.Write(http.Serve(httpHandler))
}
