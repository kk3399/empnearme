package main

import (
	_ "net/http/pprof"

	"github.com/kdamarla/empnearme/buntdb"
	"github.com/kdamarla/empnearme/http"
	logWriter "github.com/kdamarla/empnearme/log"
)

const dbFileName = "data.db"
const cacheDbFileName = "cache.db"

func main() {

	logWriter.Init()
	logger := logWriter.Writer{}
	repo := buntdb.Init(logger, dbFileName, cacheDbFileName)
	defer repo.Close()

	logger.Info("db is open")

	lcaHandler := http.LcaHandler{LcaRepo: repo, Log: logger}
	httpHandler := http.Handler{LcaHandler: lcaHandler}
	httpHandler.StartProfiling()
	logger.Write(http.Serve(httpHandler))
}
