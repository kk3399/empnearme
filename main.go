package main

import (
	"github.com/kdamarla/empnearme/buntdb"
	"github.com/kdamarla/empnearme/http"
	logWriter "github.com/kdamarla/empnearme/log"
)

func main() {
	logWriter.Init()
	logger := logWriter.Writer{}
	repo := buntdb.Init(logger)
	defer repo.Close()

	repo.Load()

	lcaHandler := http.LcaHandler{LcaRepo: repo, Log: logger}
	httpHandler := http.Handler{LcaHandler: lcaHandler}
	logger.Write(http.Serve(httpHandler))
}
