package main

import (
	"fmt"
	_ "net/http/pprof"
	"os"
	"path/filepath"
	"runtime"

	"github.com/kk3399/empnearme/http"
	logWriter "github.com/kk3399/empnearme/log"
	"github.com/kk3399/empnearme/store"
)

const dbFileName = "data.gob"

func main() {

	runtime.GOMAXPROCS(1)

	ex, err := os.Executable()
	if err != nil {
		panic(err)
	}
	exPath := filepath.Dir(ex)
	fmt.Println(exPath)

	logWriter.Init()
	logger := logWriter.Writer{}
	repo := store.Init(logger)

	logger.Info("db is open")

	lcaHandler := http.LcaHandler{LcaRepo: repo, Log: logger}
	httpHandler := http.Handler{LcaHandler: lcaHandler}
	httpHandler.StartProfiling()
	logger.Write(http.Serve(httpHandler))
}
