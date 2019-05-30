package writer

import (
	"os"

	log "github.com/sirupsen/logrus"
)

//Writer for logs
type Writer struct {
}

//Init is the constructor here
func Init() {
	log.SetFormatter(&log.TextFormatter{
		FullTimestamp: true,
	})

	log.SetOutput(os.Stdout)
}

func (writer Writer) Write(err error) {
	if err != nil {
		writer.Error(err.Error())
	}
}

//Info message log
func (writer Writer) Info(message string) {
	log.Info(message)
}

//Debug message log
func (writer Writer) Debug(message string) {
	log.Debug(message)
}

//Warn message log
func (writer Writer) Warn(message string) {
	log.Warn(message)
}

//Error message log
func (writer Writer) Error(message string) {
	log.Error(message)
}

//Fatal message log
func (writer Writer) Fatal(message string) {
	log.Fatal(message)
}

//Panic message log
func (writer Writer) Panic(message string) {
	log.Panic(message)
}
