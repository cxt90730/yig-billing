package helper

import (
	"log"
	"os"
)

var Logger *log.Logger

func NewLogger() {
	f, err := os.OpenFile(Conf.LogPath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		panic("Failed to open log file " + Conf.LogPath)
	}
	Logger = log.New(f, "[yig]", log.LstdFlags)
	Logger.Println("[INFO] Start Yig Billing...")
	Logger.Printf("[TRACE] Config:\n %+v", Conf)
	return
}
