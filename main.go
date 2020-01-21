package main

import (
	"github.com/journeymidnight/yig-billing/billing"
	"github.com/journeymidnight/yig-billing/helper"
	"github.com/journeymidnight/yig-billing/messagebus"
	"github.com/journeymidnight/yig-billing/redis"
	"log"
	"os"
	"os/signal"
	"runtime"
	"syscall"
)

func main() {
	// Initialization modules
	initModules()

	// Start billing server
	go billing.Billing()

	signal.Ignore()
	signalQueue := make(chan os.Signal)
	signal.Notify(signalQueue, syscall.SIGINT, syscall.SIGTERM,
		syscall.SIGQUIT, syscall.SIGHUP, syscall.SIGUSR1)
	for {
		s := <-signalQueue
		switch s {
		case syscall.SIGHUP:
			log.Println("[WARNING] Recieve signal SIGHUP")
			// reload units
			initModules()
		case syscall.SIGUSR1:
			log.Println("[WARNING] Recieve signal SIGUSR1")
			go DumpStacks()
		default:
			log.Println("[WARNING] Recieve signal:", s.String())
			log.Println("[INFO] Stop yig billing...")
			return
		}
	}
}

func DumpStacks() {
	buf := make([]byte, 1<<16)
	stacklen := runtime.Stack(buf, true)
	helper.Logger.Printf("=== received SIGQUIT ===\n*** goroutine dump...\n%s\n*** end\n", buf[:stacklen])
}

func initModules() {
	// Load configuration files and logs
	helper.ReadConfig()
	helper.NewLogger()
	// Initialize Redis config
	redis.NewRedisConn()
	// Initialize kafka consumer
	messagebus.NewConsumer()
}
