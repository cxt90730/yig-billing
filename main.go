package main

import (
	"github.com/journeymidnight/yig-billing/billing"
	"github.com/journeymidnight/yig-billing/helper"
	"github.com/journeymidnight/yig-billing/lock"
	"github.com/journeymidnight/yig-billing/log"
	"github.com/journeymidnight/yig-billing/messagebus"
	"github.com/journeymidnight/yig-billing/redis"
	"os"
	"os/signal"
	"runtime"
	"syscall"
)

func main() {
	// Initialization modules
	// Load configuration files and logs
	helper.ReadConfig()
	logLevel := log.ParseLevel(helper.Conf.LogLevel)
	helper.Logger = log.NewFileLogger(helper.Conf.LogPath, logLevel)
	defer helper.Logger.Close()
	helper.Logger.Info("YIG conf:", helper.Conf)
	helper.Logger.Info("YIG instance ID:", helper.GenerateRandomId())
	// Initialize Redis config
	redis.NewRedisConn()
	defer redis.RedisConn.Close()
	// Initialize kafka consumer
	messagebus.NewConsumer()

	// Initialize distributed lock
	lock.NewLock()

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
			helper.Logger.Warn("Recieve signal SIGHUP")
		case syscall.SIGUSR1:
			helper.Logger.Warn("Recieve signal SIGUSR1")
			go DumpStacks()
		default:
			helper.Logger.Warn("Recieve signal:", s.String())
			helper.Logger.Info("Stop yig billing...")
			return
		}
	}
}

func DumpStacks() {
	buf := make([]byte, 1<<16)
	stacklen := runtime.Stack(buf, true)
	helper.Logger.Warn("=== received SIGQUIT ===\n*** goroutine dump...\n%s\n*** end\n", buf[:stacklen])
}
