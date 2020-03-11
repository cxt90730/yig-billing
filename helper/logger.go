package helper

import (
	"github.com/journeymidnight/yig-billing/log"
)

// Global singleton loggers
var Logger log.Logger

func PanicOnError(err error, message string) {
	if err != nil {
		panic(message + " " + err.Error())
	}
}
