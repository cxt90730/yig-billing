package spark

import (
	. "github.com/journeymidnight/yig-billing/helper"
	"os/exec"
	"time"
)

const TimeLayoutStr = "2006-01-02 15"

func ExecBash(params ...string) {
	Logger.Info("Begin to execBash: /bin/bash", params)

	cmd := exec.Command("/bin/bash", params...)
	err := cmd.Run()
	if err != nil {
		Logger.Error(time.Now().Format(TimeLayoutStr),": Command finished with error:", err)
	}
	Logger.Info("Finish execBash", time.Now().Format("2006-01-02 15:04:05"))
	return
}
