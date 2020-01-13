package spark

import (
	. "github.com/journeymidnight/yig-billing/helper"
	"os/exec"
	"time"
)

const TimeLayoutStr = "2006-01-02 15"

func ExecBash(params ...string) {
	Logger.Println("[TRACE] Begin to execBash: /bin/bash", params)

	cmd := exec.Command("/bin/bash", params...)
	//开始执行c包含的命令，但并不会等待该命令完成即返回
	cmd.Start()

	Logger.Printf("[TRACE] Waiting for command:%v to finish...\n", params)
	//阻塞等待fork出的子进程执行的结果，和cmd.Start()配合使用[不等待回收资源，会导致fork出执行shell命令的子进程变为僵尸进程]
	err := cmd.Wait()
	if err != nil {
		Logger.Printf("[ERROR] %v: Command finished with error: %v\n", time.Now().Format(TimeLayoutStr), err)
	}
	Logger.Println("[TRACE] Finish execBash", time.Now().Format("2006-01-02 15:04:05"))
	return
}
