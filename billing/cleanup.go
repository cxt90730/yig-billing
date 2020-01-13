package billing

import (
	"github.com/journeymidnight/yig-billing/db"
	. "github.com/journeymidnight/yig-billing/helper"
	"time"
)

func cleanUpExpiredObjects() {
	time.Sleep(10 * time.Minute)
	Logger.Println("[INFO] Begin to cleanUpExpiredObjects", time.Now().Format("2006-01-02 15:04:05"))
	deleted := uint8(1)
	delTime := time.Now().UTC()
	err := db.DbClient.DeleteBillingObjects(deleted, delTime)
	if err != nil {
		Logger.Println("[ERROR] cleanUpExpiredObjects error:", err)
		return
	}
	Logger.Println("[INFO] Finish to cleanUpExpiredObjects", time.Now().Format("2006-01-02 15:04:05"))
	return
}
