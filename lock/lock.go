package lock

import "github.com/journeymidnight/yig-billing/helper"

type Lock interface {
	AutoRefreshLock()
	GetOperatorPermission() bool
	FinishedNotification()
	StandbyStart() bool
}

var BillingLock Lock

func NewLock() {
	switch helper.Conf.LockStore {
	case "redis":
		BillingLock = &RedisLock{}
	}
}
