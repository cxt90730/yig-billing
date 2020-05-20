package lock

import (
	"github.com/journeymidnight/yig-billing/spark"
	"time"

	"github.com/bsm/redislock"
	"github.com/journeymidnight/yig-billing/helper"
	"github.com/journeymidnight/yig-billing/redis"
)

const (
	YIG_BILLING_LOCK     = "yig_billing_redis_lock_key"
	FINISH_BILLING_CHECK = "yig_billing_finish_billing_time:"
)

var (
	Down  bool
	mutex *redislock.Lock
)

type RedisLock struct{}

func (r *RedisLock) AutoRefreshLock() {
	helper.Logger.Info("Open lock maintenance procedure......")
	c := time.Tick(time.Duration(helper.Conf.RefreshLockTime) * time.Second)
	for {
		select {
		case <-c:
			if Down {
				helper.Logger.Info("release the lock......")
				return
			}
			err := mutex.Refresh(time.Duration(helper.Conf.LockTime)*time.Second, nil)
			switch err {
			case nil:
				helper.Logger.Info("Refresh lock success...")
				continue
			case redislock.ErrNotObtained:
				helper.Logger.Info("No longer hold lock ...")
				return
			default:
				helper.Logger.Error("Refresh lock failed, err is:", err)
			}
		}
	}
}

func (r *RedisLock) GetOperatorPermission() bool {
	Down = false
	m, err := redis.Locker.Obtain(YIG_BILLING_LOCK, time.Duration(helper.Conf.LockTime)*time.Minute, nil)
	if err == redislock.ErrNotObtained {
		helper.Logger.Error("Failed to obtain work permission!")
		return false
	} else if err != nil {
		helper.Logger.Error("Lock seems does not work, so quit", err.Error())
		panic(err)
	}
	if isFinished() {
		return false
	}
	mutex = m
	helper.Logger.Info("Successfully acquired the lock......")
	return true
}

func (r *RedisLock) FinishedNotification() {
	info := redis.MessageForRedis{
		Key:   FINISH_BILLING_CHECK + time.Now().Format(spark.TimeLayoutStr) + ":00:00",
		Value: time.Now().String(),
	}
	ok, err := redis.RedisConn.SetNXToRedis(info, 1800)
	if err != nil {
		helper.Logger.Error("Failed to set completion flag, err is:", err.Error())
	}
	if !ok {
		helper.Logger.Error("The flag has been set!")
	}
	Down = true
	err = mutex.Release()
	if err != nil {
		helper.Logger.Error("Failed to manually release the releasable lock, err is:", err.Error())
	}
	helper.Logger.Info("Operation completed......")
}

func (r *RedisLock) ExceptionNotCompleted() {
	Down = true
	err := mutex.Release()
	if err != nil {
		helper.Logger.Error("Failed to manually release the releasable lock, err is:", err.Error())
	}
	helper.Logger.Warn("The program exits abnormally, release the lock and let the standby node try again......")
}

func (r *RedisLock) StandbyStart() bool {
	if isFinished() {
		return false
	}
	count := 0
	timeout := make(chan bool)
	c := time.Tick(time.Duration(helper.Conf.CheckPoint) * time.Minute)
	for {
		select {
		case <-c:
			switch isFinished() {
			case false:
				if r.GetOperatorPermission() {
					return true
				}
				count++
				if count == 8 {
					timeout <- true
				}
				continue
			case true:
				return false
			}
		case <-timeout:
			return false
		}
	}
}

func isFinished() bool {
	key := FINISH_BILLING_CHECK + time.Now().Format(spark.TimeLayoutStr) + ":00:00"
	result := redis.RedisConn.GetFromRedis(key)
	if result != "" {
		return true
	}
	return false
}
