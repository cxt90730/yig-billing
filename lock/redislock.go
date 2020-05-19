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
	down  chan bool
	mutex *redislock.Lock
)

type RedisLock struct{}

func (r *RedisLock) AutoRefreshLock() {
	c := time.Tick(time.Duration(helper.Conf.RefreshLockTime) * time.Minute)
	for {
		select {
		case <-c:
			err := mutex.Refresh(time.Duration(helper.Conf.LockTime)*time.Minute, nil)
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
		case <-down:
			helper.Logger.Info("Successfully exit the lock maintenance process...")
		}
	}
}

func (r *RedisLock) GetOperatorPermission() bool {
	down = make(chan bool)
	m, err := redis.Locker.Obtain(YIG_BILLING_LOCK, time.Duration(helper.Conf.LockTime)*time.Minute, nil)
	if err == redislock.ErrNotObtained {
		helper.Logger.Error("Failed to obtain work permission!")
		return false
	} else if err != nil {
		helper.Logger.Error("Lock seems does not work, so quit", err.Error())
		panic(err)
	}
	mutex = m
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
	down <- true
	err = mutex.Release()
	if err != nil {
		helper.Logger.Error("Failed to manually release the releasable lock, err is:", err.Error())
	}
}

func (r *RedisLock) StandbyStart() bool {
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
