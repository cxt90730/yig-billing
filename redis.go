package main

import (
	"github.com/garyburd/redigo/redis"
	"sync"
	"time"
)

const (
	PidUsagePrefix    = "u_p_"
	BucketUsagePrefix = "u_b_"
)

func newRedisConn(address, password string) *redis.Pool {
	pwd := redis.DialPassword(password)
	return &redis.Pool{
		MaxIdle:     3,
		IdleTimeout: 60 * time.Second,
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", address, pwd)
			if err != nil {
				return nil, err
			}
			return c, err
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			_, err := c.Do("PING")
			return err
		},
	}
}

func setToRedis(wg *sync.WaitGroup, pool *redis.Pool, key string, value BillingUsage) {
	newKey := key
	conn := pool.Get()
	_, err := conn.Do("SET", newKey, value)
	if err != nil {
		logger.Println("[WARNING] Redis set error:", err)
		return
	}
	wg.Done()
}

func setUidToRedis(cache map[string]BillingUsage) {
	wg := new(sync.WaitGroup)
	conn := newRedisConn(conf.RedisUrl, conf.RedisPassword)
	defer conn.Close()
	for key, value := range cache {
		wg.Add(1)
		setToRedis(wg, conn, key, value)
	}
	wg.Wait()
}

