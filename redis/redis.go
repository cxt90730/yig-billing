package redis

import (
	"github.com/garyburd/redigo/redis"
	. "github.com/journeymidnight/yig-billing/helper"
	"time"
)

const (
	PidUsagePrefix    = "u_p_"
	BucketUsagePrefix = "u_b_"
)

type MessageForRedis struct {
	Key   string
	Value string
}

type Redis struct {
	pool *redis.Pool
}

var RedisConn *Redis

func NewRedisConn() {
	pwd := redis.DialPassword(Conf.RedisPassword)
	RedisConn = new(Redis)
	RedisConn.pool = &redis.Pool{
		MaxIdle:     3,
		IdleTimeout: 60 * time.Second,
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", Conf.RedisUrl, pwd)
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
	Logger.Println("[INFO] Initialize redis successfully")
}

func (r *Redis) SetToRedis(message MessageForRedis) {
	conn := r.pool.Get()
	_, err := conn.Do("SET", message.Key, message.Value)
	if err != nil {
		Logger.Println("[WARNING] Redis set error:", err)
		return
	}
	defer conn.Close()
}
