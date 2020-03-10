package redis

import (
	"github.com/garyburd/redigo/redis"
	. "github.com/journeymidnight/yig-billing/helper"
	"time"
)

const (
	PidUsagePrefix     = "u_p_"
	BucketUsagePrefix  = "u_b_"
	BillingUsagePrefix = "b:"
	MATCH              = "match"
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
	Logger.Info("Initialize redis successfully")
}

func (r *Redis) SetToRedis(message MessageForRedis) {
	conn := r.pool.Get()
	defer conn.Close()
	_, err := conn.Do("SET", message.Key, message.Value)
	if err != nil {
		Logger.Warn("Redis set error:", err)
		return
	}
}

func (r *Redis) SetToRedisWithExpire(message MessageForRedis, expire int, uuid string) {
	conn := r.pool.Get()
	defer conn.Close()
	_, err := conn.Do("SETEX", message.Key, expire, message.Value)
	if err != nil {
		Logger.Warn("Redis set error:", err, "uuid is:", uuid)
		return
	}
}

func (r *Redis) GetUserAllKeys(keyPrefix string) (allKeys []string) {
	conn := r.pool.Get()
	defer conn.Close()
	iter := 0
	var keys []string
	for {
		// we scan with our iter offset, starting at 0
		if arr, err := redis.MultiBulk(conn.Do("SCAN", iter, MATCH, keyPrefix)); err != nil {
			Logger.Error("Redis sacn error:", err, "KeyPrefix is:", keyPrefix)
		} else {
			// now we get the iter and the keys from the multi-bulk reply
			iter, _ = redis.Int(arr[0], nil)
			keys, _ = redis.Strings(arr[1], nil)
		}
		allKeys = append(allKeys, keys...)
		// check if we need to stop...
		if iter == 0 {
			break
		}
	}
	return
}

func (r *Redis) GetFromRedis(key string) (result string) {
	conn := r.pool.Get()
	defer conn.Close()
	result, err := redis.String(conn.Do("GET", key))
	if err != nil {
		Logger.Error("Redis get error:", err, "Key is:", key)
		return
	}
	return
}
