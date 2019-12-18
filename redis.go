package main

import "github.com/garyburd/redigo/redis"

const (
	FLAG   = "Usage_Cache_"
	Bucket = "Bucket_"
)

func redisConn(address, password string) (redis.Conn, error) {
	pwd := redis.DialPassword(password)
	conn, err := redis.Dial("tcp", address, pwd)
	if err != nil {
		return nil, err
	}
	return conn, nil
}

func setToRedis(key string, value BillingUsage) {
	newKey := FLAG + key
	conn, err := redisConn(conf.RedisUrl, conf.RedisPassword)
	if err != nil {
		logger.Println("[WARNING] Connection redis error:", err)
		return
	}
	defer conn.Close()
	_, err = conn.Do("SET", newKey, value)
	if err != nil {
		logger.Println("[WARNING] Redis set error:", err)
		return
	}
}

func setUidToRedis(cache map[string]BillingUsage) {
	for key, value := range cache {
		setToRedis(key, value)
	}
}

