package redis

import (
	"github.com/go-redis/redis/v7"
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

type Redis interface {
	SetToRedis(message MessageForRedis)
	SetToRedisWithExpire(message MessageForRedis, expire int, uuid string)
	GetUserAllKeys(keyPrefix string) (allKeys []string)
	GetFromRedis(key string) (result string)
	Close()
}

var RedisConn Redis

func NewRedisConn() {
	switch Conf.RedisStore {
	case "single":
		r := NewRedisSingle()
		RedisConn = r.(Redis)
	case "cluster":
		r := NewRedisCluster()
		RedisConn = r.(Redis)
	default:
		r := NewRedisSingle()
		RedisConn = r.(Redis)
	}
	Logger.Info("Initialize redis successfully with ", Conf.RedisStore)
}

type SingleRedis struct {
	client *redis.Client
}

func NewRedisSingle() interface{} {
	options := &redis.Options{
		Addr:         Conf.RedisUrl,
		DialTimeout:  time.Duration(60) * time.Second,
		ReadTimeout:  time.Duration(5) * time.Second,
		WriteTimeout: time.Duration(5) * time.Second,
		IdleTimeout:  time.Duration(5) * time.Second,
	}
	if Conf.RedisPassword != "" {
		options.Password = Conf.RedisPassword
	}
	client := redis.NewClient(options)
	_, err := client.Ping().Result()
	if err != nil {
		Logger.Error("redis PING err:", err)
		return nil
	}
	r := &SingleRedis{client: client}
	return interface{}(r)
}

func (r *SingleRedis) SetToRedis(message MessageForRedis) {
	conn := r.client.Conn()
	defer conn.Close()
	_, err := conn.Set(message.Key, message.Value, 0).Result()
	if err != nil {
		Logger.Warn("Redis set error:", err)
		return
	}
}

func (r *SingleRedis) SetToRedisWithExpire(message MessageForRedis, expire int, uuid string) {
	conn := r.client.Conn()
	defer conn.Close()
	_, err := conn.Set(message.Key, message.Value, time.Duration(expire)*time.Second).Result()
	if err != nil {
		Logger.Warn("Redis set error:", err, "uuid is:", uuid)
		return
	}
}

func (r *SingleRedis) GetUserAllKeys(keyPrefix string) (allKeys []string) {
	conn := r.client.Conn()
	defer conn.Close()
	var cursor uint64
	for {
		var keys []string
		var err error
		// we scan with our iter offset, starting at 0
		keys, cursor, err = conn.Scan(cursor, keyPrefix, 10).Result()
		if err != nil {
			panic(err)
		}
		allKeys = append(allKeys, keys...)
		// check if we need to stop...
		if cursor == 0 {
			break
		}
	}
	return
}

func (r *SingleRedis) GetFromRedis(key string) (result string) {
	conn := r.client.Conn()
	defer conn.Close()
	result, err := conn.Get(key).Result()
	if err != nil {
		Logger.Error("Redis get error:", err, "Key is:", key)
		return
	}
	return
}

func (s *SingleRedis) Close() {
	if err := s.client.Close(); err != nil {
		Logger.Error("can not close redis client. err:", err)
	}
}

type ClusterRedis struct {
	cluster *redis.ClusterClient
}

func NewRedisCluster() interface{} {
	clusterRedis := &redis.ClusterOptions{
		Addrs:        Conf.RedisGroup,
		DialTimeout:  time.Duration(60) * time.Second,
		ReadTimeout:  time.Duration(5) * time.Second,
		WriteTimeout: time.Duration(5) * time.Second,
		IdleTimeout:  time.Duration(5) * time.Second,
	}
	if Conf.RedisPassword != "" {
		clusterRedis.Password = Conf.RedisPassword
	}
	cluster := redis.NewClusterClient(clusterRedis)
	_, err := cluster.Ping().Result()
	if err != nil {
		Logger.Error("Cluster Mode redis PING err:", err)
		return nil
	}
	r := &ClusterRedis{cluster: cluster}
	return interface{}(r)
}

func (r *ClusterRedis) SetToRedis(message MessageForRedis) {
	conn := r.cluster
	_, err := conn.Set(message.Key, message.Value, 0).Result()
	if err != nil {
		Logger.Warn("Redis set error:", err)
		return
	}
}

func (r *ClusterRedis) SetToRedisWithExpire(message MessageForRedis, expire int, uuid string) {
	conn := r.cluster
	_, err := conn.Set(message.Key, message.Value, time.Duration(expire)*time.Second).Result()
	if err != nil {
		Logger.Warn("Redis set error:", err, "uuid is:", uuid)
		return
	}
}

func (r *ClusterRedis) GetUserAllKeys(keyPrefix string) (allKeys []string) {
	conn := r.cluster
	var cursor uint64
	for {
		var keys []string
		var err error
		// we scan with our iter offset, starting at 0
		keys, cursor, err = conn.Scan(cursor, keyPrefix, 10).Result()
		if err != nil {
			panic(err)
		}
		allKeys = append(allKeys, keys...)
		// check if we need to stop...
		if cursor == 0 {
			break
		}
	}
	return
}

func (r *ClusterRedis) GetFromRedis(key string) (result string) {
	conn := r.cluster
	result, err := conn.Get(key).Result()
	if err != nil {
		Logger.Error("Redis get error:", err, "Key is:", key)
		return
	}
	return
}

func (s *ClusterRedis) Close() {
	if err := s.cluster.Close(); err != nil {
		Logger.Error("can not close redis cluster err:", err)
	}
}
