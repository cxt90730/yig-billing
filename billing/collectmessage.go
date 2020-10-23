package billing

import (
	. "github.com/journeymidnight/yig-billing/helper"
	"github.com/journeymidnight/yig-billing/messagebus"
	"github.com/journeymidnight/yig-billing/redis"
	"strconv"
	"time"
)

const (
	TimeLayout            = "2006-01-02 15:04:05"
	SEPARATOR             = ":"
	MinSizeForStandardIa  = 64 << 10
	DEADLINEFORSTANDARDIA = 720 * time.Hour // 30 days
	DEADLINEFORGLACIER    = 1440 * time.Hour // 60 days
)

func collectMessage() {
	go messagebus.StartConsumerReceiver()
	go collector()
}

func collector() {
	for {
		message := <-messagebus.ConsumerMessagePipe
		isEffective := isMessageOfNonStandardDelete(message)
		if isEffective {
			recordUnexpiredObject(message)
		} else {
			Logger.Info("UUID is:", message.Uuid)
			continue
		}
	}
}

func isMessageOfNonStandardDelete(message messagebus.ConsumerMessage) bool {
	if message.Messages[messagebus.HttpStatus] != "200" || message.Messages[messagebus.BucketName] == "-" || message.Messages[messagebus.ObjectName] == "-" {
		Logger.Warn("Request err, uid is:", message.Uuid)
		return false
	} else {
		if message.Messages[messagebus.StorageClass] != "STANDARD" && message.Messages[messagebus.StorageClass] != "-" && message.Messages[messagebus.OperationName] == "DeleteObject"{
			return true
		}
		Logger.Warn("Request storage class err, uid is:", message.Uuid)
		return false
	}
}

func recordUnexpiredObject(msg messagebus.ConsumerMessage) {
	Logger.Info("Enter del delete object, Uuid is:", msg.Uuid)
	info := msg.Messages
	storageClass := info[messagebus.StorageClass]
	deadLine := info[messagebus.LastModifiedTime]
	expire := getDeadLine(deadLine, storageClass)
	if expire == 0 {
		return
	}
	projectId := info[messagebus.ProjectId]
	bucketName := info[messagebus.BucketName]
	objectName := info[messagebus.ObjectName]
	objectSize := info[messagebus.ObjectSize]

	redisMsg := new(redis.MessageForRedis)
	redisMsg.Key = redis.BillingUsagePrefix + projectId + SEPARATOR + storageClass + SEPARATOR + bucketName + SEPARATOR + objectName
	redisMsg.Value = getBillingSize(objectSize)
	redis.RedisConn.SetToRedisWithExpire(*redisMsg, expire, msg.Uuid)
	Logger.Info("Delete object successful, Uuid is:", msg.Uuid, ",expire is:", expire, "s")
}

func getBillingSize(objectSize string) (countSize string) {
	size, _ := strconv.ParseInt(objectSize, 10, 64)
	count := (size + MinSizeForStandardIa - 1) / MinSizeForStandardIa * MinSizeForStandardIa
	countSize = strconv.FormatInt(count, 10)
	return
}

func getDeadLine(createTime, storageclass string) (expire int) {
	var deadTime time.Time
	t, _ := time.Parse(TimeLayout, createTime)
	if storageclass == "STANDARD_IA" {
		deadTime = t.Add(DEADLINEFORSTANDARDIA)
	} else if storageclass == "GLACIER" {
		deadTime = t.Add(DEADLINEFORGLACIER)
	}
	nowTime := time.Now()
	if deadTime.Unix() > nowTime.Unix() {
		expire = int(deadTime.Unix() - nowTime.Unix())
		return
	}
	return 0
}
