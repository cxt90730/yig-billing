package billing

import (
	. "github.com/journeymidnight/yig-billing/helper"
	"github.com/journeymidnight/yig-billing/messagebus"
	"github.com/journeymidnight/yig-billing/redis"
	"strconv"
)

const (
	DEADLINE             = 2592000
	SEPARATOR            = ":"
	MinSizeForStandardIa = 64 << 10
)

func collectMessage() {
	go messagebus.StartConsumerReceiver()
	go collector()
}

func collector() {
	for {
		message := <-messagebus.ConsumerMessagePipe
		isEffective := isMessageEffective(message)
		if !isEffective {
			Logger.Println("[INVALID MESSAGE] UUID is:", message.Uuid)
			continue
		}
		if message.Messages[messagebus.OperationName] == "DeleteObject" {
			delDeleteObject(message)
		} else {
			Logger.Println("[INVALID MESSAGE] UUID is:", message.Uuid)
		}
	}
}

func isMessageEffective(message messagebus.ConsumerMessage) bool {
	if message.Messages[messagebus.HttpStatus] != "200" || message.Messages[messagebus.BucketName] == "-" || message.Messages[messagebus.ObjectName] == "-" {
		Logger.Println("[INVALID MESSAGE] Request err, uid is:", message.Uuid)
		return false
	} else {
		if message.Messages[messagebus.StorageClass] != "STANDARD" && message.Messages[messagebus.StorageClass] != "-" {
			return true
		}
		Logger.Println("[INVALID MESSAGE] Request storage class err, uid is:", message.Uuid)
		return false
	}
}

func delDeleteObject(msg messagebus.ConsumerMessage) {
	Logger.Println("[MESSAGE] Enter del delete object, Uuid is:", msg.Uuid)
	info := msg.Messages
	projectId := info[messagebus.ProjectId]
	bucketName := info[messagebus.BucketName]
	objectName := info[messagebus.ObjectName]
	storageClass := info[messagebus.StorageClass]
	objectSize := info[messagebus.ObjectSize]
	redisMsg := new(redis.MessageForRedis)
	redisMsg.Key = redis.BillingUsagePrefix + projectId + SEPARATOR + storageClass + SEPARATOR + bucketName + SEPARATOR + objectName
	redisMsg.Value = getBillingSize(objectSize)
	redis.RedisConn.SetToRedisWithExpire(*redisMsg, DEADLINE, msg.Uuid)
	Logger.Println("[MESSAGE] Delete object successful, Uuid is:", msg.Uuid)
}

func getBillingSize(objectSize string) (countSize string) {
	size, _ := strconv.ParseInt(objectSize, 10, 64)
	count := (size + MinSizeForStandardIa - 1) / MinSizeForStandardIa * MinSizeForStandardIa
	countSize = strconv.FormatInt(count, 10)
	return
}
