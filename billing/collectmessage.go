package billing

import (
	"github.com/journeymidnight/yig-billing/db"
	. "github.com/journeymidnight/yig-billing/helper"
	"github.com/journeymidnight/yig-billing/messagebus"
	"math"
	"strconv"
	"time"
)

const (
	MinSizeForStandardIa = 64 << 10
	Deleted              = uint8(1)
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
		}
		switch message.Messages[messagebus.OperationName] {
		case "PutObject":
			delPutObject(message)
			break
		case "PutObjectPart":
			delPutObjectPart(message)
			break
		case "CopyObject":
			delCopyObject(message)
			break
		case "CopyObjectPart":
			delCopyObjectPart(message)
			break
		case "RenameObject":
			delRenameObject(message)
			break
		case "PostObject":
			delPostObject(message)
			break
		case "DeleteObject":
			delDeleteObject(message)
			break
		default:
			Logger.Println("[INVALID MESSAGE] UUID is:", message.Uuid)
		}
	}
}

func isMessageEffective(message messagebus.ConsumerMessage) bool {
	if message.Messages[messagebus.HttpStatus] != "200" || message.Messages[messagebus.BucketName] == "-" || message.Messages[messagebus.ObjectName] == "-" {
		return false
	} else {
		if message.Messages[messagebus.StorageClass] != "STANDARD" || message.Messages[messagebus.TargetStorageClass] != "STANDARD" || (message.Messages[messagebus.StorageClass] != "-" && message.Messages[messagebus.TargetStorageClass] != "-" || (message.Messages[messagebus.StorageClass] == "STANDARD" && message.Messages[messagebus.TargetStorageClass] == "STANDARD")) {
			return true
		}
		return false
	}
}

// TODO : Verify the data in the database, and verify whether the message is out of date, that is, to determine the modification time, Determine whether the storage type conversion is from low frequency to standard, and then how to handle the data in the database and whether to delete it
func delPutObject(msg messagebus.ConsumerMessage) {
	Logger.Println("[MESSAGE] Enter del put object, Uuid is:", msg.Uuid)
	info := msg.Messages
	object := new(db.BillingInfo)
	object.ProjectId = info[messagebus.ProjectId]
	object.BucketName = info[messagebus.BucketName]
	object.ObjectName = info[messagebus.ObjectName]
	storageClass := StorageClassStringMap[info[messagebus.TargetStorageClass]]
	object.StorageClass = uint8(storageClass)
	object.CreateTime, _ = time.Parse(db.TIME_LAYOUT_TIDB, info[messagebus.TimeLocal])
	v := math.MaxUint64 - uint64(object.CreateTime.UnixNano())
	object.Version = strconv.FormatUint(v, 10)
	bodyBytes, _ := strconv.ParseInt(info[messagebus.BodyBytesSent], 10, 64)
	objectSize, _ := strconv.ParseInt(info[messagebus.ObjectSize], 10, 64)
	amount := bodyBytes - objectSize
	if amount == 0 {
		object.Size = objectSize
		object.CountSize = getCountSize(object.Size)
		err := db.DbClient.UpdateBilling(*object)
		if err != nil {
			Logger.Println("[MESSAGE ERROR] Del Put Object Error with database update, err:", err, "Uuid is :", msg.Uuid)
			return
		}
	} else {
		object.Size = bodyBytes
		object.CountSize = getCountSize(object.Size)
		err := db.DbClient.InsertBilling(*object)
		if err != nil {
			Logger.Println("[MESSAGE ERROR] Del Put Object Error with database insert, err:", err, "Uuid is :", msg.Uuid)
			return
		}
	}
	Logger.Println("[MESSAGE] Put object successful, Uuid is:", msg.Uuid)
}

// TODO
func delPutObjectPart(msg messagebus.ConsumerMessage) {

}

// TODO : When the source object is inconsistent with the target object, update the entire object information
func delCopyObject(msg messagebus.ConsumerMessage) {
	Logger.Println("[MESSAGE] Enter del Copy object, Uuid is:", msg.Uuid)
	info := msg.Messages
	object := new(db.BillingInfo)
	object.ProjectId = info[messagebus.ProjectId]
	object.BucketName = info[messagebus.BucketName]
	object.ObjectName = info[messagebus.ObjectName]
	object.CreateTime, _ = time.Parse(db.TIME_LAYOUT_TIDB, info[messagebus.TimeLocal])
	v := math.MaxUint64 - uint64(object.CreateTime.UnixNano())
	object.Version = strconv.FormatUint(v, 10)
	objectSize, _ := strconv.ParseInt(info[messagebus.ObjectSize], 10, 64)
	object.Size = objectSize
	object.CountSize = getCountSize(object.Size)
	srcStorageClass := StorageClassStringMap[info[messagebus.StorageClass]]
	targetStorageClass := StorageClassStringMap[info[messagebus.TargetStorageClass]]
	if srcStorageClass == 0 {
		object.StorageClass = uint8(targetStorageClass)
		err := db.DbClient.InsertBilling(*object)
		if err != nil {
			Logger.Println("[MESSAGE ERROR] Del Copy Object Error with database insert, err:", err, "Uuid is :", msg.Uuid)
			return
		}
	} else if targetStorageClass == 0 {
		err := db.DbClient.DeleteBillingObject(*object)
		if err != nil {
			Logger.Println("[MESSAGE ERROR] Del Copy Object Error with database delete, err:", err, "Uuid is :", msg.Uuid)
			return
		}
	} else {
		object.StorageClass = uint8(targetStorageClass)
		err := db.DbClient.UpdateBilling(*object)
		if err != nil {
			Logger.Println("[MESSAGE ERROR] Del Copy Object Error with database update, err:", err, "Uuid is :", msg.Uuid)
			return
		}
	}
	Logger.Println("[MESSAGE] Copy object successful, Uuid is:", msg.Uuid)
}

// TODO
func delCopyObjectPart(msg messagebus.ConsumerMessage) {

}

// TODO
func delRenameObject(msg messagebus.ConsumerMessage) {

}

// TODO
func delPostObject(msg messagebus.ConsumerMessage) {

}

func delDeleteObject(msg messagebus.ConsumerMessage) {
	Logger.Println("[MESSAGE] Enter del delete object, Uuid is:", msg.Uuid)
	info := msg.Messages
	object := new(db.BillingInfo)
	object.ProjectId = info[messagebus.ProjectId]
	object.BucketName = info[messagebus.BucketName]
	object.ObjectName = info[messagebus.ObjectName]
	object.CreateTime, _ = time.Parse(db.TIME_LAYOUT_TIDB, info[messagebus.TimeLocal])
	object.Deleted = Deleted
	object.DeleteTime = object.CreateTime.AddDate(0, 1, 0)
	err := db.DbClient.UpdateBillingFlag(*object)
	if err != nil {
		Logger.Println("[MESSAGE ERROR] Del Delete Object Error with database update, err:", err, "Uuid is :", msg.Uuid)
		return
	}
	Logger.Println("[MESSAGE] Delete object successful, Uuid is:", msg.Uuid)
}

// Round up for the smallest unit
func getCountSize(objectSize int64) (countSize int64) {
	countSize = objectSize - (objectSize % MinSizeForStandardIa) + MinSizeForStandardIa
	return
}
