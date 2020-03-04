package tikv

import (
	"context"
	"errors"
	"github.com/journeymidnight/yig-billing/helper"
	"strings"
	"time"
)

func genBucketKey(bucketName string) []byte {
	return GenKey(TableBucketPrefix, bucketName)
}

func genUserBucketKey(ownerId, bucketName string) []byte {
	return GenKey(TableUserBucketPrefix, ownerId, bucketName)
}

const (
	MaxUserBucketKey = 10000
)

func (c *TiKVClient) PutNewBucket(bucket Bucket) error {
	bucketKey := genBucketKey(bucket.Name)
	userBucketKey := genUserBucketKey(bucket.OwnerId, bucket.Name)
	existBucket, err := c.TxExist(bucketKey)
	if err != nil {
		return err
	}
	existUserBucket, err := c.TxExist(userBucketKey)
	if err != nil {
		return err
	}
	if existBucket && existUserBucket {
		return errors.New("bucket already exists")
	}

	return c.TxPut(bucketKey, bucket, userBucketKey, 0)
}

func (c *TiKVClient) FetchUserBuckets(
	userBuckets map[string][]string, timestamp uint64) {

	for {
		iter, err := c.TxIter([]byte(`u\`),
			[]byte("u\\\xff"), // 3 bytes, ascii: [117 92 255]
			timestamp)
		if err != nil {
			helper.Logger.Error("TxIter error:", err)
			time.Sleep(time.Second)
			continue
		}
		for iter.Valid() {
			// should be u\{UserID}\{BucketName}
			split := strings.Split(string(iter.Key()), `\`)
			if len(split) != 3 {
				_ = iter.Next(context.TODO())
				continue
			}
			userName := split[1]
			bucketName := split[2]
			userBuckets[userName] = append(userBuckets[userName], bucketName)
			_ = iter.Next(context.TODO())
		}
		iter.Close()
		break
	}
}
