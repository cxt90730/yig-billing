package tikv

import (
	"context"
	"github.com/journeymidnight/yig-billing/helper"
)

func GenObjectKey(bucketName, objectName, version string) []byte {
	// TODO: GetLatestObject
	if version == NullVersion || version == "" {
		return GenKey(bucketName, objectName)
	} else {
		return GenKey(bucketName, objectName, version)
	}
}

func (c *TiKVClient) PutObject(object *Object) error {
	objectKey := GenObjectKey(object.BucketName, object.Name, object.VersionId)
	tx, err := c.TxnCli.Begin(context.TODO())
	if err != nil {
		return err
	}
	defer func() {
		if err == nil {
			err = tx.Commit(context.TODO())
		}
		if err != nil {
			_ = tx.Rollback()
		}
	}()

	objectVal, err := helper.MsgPackMarshal(object)
	if err != nil {
		return err
	}

	err = tx.Set(objectKey, objectVal)
	if err != nil {
		return err
	}
	return nil
}
