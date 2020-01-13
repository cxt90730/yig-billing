package db

import (
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
	. "github.com/journeymidnight/yig-billing/helper"
	"os"
	"time"
)

const TIME_LAYOUT_TIDB = "2006-01-02 15:04:05"

type TidbClient struct {
	Client *sql.DB
}

type BillingInfo struct {
	ProjectId    string
	BucketName   string
	ObjectName   string
	Version      string
	StorageClass uint8
	Size         int64
	CountSize    int64
	CreateTime   time.Time
	Deleted      uint8
	DeleteTime   time.Time
}

var DbClient *TidbClient

func NewTIdbClient() {
	DbClient = &TidbClient{}
	conn, err := sql.Open("mysql", Conf.TidbConnection)
	if err != nil {
		Logger.Println("[ERROR] Initialize database error!", err, Conf.TidbConnection)
		os.Exit(1)
	}
	conn.SetMaxIdleConns(Conf.DbMaxIdleConns)
	conn.SetMaxOpenConns(Conf.DbMaxOpenConns)
	conn.SetConnMaxLifetime(time.Duration(Conf.DbConnMaxLifeSeconds) * time.Second)
	DbClient.Client = conn
	Logger.Println("[INFO] Initialize database successful!")
	return
}

func (DB *TidbClient) InsertBilling(b BillingInfo) (err error) {
	var sqlTx *sql.Tx
	var tx interface{}
	tx, err = DB.Client.Begin()
	if err != nil {
		return err
	}
	defer func() {
		if err == nil {
			err = sqlTx.Commit()
		}
		if err != nil {
			sqlTx.Rollback()
		}
	}()
	sqlTx, _ = tx.(*sql.Tx)
	_, err = sqlTx.Exec("insert into userbilling(ownerid,bucketname,objectname,version,storageclass,size,countsize,lastmodifiedtime,deleted,deltime) values (?,?,?,?,?,?,?,?,?)", b.ProjectId, b.BucketName, b.ObjectName, b.Version, b.StorageClass, b.Size, b.CountSize, b.CreateTime, b.Deleted, b.DeleteTime)
	if err != nil {
		return err
	}
	return nil
}

func (DB *TidbClient) UpdateBilling(b BillingInfo) (err error) {
	var sqlTx *sql.Tx
	var tx interface{}
	tx, err = DB.Client.Begin()
	if err != nil {
		return err
	}
	defer func() {
		if err == nil {
			err = sqlTx.Commit()
		}
		if err != nil {
			sqlTx.Rollback()
		}
	}()
	sqlTx, _ = tx.(*sql.Tx)
	_, err = sqlTx.Exec("update userbilling set storageclass=?,size=?,countsize=?,lastmodifiedtime=? where ownerid=? and bucketname=? and objectname=?", b.StorageClass, b.Size, b.CountSize, b.CreateTime.Format(TIME_LAYOUT_TIDB), b.ProjectId, b.BucketName, b.ObjectName)
	if err != nil {
		return err
	}
	return nil
}

func (DB *TidbClient) UpdateBillingFlag(b BillingInfo) (err error) {
	var sqlTx *sql.Tx
	var tx interface{}
	tx, err = DB.Client.Begin()
	if err != nil {
		return err
	}
	defer func() {
		if err == nil {
			err = sqlTx.Commit()
		}
		if err != nil {
			sqlTx.Rollback()
		}
	}()
	sqlTx, _ = tx.(*sql.Tx)
	_, err = sqlTx.Exec("update userbilling set deleted=?,deltime=? where ownerid=? and bucketname=? and objectname=?", b.Deleted, b.DeleteTime.Format(TIME_LAYOUT_TIDB), b.ProjectId, b.BucketName, b.ObjectName)
	if err != nil {
		return err
	}
	return nil
}

func (DB *TidbClient) DeleteBillingObject(b BillingInfo) (err error) {
	var sqlTx *sql.Tx
	var tx interface{}
	tx, err = DB.Client.Begin()
	if err != nil {
		return err
	}
	defer func() {
		if err == nil {
			err = sqlTx.Commit()
		}
		if err != nil {
			sqlTx.Rollback()
		}
	}()
	sqlTx, _ = tx.(*sql.Tx)
	_, err = sqlTx.Exec("delete from userbilling where ownerid=? and bucketname=? and objectname=?", b.ProjectId, b.BucketName, b.ObjectName)
	if err != nil {
		return err
	}
	return nil
}

func (DB *TidbClient) DeleteBillingObjects(deleted uint8, time time.Time) (err error) {
	var sqlTx *sql.Tx
	var tx interface{}
	tx, err = DB.Client.Begin()
	if err != nil {
		return err
	}
	defer func() {
		if err == nil {
			err = sqlTx.Commit()
		}
		if err != nil {
			sqlTx.Rollback()
		}
	}()
	sqlTx, _ = tx.(*sql.Tx)
	_, err = sqlTx.Exec("delete from userbilling where deleted=? and deltime<?", deleted, time.Format(TIME_LAYOUT_TIDB))
	if err != nil {
		return err
	}
	return nil
}
