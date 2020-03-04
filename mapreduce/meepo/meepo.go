package main

import (
	"context"
	"fmt"
	"github.com/journeymidnight/yig-billing/helper"
	pb "github.com/journeymidnight/yig-billing/mapreduce/proto"
	"github.com/journeymidnight/yig-billing/tikv"
	"github.com/tikv/client-go/txnkv/kv"
	"strings"
	"sync"
	"time"
)

func NewMeepo(registryClient pb.MeepoRegistryClient,
	pdAddresses []string, concurrence int) *Meepo {

	m := &Meepo{
		registryClient: registryClient,
		runningJobs:    make(map[string]pb.Job),
		jobLock:        sync.Mutex{},
		dispatchQueue:  make(chan pb.Job, 1000),
		tikvClient:     tikv.NewClient(pdAddresses),
	}
	for i := 0; i < concurrence; i++ {
		go m.workRoutine()
	}
	return m
}

type Meepo struct {
	registryClient pb.MeepoRegistryClient
	// job unique identifier -> job struct,
	// where "job unique identifier" = "timestamp"-"job ID"
	runningJobs map[string]pb.Job
	// lock for "runningJobs"
	jobLock       sync.Mutex
	dispatchQueue chan pb.Job
	tikvClient    *tikv.TiKVClient
}

func (m *Meepo) AddJob(ctx context.Context, job *pb.Job) (*pb.Empty, error) {
	jobIdentifier := fmt.Sprintf("%d-%d", job.Timestamp, job.Id)
	m.jobLock.Lock()
	defer m.jobLock.Unlock()

	if _, ok := m.runningJobs[jobIdentifier]; ok { // duplicate, do nothing
		return &pb.Empty{}, nil
	}
	helper.Logger.Info("New job", jobIdentifier, "added:",
		string(job.StartKey), "->", string(job.EndKey))
	m.runningJobs[jobIdentifier] = *job
	m.dispatchQueue <- *job
	return &pb.Empty{}, nil
}

func (m *Meepo) workRoutine() {
	for {
		job, ok := <-m.dispatchQueue
		if !ok { // use channel closing as indication of meepo stop
			return
		}
		helper.Logger.Info("Handling job:", job.Timestamp, job.Id)
		var iter kv.Iterator
		var err error
		err = helper.WithRetry(3, func() error {
			iter, err = m.tikvClient.TxIter(job.StartKey, job.EndKey, job.Timestamp)
			return err
		})()
		if err != nil {
			helper.Logger.Error("Job", job.Timestamp, job.Id, "failed:", err)
			job.Error = err.Error()
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			_, err = m.registryClient.JobDone(ctx, &job)
			cancel()
			if err != nil {
				helper.Logger.Error("Job", job.Timestamp, job.Id, "JobDone failed:", err)
			}
			continue
		}

		result := make(map[string]*pb.UsageByClass)
		for iter.Valid() {
			job.Entries += 1
			m.processEntry(result, iter.Key(), iter.Value())
			_ = iter.Next(context.TODO()) // TODO error handling
		}
		iter.Close()
		job.Usage = result
		err = helper.WithRetry(3, func() error {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			_, err = m.registryClient.JobDone(ctx, &job)
			return err
		})()
		if err != nil {
			helper.Logger.Error("Job", job.Timestamp, job.Id,
				"JobDone failed:", err)
		}
		helper.Logger.Info("Job finished:", job.Timestamp, job.Id,
			"entry processed:", job.Entries)
	}
}

const _64KiB = 64 << 10

func (m *Meepo) processEntry(result map[string]*pb.UsageByClass, k []byte, v []byte) {
	if len(k) < 2 {
		return
	}
	if k[1] == tikv.TableSeparatorChar {
		switch k[0] {
		case tikv.TableObjectPartChar:
			var objectPart tikv.Part
			err := helper.MsgPackUnMarshal(v, &objectPart)
			if err != nil {
				helper.Logger.Warn("Bad part:", string(k), "err:", err)
				return
			}
			// p\{BucketName}\{ObjectName}\{UploadId}\{BE.Num}
			bucketName := strings.Split(string(k), tikv.TableSeparator)[1]
			if result[bucketName] == nil {
				result[bucketName] = &pb.UsageByClass{}
			}
			// no storage class info for "Part", so be it
			result[bucketName].Standard += objectPart.Size
			return
		default:
			// we don't care other types of entries
			return
		}
	}
	// second byte not `\`, an ordinary object
	var object tikv.Object
	err := helper.MsgPackUnMarshal(v, &object)
	if err != nil {
		helper.Logger.Warn("Bad object:", string(k), "err:", err)
		return
	}
	// {BucketName}\{ObjectName}
	bucketName := strings.Split(string(k), tikv.TableSeparator)[0]
	if result[bucketName] == nil {
		result[bucketName] = &pb.UsageByClass{}
	}
	switch object.StorageClass {
	case tikv.ObjectStorageClassStandard:
		result[bucketName].Standard += object.Size
	case tikv.ObjectStorageClassGlacier:
		result[bucketName].Glacier += (object.Size + _64KiB - 1) / _64KiB * _64KiB
	case tikv.ObjectStorageClassStandardIa:
		result[bucketName].StandardIA += (object.Size + _64KiB - 1) / _64KiB * _64KiB
	default:
		return
	}
}
