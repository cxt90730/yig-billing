package main

import (
	"context"
	"errors"
	"fmt"
	"github.com/journeymidnight/yig-billing/helper"
	"github.com/journeymidnight/yig-billing/log"
	pb "github.com/journeymidnight/yig-billing/mapreduce/proto"
	"google.golang.org/grpc"
	"net"
	"sync/atomic"
	"time"
)

var RunningJobs int64 = 0

func main() {
	helper.ReadConfig()
	logLevel := log.ParseLevel(helper.Conf.LogLevel)
	helper.Logger = log.NewFileLogger(helper.Conf.LogPath, logLevel)

	meepoConf := helper.Conf.Meepo
	tikvConf := helper.Conf.Tikv
	helper.Logger.Info("My IP:", meepoConf.SelfAddress, "port:", meepoConf.SelfPort)
	helper.Logger.Info(
		"Registry IP:", meepoConf.RegistryAddress, "port:", meepoConf.RegistryPort)

	registryClient, err := newRegistryClient(meepoConf)
	if err != nil {
		panic("Cannot connect to registry: " + err.Error())
	}

	go heartbeat(meepoConf, registryClient)

	listener, err := net.Listen("tcp",
		fmt.Sprintf("[::]:%d", meepoConf.SelfPort))
	if err != nil {
		panic("net.Listen error: " + err.Error())
	}
	grpcServer := grpc.NewServer()
	pb.RegisterMeepoServer(grpcServer, NewMeepo(
		registryClient, tikvConf.PdAddresses, meepoConf.Concurrence))
	err = grpcServer.Serve(listener)
	helper.Logger.Error("Server stopped with error:", err)
}

func newRegistryClient(conf helper.MeepoConfig) (pb.MeepoRegistryClient, error) {
	ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
	conn, err := grpc.DialContext(ctx,
		fmt.Sprintf("%s:%d", conf.RegistryAddress, conf.RegistryPort),
		grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		return nil, errors.New("grpc.Dial error: " + err.Error())
	}
	client := pb.NewMeepoRegistryClient(conn)
	return client, nil
}

func heartbeat(conf helper.MeepoConfig, registryClient pb.MeepoRegistryClient) {
	for {
		self := pb.MeepoInstance{
			IP:          conf.SelfAddress,
			Port:        int32(conf.SelfPort),
			RunningJobs: atomic.LoadInt64(&RunningJobs),
		}
		_, err := registryClient.Register(context.Background(), &self)
		if err != nil {
			helper.Logger.Error("Register failed:", err)
		}

		time.Sleep(10 * time.Second) // heartbeat interval
	}
}
