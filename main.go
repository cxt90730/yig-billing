package main

import (
	"time"
	"github.com/robfig/cron"
	"log"
	"os"
	"io/ioutil"
	"strings"
	"bufio"
	"strconv"
	"math"
	"encoding/json"
	"os/signal"
	"syscall"
	"runtime"
)

const (
	BillingTypeAPI     = "API"
	BillingTypeTraffic = "TRAFFIC"
)

type BatchUsage struct {
	RegionId  string      `json:"regionId"`
	BeginTime string      `json:"beginTime"`
	EndTime   string      `json:"endTime"`
	Data      []UserUsage `json:"data"`
}

type UserUsage struct {
	ProjectId string         `json:"projectId"`
	Usages    []BillingUsage `json:"usages"`
}

type BillingUsage struct {
	BillType string `json:"billType"`
	Usage    uint64 `json:"usage"`
}

var logger *log.Logger
var conf Config

const timeLayoutStr = "2006-01-02 15"
const folderLayoutStr = "2006010215"

func runBilling() {
	logger.Println("[INFO] Begin to runBilling", time.Now().Format("2006-01-02 15:04:05"))
	task := new(Task)
	task.PidCache = make(map[string][]BillingUsage)

	// Get Usages by '.csv' file exported by TiSpark
	task.ConstructUsageData()
	// Get Traffic From Prometheus
	task.ConstructTrafficData()
	// Get API Count From Prometheus
	task.ConstructAPIData()

	// Constuct BillingData
	task.BillingData.RegionId = conf.RegionId
	hd, _ := time.ParseDuration("-1h")
	task.BillingData.BeginTime = time.Now().Add(hd).Format(timeLayoutStr) + ":00:00"
	task.BillingData.EndTime = time.Now().Format(timeLayoutStr) + ":00:00"
	for pid, u := range task.PidCache {
		// Ignore test data
		if pid == "-" || pid == "hehehehe" {
			continue
		}
		userUsage := UserUsage{
			ProjectId: pid,
			Usages:    u,
		}
		task.BillingData.Data = append(task.BillingData.Data, userUsage)
	}

	sendingData, err := json.Marshal(task.BillingData)
	if err != nil {
		logger.Println("[ERROR] json.Marshal", task.BillingData, "error:", err)
		return
	}

	logger.Println("[TRACE] Sending data:", string(sendingData))

	// Implement
	err = Send(conf.Producer, sendingData)
	if err != nil {
		logger.Println("[ERROR] Sending data error:", err)
		return
	}
	// Sending data
	logger.Println("[INFO] Finish runBilling", time.Now().Format("2006-01-02 15:04:05"))
}

func Send(p Producer, data []byte) error {
	return p.Send(data)
}

type Task struct {
	BillingData BatchUsage
	PidCache    map[string][]BillingUsage
}

func (t *Task) ConstructCache(pid, usageType string, usageCount uint64) {
	usage := BillingUsage{
		BillType: usageType,
		Usage:    usageCount,
	}
	if _, ok := t.PidCache[pid]; ok {
		t.PidCache[pid] = append(t.PidCache[pid], usage)
	} else {
		t.PidCache[pid] = []BillingUsage{usage}
	}
}

func (t *Task) ConstructUsageData() {
	hour := time.Now().Format(folderLayoutStr)
	usageDataDir := conf.UsageDataDir + string(os.PathSeparator) + hour
	logger.Println("[TRACE] usageDataDir:", usageDataDir)
	execBash(conf.TisparkShell, usageDataDir, conf.SparkHome)

	// Find Usage files, Construct map
	dir, err := ioutil.ReadDir(usageDataDir)
	if err != nil {
		logger.Println("[ERROR] Read UsageDataDir error:", err)
		return
	}
	for _, fi := range dir {
		if fi.IsDir() {
			continue
		}
		if strings.HasSuffix(fi.Name(), ".csv") {
			filePath := usageDataDir + string(os.PathSeparator) + fi.Name()
			logger.Println("[TRACE] Read File:", filePath)
			t.ConstructUsageDataByFile(filePath)
			break
		}
	}
}

func (t *Task) ConstructUsageDataByFile(filePath string) {
	logger.Println("[TRACE] Begin to ConstructUsageDataByFile", time.Now().Format("2006-01-02 15:04:05"))
	f, err := os.Open(filePath)
	if err != nil {
		logger.Println("[ERROR] Read file", filePath, "error:", err)
		return
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := sc.Text()

		data := strings.Split(line, ",")
		if len(data) < 3 {
			continue
		}
		pid := data[0]
		usageTypeIndex, err := strconv.Atoi(data[1])
		if err != nil {
			logger.Println("[ERROR] strconv.Atoi", data[1], "error:", err)
			continue
		}
		usageType := StorageClassIndexMap[(StorageClass(usageTypeIndex))]
		usageCount, err := strconv.ParseUint(data[2], 10, 64)
		if err != nil {
			logger.Println("[ERROR] strconv.ParseUint", data[2], "error:", err)
			continue
		}
		t.ConstructCache(pid, usageType, usageCount)
	}
	logger.Println("[TRACE] Finish ConstructUsageDataByFile", time.Now().Format("2006-01-02 15:04:05"))
}

func (t *Task) ConstructTrafficData() {
	logger.Println("[TRACE] Begin to ConstructTrafficData", time.Now().Format("2006-01-02 15:04:05"))
	usageType := BillingTypeTraffic
	// `sum(increase(yig_http_response_size_bytes{is_private_subnet="false", method="GET", cdn_request="false"}[1h]))by(bucket_owner)`
	queryString := "sum(increase(yig_http_response_size_bytes{is_private_subnet=%22false%22,method=%22GET%22,cdn_request=%22false%22}[1h]))by(bucket_owner)"
	res := getDataFromPrometheus(queryString)
	if res == nil {
		logger.Println("[ERROR] Get Empty TrafficData")
		return
	}
	for _, v := range res.Data.Result {
		var pid, usageString string
		pidMap, ok := v.Metric.(map[string]interface{})
		if !ok {
			continue
		} else {
			pid = pidMap["bucket_owner"].(string)
		}
		if len(v.Value) < 2 {
			continue
		}
		// float string
		usageString = v.Value[1].(string)
		if usageString == "0" {
			continue
		}
		usageFloat, err := strconv.ParseFloat(usageString, 64)
		if err != nil {
			logger.Println("[ERROR] strconv.ParseFloat", usageString, "err:", err)
			continue
		}
		t.ConstructCache(pid, usageType, uint64(math.Ceil(usageFloat)))
	}
	logger.Println("[TRACE] Finish ConstructTrafficData", time.Now().Format("2006-01-02 15:04:05"))

}

func (t *Task) ConstructAPIData() {
	logger.Println("[TRACE] Begin to ConstructAPIData", time.Now().Format("2006-01-02 15:04:05"))

	usageType := BillingTypeAPI
	queryString := `sum(increase(yig_http_response_count_total[1h]))by(bucket_owner)`
	res := getDataFromPrometheus(queryString)
	if res == nil {
		logger.Println("[ERROR] Get Empty TrafficData")
		return
	}
	for _, v := range res.Data.Result {
		var pid, usageString string
		pidMap, ok := v.Metric.(map[string]interface{})
		if !ok {
			continue
		} else {
			pid = pidMap["bucket_owner"].(string)
		}
		if len(v.Value) < 2 {
			continue
		}
		// float string
		usageString = v.Value[1].(string)
		if usageString == "0" {
			continue
		}
		usageFloat, err := strconv.ParseFloat(usageString, 64)
		if err != nil {
			logger.Println("[ERROR] strconv.ParseFloat", usageString, "err:", err)
			continue
		}
		t.ConstructCache(pid, usageType, uint64(math.Ceil(usageFloat)))
	}
	logger.Println("[TRACE] Finish ConstructAPIData", time.Now().Format("2006-01-02 15:04:05"))
}

func main() {
	readConfig()
	f, err := os.OpenFile(conf.LogPath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		panic("Failed to open log file " + conf.LogPath)
	}
	defer f.Close()
	logger = log.New(f, "[yig]", log.LstdFlags)

	logger.Println("[INFO] Start Yig Billing...")
	logger.Printf("[TRACE] Config:\n %+v", conf)

	if !conf.EnableCron {
		runBilling()
	} else {
		c := cron.New()
		c.AddFunc(conf.Spec, runBilling)
		c.Start()
		signal.Ignore()
		signalQueue := make(chan os.Signal)
		signal.Notify(signalQueue, syscall.SIGINT, syscall.SIGTERM,
			syscall.SIGQUIT, syscall.SIGHUP, syscall.SIGUSR1)
		for {
			s := <-signalQueue
			switch s {
			case syscall.SIGHUP:
				log.Println("[WARNING] Recieve signal SIGHUP")
				// reload config file
				readConfig()
			case syscall.SIGUSR1:
				log.Println("[WARNING] Recieve signal SIGUSR1")
				go DumpStacks()
			default:
				log.Println("[WARNING] Recieve signal:", s.String())
				log.Println("[INFO] Stop yig billing...")
				return
			}
		}
	}
}

func DumpStacks() {
	buf := make([]byte, 1<<16)
	stacklen := runtime.Stack(buf, true)
	logger.Printf("=== received SIGQUIT ===\n*** goroutine dump...\n%s\n*** end\n", buf[:stacklen])
}
