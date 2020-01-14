package billing

import (
	"bufio"
	"encoding/json"
	. "github.com/journeymidnight/yig-billing/helper"
	"github.com/journeymidnight/yig-billing/prometheus"
	"github.com/journeymidnight/yig-billing/redis"
	"github.com/journeymidnight/yig-billing/spark"

	"io/ioutil"
	"math"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	BillingTypeAPI          = "API"
	BillingTypeTraffic      = "TRAFFIC"
	BillingTypeDataRetrieve = "DATARETRIEVE"
	LoggerBucketInfo        = "PLUGIN LOG"
	LoggerBuckteError       = "WARRING BUT NOT IMPORTANT"
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

type BillingUsageCache struct {
	Cache map[string]uint64
}

const folderLayoutStr = "2006010215"

func postBilling() {
	wg := new(sync.WaitGroup)
	Logger.Println("[INFO] Begin to runBilling", time.Now().Format("2006-01-02 15:04:05"))
	task := new(Task)
	task.PidCache = make(map[string][]BillingUsage)
	task.RedisUsageCache = make(map[string]BillingUsage)

	// Get Usages by Redis which other storage class object had been deleted
	task.ConstructUsageOtherStorageClassBeenDeletedData()
	// Get Usages by '.csv' file exported by TiSpark
	task.ConstructUsageData(Conf.TisparkShell)
	// Get Traffic From Prometheus
	task.ConstructTrafficData()
	// Get Standard_IA DataRetrieve from Prometheus
	task.ConstructRetrieveStandardIaData()
	// Get API Count From Prometheus
	task.ConstructAPIData()
	// If Enable Usage Cache Is On, Cache Usage to Redis
	if Conf.EnableUsageCache {
		wg.Add(1)
		go task.cacheUsageToRedis(wg)
	}

	// Constuct BillingData
	task.BillingData.RegionId = Conf.RegionId
	hd, _ := time.ParseDuration("-1h")
	task.BillingData.BeginTime = time.Now().Add(hd).Format(spark.TimeLayoutStr) + ":00:00"
	task.BillingData.EndTime = time.Now().Format(spark.TimeLayoutStr) + ":00:00"
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
		Logger.Println("[ERROR] json.Marshal", task.BillingData, "error:", err)
		return
	}

	Logger.Println("[TRACE] Sending data:", string(sendingData))

	// Implement
	err = Send(Conf.Producer, sendingData)
	if err != nil {
		Logger.Println("[ERROR] Sending data error:", err)
		return
	}
	// Sending data
	Logger.Println("[INFO] Finish runBilling", time.Now().Format("2006-01-02 15:04:05"))
	wg.Wait()

}

func Send(p Producer, data []byte) error {
	return p.Send(data)
}

type Task struct {
	BillingData                   BatchUsage
	OtherStorageClassDeletedCache map[string]BillingUsageCache
	PidCache                      map[string][]BillingUsage
	RedisUsageCache               map[string]BillingUsage
}

func (t *Task) cacheUsageToRedis(wg *sync.WaitGroup) {
	t.ConstructUsageData(Conf.TisparkShellBucket)
	// Start Billing Usage to Redis
	Logger.Println("[PLUGIN] Start Calculate Usage", time.Now().Format("2006-01-02 15:04:05"))
	// SetToRedis
	messages := []redis.MessageForRedis{}
	for k, v := range t.RedisUsageCache {
		message := new(redis.MessageForRedis)
		message.Key = k
		value := redis.RedisConn.GetFromRedis(k)
		if len(value) > 0 {
			message.Value = value + v.BillType + ":" + strconv.FormatUint(v.Usage, 10) + ","
		} else {
			message.Value = v.BillType + ":" + strconv.FormatUint(v.Usage, 10) + ","
		}
		redis.RedisConn.SetToRedis(*message)
		messages = append(messages, *message)
	}
	// Ended
	Logger.Println("[MESSAGE] Calculate usage to redis with：", messages)
	Logger.Println("[PLUGIN] Finish Calculate usage", time.Now().Format("2006-01-02 15:04:05"))
	wg.Done()
}

func (t *Task) ConstructCache(pid, usageType string, usageCount uint64) {
	if cache := t.OtherStorageClassDeletedCache[pid].Cache; cache != nil {
		for k, v := range cache {
			if k == usageType {
				usageCount = usageCount + v
			}
		}
	}
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

func (t *Task) ConstructUsageOtherStorageClassBeenDeletedData() {
	t.OtherStorageClassDeletedCache = make(map[string]BillingUsageCache)
	allKeys := redis.RedisConn.GetUserAllKeys(redis.BillingUsagePrefix + "*")
	if len(allKeys) > 0 {
		for _, key := range allKeys {
			keyMembers := strings.Split(key, ":")
			pid := keyMembers[1]
			storageClass := keyMembers[2]
			usage := redis.RedisConn.GetFromRedis(key)
			if usage != "" {
				uintUsage, err := strconv.ParseUint(usage, 10, 64)
				if err != nil {
					Logger.Println("[ERROR] strconv.ParseInt with UsageOtherStorageClassBeenDeleted", usage, "error:", err)
				}
				if t.OtherStorageClassDeletedCache[pid].Cache == nil {
					billingCache := new(BillingUsageCache)
					cache := make(map[string]uint64)
					cache[storageClass] = uintUsage
					billingCache.Cache = cache
					t.OtherStorageClassDeletedCache[pid] = *billingCache
				} else {
					isUsageBeenCache := false
					for k, _ := range t.OtherStorageClassDeletedCache[pid].Cache {
						if k == storageClass {
							t.OtherStorageClassDeletedCache[pid].Cache[storageClass] = t.OtherStorageClassDeletedCache[pid].Cache[storageClass] + uintUsage
							isUsageBeenCache = true
						}
					}
					if !isUsageBeenCache {
						t.OtherStorageClassDeletedCache[pid].Cache[storageClass] = uintUsage
					}
				}
			}
		}
	}
	Logger.Println("[MESSAGE] ConstructUsageOtherStorageClassBeenDeletedData return is:", t.OtherStorageClassDeletedCache)
	Logger.Println("[TRACE] Finish ConstructUsageOtherStorageClassBeenDeletedData", time.Now().Format("2006-01-02 15:04:05"))
}

func (t *Task) ConstructUsageData(path string) {
	var loggerUsageInfo, loggerUsageError string
	key := path
	if key == Conf.TisparkShellBucket {
		loggerUsageInfo = LoggerBucketInfo
		loggerUsageError = LoggerBuckteError
	} else {
		loggerUsageInfo = ""
		loggerUsageError = ""
	}
	hour := time.Now().Format(folderLayoutStr)
	usageDataDir := Conf.UsageDataDir + string(os.PathSeparator) + hour
	Logger.Println(loggerUsageInfo, "[TRACE] usageDataDir:", usageDataDir)
	spark.ExecBash(path, usageDataDir, Conf.SparkHome)

	// Find Usage files, Construct map
	dir, err := ioutil.ReadDir(usageDataDir)
	if err != nil {
		Logger.Println("[ERROR] Read UsageDataDir error:", err)
		return
	}
	for _, fi := range dir {
		if fi.IsDir() {
			continue
		}
		if strings.HasSuffix(fi.Name(), ".csv") {
			filePath := usageDataDir + string(os.PathSeparator) + fi.Name()
			Logger.Println(loggerUsageInfo, "[TRACE] Read File:", filePath)
			t.ConstructUsageDataByFile(loggerUsageInfo, loggerUsageError, key, filePath)
			break
		}
	}
}

func (t *Task) ConstructUsageDataByFile(loggerUsageInfo, loggerUsageError, key, filePath string) {
	Logger.Println(loggerUsageInfo, "[TRACE] Begin to ConstructUsageDataByFile", time.Now().Format("2006-01-02 15:04:05"))
	f, err := os.Open(filePath)
	if err != nil {
		Logger.Println("[ERROR] Read file", filePath, "error:", err)
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
			Logger.Println(loggerUsageError, "[ERROR] strconv.Atoi", data[1], "error:", err)
			continue
		}
		usageType := StorageClassIndexMap[(StorageClass(usageTypeIndex))]
		usageCountFlout, err := strconv.ParseFloat(data[2], 64)
		if err != nil {
			Logger.Println(loggerUsageError, "[ERROR] strconv.ParseFloat", data[2], "error:", err)
			continue
		}
		usageCount := Wrap(usageCountFlout, 1)
		if key != Conf.TisparkShellBucket {

			t.ConstructCache(pid, usageType, usageCount)
			pid = redis.PidUsagePrefix + pid
		} else {
			pid = redis.BucketUsagePrefix + pid
		}
		t.RedisUsageCache[pid] = BillingUsage{BillType: usageType, Usage: usageCount}
	}
	Logger.Println(loggerUsageInfo, "[TRACE] Finish ConstructUsageDataByFile", time.Now().Format("2006-01-02 15:04:05"))
}

func Wrap(num float64, retain int) uint64 {
	return uint64(num * math.Pow10(retain))
}

func (t *Task) ConstructTrafficData() {
	Logger.Println("[TRACE] Begin to ConstructTrafficData", time.Now().Format("2006-01-02 15:04:05"))
	usageType := BillingTypeTraffic
	// `sum(increase(yig_http_response_size_bytes{is_private_subnet="false", method="GET", cdn_request="false"}[1h]))by(bucket_owner)`
	queryString := "sum(increase(yig_http_response_size_bytes{is_private_subnet=%22false%22,method=%22GET%22,cdn_request=%22false%22}[1h]))by(bucket_owner)"
	res := prometheus.GetDataFromPrometheus(queryString)
	if res == nil {
		Logger.Println("[ERROR] Get Empty TrafficData")
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
			Logger.Println("[ERROR] strconv.ParseFloat", usageString, "err:", err)
			continue
		}
		t.ConstructCache(pid, usageType, uint64(math.Ceil(usageFloat)))
	}
	Logger.Println("[TRACE] Finish ConstructTrafficData", time.Now().Format("2006-01-02 15:04:05"))
}

func (t *Task) ConstructRetrieveStandardIaData() {
	Logger.Println("[TRACE] Begin to ConstructDateRetrieveData", time.Now().Format("2006-01-02 15:04:05"))
	usageType := BillingTypeDataRetrieve
	// `sum(increase(yig_http_response_size_bytes{is_private_subnet="false", method="GET", cdn_request="false"}[1h]))by(bucket_owner)`
	queryString := "sum(increase(yig_http_response_size_bytes{is_private_subnet=%22false%22,method=%22GET%22,cdn_request=%22false%22,storage_class=%22STANDARD_IA%22}[1h]))by(bucket_owner)"
	res := prometheus.GetDataFromPrometheus(queryString)
	if res == nil {
		Logger.Println("[ERROR] Get Empty TrafficData")
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
			Logger.Println("[ERROR] strconv.ParseFloat", usageString, "err:", err)
			continue
		}
		t.ConstructCache(pid, usageType, uint64(math.Ceil(usageFloat)))
	}
	Logger.Println("[TRACE] Finish ConstructTrafficData", time.Now().Format("2006-01-02 15:04:05"))
}

func (t *Task) ConstructAPIData() {
	Logger.Println("[TRACE] Begin to ConstructAPIData", time.Now().Format("2006-01-02 15:04:05"))

	usageType := BillingTypeAPI
	queryString := `sum(increase(yig_http_response_count_total[1h]))by(bucket_owner)`
	res := prometheus.GetDataFromPrometheus(queryString)
	if res == nil {
		Logger.Println("[ERROR] Get Empty TrafficData")
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
			Logger.Println("[ERROR] strconv.ParseFloat", usageString, "err:", err)
			continue
		}
		t.ConstructCache(pid, usageType, uint64(math.Ceil(usageFloat)))
	}
	Logger.Println("[TRACE] Finish ConstructAPIData", time.Now().Format("2006-01-02 15:04:05"))
}
