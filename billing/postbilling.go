package billing

import (
	"bufio"
	"encoding/json"
	. "github.com/journeymidnight/yig-billing/helper"
	"github.com/journeymidnight/yig-billing/lock"
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
	BillingTypeDataRestore  = "RESTORE"
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
	bl := lock.BillingLock
	// If the lock cannot be obtained, it acts as a standby node. When the working node fails, the standby node starts to work.
	if bl.GetOperatorPermission() {
		// Open lock to maintain Ctrip
		go bl.AutoRefreshLock()
	} else {
		Logger.Info("The node has become the standby node in this period......")
		// Check if the master node is invalid, and exit the method if it completes successfully
		if bl.StandbyStart() {
			Logger.Warn("The work process is invalid, the election is started as the work process...")
		} else {
			Logger.Info("The work node completed successfully, exit the work process, and wait for the next moment......")
			return
		}
	}
	Logger.Info("Begin to runBilling", time.Now().Format("2006-01-02 15:04:05"))
	task := new(Task)
	task.PidCache = make(map[string][]BillingUsage)
	task.RedisUsageCache = make(map[string][]BillingUsage)

	startPostBillingTime := time.Now().UnixNano() / 1e6
	// Get Usages by Redis which Standard_IA and Glacier object had been deleted
	task.ConstructDeletedUsage()
	// Get Usages by '.csv' file exported by TiSpark
	task.ConstructUsageData(Conf.TisparkShell)
	// Get Traffic From Prometheus
	task.ConstructTrafficData()
	// Get Standard_IA DataRetrieve from Prometheus
	task.ConstructRetrieveStandardIaData()
	// Get Glacier Restore(data retrieve) from Prometheus
	task.ConstructRestoreGlacierData()
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
		Logger.Error("json.Marshal", task.BillingData, "error:", err)
		bl.ExceptionNotCompleted()
		return
	}

	Logger.Info("Sending data:", string(sendingData))

	// Implement
	err = Send(Conf.Producer, sendingData)
	if err != nil {
		Logger.Error("Sending data error:", err)
		bl.ExceptionNotCompleted()
		return
	}
	// Sending data
	endPostBillingTime := time.Now().UnixNano() / 1e6
	consumeTime := endPostBillingTime - startPostBillingTime
	Logger.Info("Finish runBilling", time.Now().Format("2006-01-02 15:04:05"), "consumed time:", consumeTime, "ms")
	wg.Wait()
	bl.FinishedNotification()
	Logger.Info("The work is completed during this period, and the work pipeline is exited")
}

func Send(p Producer, data []byte) error {
	return p.Send(data)
}

type Task struct {
	BillingData                   BatchUsage
	OtherStorageClassDeletedCache map[string]BillingUsageCache
	PidCache                      map[string][]BillingUsage
	RedisUsageCache               map[string][]BillingUsage
}

func (t *Task) cacheUsageToRedis(wg *sync.WaitGroup) {
	var messages []redis.MessageForRedis
	t.ConstructUsageData(Conf.TisparkShellBucket)
	// Start Billing Usage to Redis
	Logger.Info("Start Calculate Usage", time.Now().Format("2006-01-02 15:04:05"))
	// SetToRedis
	for k, v := range t.RedisUsageCache {
		message := new(redis.MessageForRedis)
		message.Key = k
		for _, billingUsage := range v {
			if message.Value == "" {
				message.Value = billingUsage.BillType + ":" + strconv.FormatUint(billingUsage.Usage, 10)
			} else {
				message.Value = message.Value + "," + billingUsage.BillType + ":" + strconv.FormatUint(billingUsage.Usage, 10)
			}
		}
		redis.RedisConn.SetToRedis(*message)
		messages = append(messages, *message)
	}
	// Ended
	Logger.Info("Calculate usage is:", messages)
	Logger.Info("Finish Calculate usage", time.Now().Format("2006-01-02 15:04:05"))
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

func (t *Task) ConstructDeletedUsage() {
	t.OtherStorageClassDeletedCache = make(map[string]BillingUsageCache)
	allKeys, err := redis.RedisConn.GetUserAllKeys(redis.BillingUsagePrefix + "*")
	if err != nil {
		Logger.Error("ConstructDeletedUsage return is:", t.OtherStorageClassDeletedCache)
	}
	if len(allKeys) > 0 {
		for _, key := range allKeys {
			keyMembers := strings.Split(key, ":")
			pid := keyMembers[1]
			storageClass := keyMembers[2]
			usage := redis.RedisConn.GetFromRedis(key)
			if usage != "" {
				uintUsage, err := strconv.ParseUint(usage, 10, 64)
				if err != nil {
					Logger.Error("strconv.ParseInt with ConstructIADeletedUsage", usage, "error:", err)
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
	Logger.Info("ConstructDeletedUsage return is:", t.OtherStorageClassDeletedCache)
	Logger.Info("Finish ConstructDeletedUsage", time.Now().Format("2006-01-02 15:04:05"))
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
	Logger.Info(loggerUsageInfo, "usageDataDir:", usageDataDir)
	spark.ExecBash(path, usageDataDir, Conf.SparkHome)

	// Find Usage files, Construct map
	dir, err := ioutil.ReadDir(usageDataDir)
	if err != nil {
		Logger.Error("Read UsageDataDir error:", err)
		return
	}
	for _, fi := range dir {
		if fi.IsDir() {
			continue
		}
		if strings.HasSuffix(fi.Name(), ".csv") {
			filePath := usageDataDir + string(os.PathSeparator) + fi.Name()
			Logger.Info(loggerUsageInfo, "Read File:", filePath)
			t.ConstructUsageDataByFile(loggerUsageInfo, loggerUsageError, key, filePath)
			break
		}
	}
}

func (t *Task) ConstructUsageDataByFile(loggerUsageInfo, loggerUsageError, key, filePath string) {
	Logger.Info(loggerUsageInfo, "Begin to ConstructUsageDataByFile", time.Now().Format("2006-01-02 15:04:05"))
	f, err := os.Open(filePath)
	if err != nil {
		Logger.Error("Read file", filePath, "error:", err)
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
			Logger.Error("strconv.Atoi", data[1], "error:", err)
			continue
		}
		usageType := StorageClassIndexMap[(StorageClass(usageTypeIndex))]
		usageCountFlout, err := strconv.ParseFloat(data[2], 64)
		if err != nil {
			Logger.Error("strconv.ParseFloat", data[2], "error:", err)
			continue
		}
		usageCount := Wrap(usageCountFlout, 0)
		if key != Conf.TisparkShellBucket {

			t.ConstructCache(pid, usageType, usageCount)
			pid = redis.PidUsagePrefix + pid
		} else {
			pid = redis.BucketUsagePrefix + pid
		}
		billingUsages := []BillingUsage{
			{BillType: usageType, Usage: usageCount},
		}
		if t.RedisUsageCache[pid] == nil {
			t.RedisUsageCache[pid] = billingUsages
		} else {
			t.RedisUsageCache[pid] = append(t.RedisUsageCache[pid], BillingUsage{BillType: usageType, Usage: usageCount})
		}
	}
	Logger.Info(loggerUsageInfo, "Finish ConstructUsageDataByFile", time.Now().Format("2006-01-02 15:04:05"))
}

func Wrap(num float64, retain int) uint64 {
	return uint64(num * math.Pow10(retain))
}

func (t *Task) ConstructTrafficData() {
	Logger.Info("Begin to ConstructTrafficData", time.Now().Format("2006-01-02 15:04:05"))
	usageType := BillingTypeTraffic
	// `sum(increase(yig_http_response_size_bytes{is_private_subnet="false", method="GET", cdn_request="false"}[1h]))by(bucket_owner)`
	queryString := "sum(increase(yig_http_response_size_bytes{is_private_subnet=%22false%22,method=%22GET%22,cdn_request=%22false%22}[1h]))by(bucket_owner)"
	res := prometheus.GetDataFromPrometheus(queryString)
	if res == nil {
		Logger.Error("Get Empty TrafficData")
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
			Logger.Error("strconv.ParseFloat", usageString, "err:", err)
			continue
		}
		t.ConstructCache(pid, usageType, uint64(math.Ceil(usageFloat)))
	}
	Logger.Info("Finish ConstructTrafficData", time.Now().Format("2006-01-02 15:04:05"))
}

func (t *Task) ConstructRetrieveStandardIaData() {
	Logger.Info("Begin to ConstructDateRetrieveData", time.Now().Format("2006-01-02 15:04:05"))
	usageType := BillingTypeDataRetrieve
	// `sum(increase(yig_http_response_size_bytes{is_private_subnet="false", method="GET", cdn_request="false"}[1h]))by(bucket_owner)`
	queryString := "sum(increase(yig_data_retrieve_size_bytes{method=%22GET%22,operation=%22GetObject%22,storage_class=%22STANDARD_IA%22}[1h]))by(bucket_owner)"
	res := prometheus.GetDataFromPrometheus(queryString)
	if res == nil {
		Logger.Error("Get Empty DateRetrieve")
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
			Logger.Error("strconv.ParseFloat", usageString, "err:", err)
			continue
		}
		t.ConstructCache(pid, usageType, uint64(math.Ceil(usageFloat)))
	}
	Logger.Info("Finish ConstructRetrieveStandardIaData", time.Now().Format("2006-01-02 15:04:05"))
}

func (t *Task) ConstructRestoreGlacierData() {
	Logger.Info("Begin to ConstructDateRetrieveData", time.Now().Format("2006-01-02 15:04:05"))
	usageType := BillingTypeDataRestore
	// `sum(increase(yig_http_response_size_bytes{is_private_subnet="false", method="GET", cdn_request="false"}[1h]))by(bucket_owner)`
	queryString := "sum(increase(yig_data_restore_size_bytes{method=%22POST%22,operation=%22RestoreObject%22,storage_class=%22GLACIER%22}[1h]))by(bucket_owner)"
	res := prometheus.GetDataFromPrometheus(queryString)
	if res == nil {
		Logger.Error("Get Empty Restore")
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
			Logger.Error("strconv.ParseFloat", usageString, "err:", err)
			continue
		}
		t.ConstructCache(pid, usageType, uint64(math.Ceil(usageFloat)))
	}
	Logger.Info("Finish ConstructRestoreGlacierData", time.Now().Format("2006-01-02 15:04:05"))
}

func (t *Task) ConstructAPIData() {
	Logger.Info("Begin to ConstructAPIData", time.Now().Format("2006-01-02 15:04:05"))

	usageType := BillingTypeAPI
	queryString := `sum(increase(yig_http_response_count_total[1h]))by(bucket_owner)`
	res := prometheus.GetDataFromPrometheus(queryString)
	if res == nil {
		Logger.Error("Get Empty TrafficData")
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
			Logger.Error("strconv.ParseFloat", usageString, "err:", err)
			continue
		}
		t.ConstructCache(pid, usageType, uint64(math.Ceil(usageFloat)))
	}
	Logger.Info("Finish ConstructAPIData", time.Now().Format("2006-01-02 15:04:05"))
}
