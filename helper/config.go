package helper

import (
	"github.com/BurntSushi/toml"
	"io/ioutil"
)

const configPath = "/etc/yig/yig-billing.toml"

var Conf Config

type Config struct {
	LockTime              int           `toml:"lock_time"`
	RefreshLockTime       int           `toml:"refresh_lock_time"`
	CheckPoint            int           `toml:"check_point"`
	LockStore             string        `toml:"lock_store"`
	PrometheusUrl         string        `toml:"prometheus_url"`
	RegionId              string        `toml:"region_id"`
	UsageDataDir          string        `toml:"usage_data_dir"`
	BucketUsageDataDir    string        `toml:"bucket_usage_data_dir"`
	LogPath               string        `toml:"log_path"`
	LogLevel              string        `toml:"log_level"` // "info", "warn", "error"
	EnablePostBillingCron bool          `toml:"enable_post_billing_cron"`
	PostBillingSpec       string        `toml:"post_billing_spec"`
	SparkHome             string        `toml:"spark_home"`
	TisparkShell          string        `toml:"tispark_shell"`
	TisparkShellBucket    string        `toml:"tispark_shell_bucket"`
	Producer              DummyProducer `toml:"producer"`
	RedisStore            string        `toml:"redis_store"` // Choose redis connection method
	RedisUrl              string        `toml:"redis_url"`
	RedisGroup            []string      `toml:"redis_group"` // Redis cluster connection address
	RedisPassword         string        `toml:"redis_password"`
	EnableUsageCache      bool          `toml:"enable_usage_cache"`
	KafkaServer           string        `toml:"kafka_server"`
	KafkaGroupId          string        `toml:"kafka_group_id"`
	KafkaTopic            string        `toml:"kafka_topic"`
}

func ReadConfig() {
	data, err := ioutil.ReadFile(configPath)
	if err != nil {
		if err != nil {
			panic("[ERROR] Cannot open /etc/yig/yig-billing.toml")
		}
	}
	_, err = toml.Decode(string(data), &Conf)
	if err != nil {
		panic("[ERROR] Load yig-billing.toml error: " + err.Error())
	}
}
