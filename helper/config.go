package helper

import (
	"github.com/BurntSushi/toml"
	"io/ioutil"
)

const configPath = "/etc/yig/yig-billing.toml"

var Conf Config

type MeepoConfig struct {
	SelfAddress     string `toml:"self_address"`
	SelfPort        int    `toml:"self_port"`
	RegistryAddress string `toml:"registry_address"`
	RegistryPort    int    `toml:"registry_port"`
	Concurrence     int    `toml:"concurrence"`
}

type TikvConfig struct {
	PdAddresses []string `toml:"pd_addresses"`
}

type Config struct {
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
	RedisUrl              string        `toml:"redis_url"`
	RedisPassword         string        `toml:"redis_password"`
	EnableUsageCache      bool          `toml:"enable_usage_cache"`
	KafkaServer           string        `toml:"kafka_server"`
	KafkaGroupId          string        `toml:"kafka_group_id"`
	KafkaTopic            string        `toml:"kafka_topic"`
	Meepo                 MeepoConfig   `toml:"meepo"`
	Tikv                  TikvConfig    `toml:"tikv"`
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
