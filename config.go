package main

import (
	"github.com/BurntSushi/toml"
	"io/ioutil"
)

const configPath = "/etc/yig/yig-billing.toml"

type Config struct {
	PrometheusUrl      string        `toml:"prometheus_url"`
	RegionId           string        `toml:"region_id"`
	UsageDataDir       string        `toml:"usage_data_dir"`
	BucketUsageDataDir string        `toml:"bucket_usage_data_dir"`
	LogPath            string        `toml:"log_path"`
	EnableCron         bool          `toml:"enable_cron"`
	Spec               string        `toml:"spec"`
	SparkHome          string        `toml:"spark_home"`
	TisparkShell       string        `toml:"tispark_shell"`
	TisparkShellBucket string        `toml:"tispark_shell_bucket"`
	Producer           DummyProducer `toml:"producer"`
	RedisUrl           string        `toml:"redis_url"`
	RedisPassword      string        `toml:"redis_password"`
	EnableUsageCache   bool          `toml:"enable_usage_cache"`
}

func readConfig() {
	data, err := ioutil.ReadFile(configPath)
	if err != nil {
		if err != nil {
			panic("[ERROR] Cannot open /etc/yig/yig-billing.toml")
		}
	}
	_, err = toml.Decode(string(data), &conf)
	if err != nil {
		panic("[ERROR] Load yig-billing.toml error: " + err.Error())
	}
}
