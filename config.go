package main

import (
	"io/ioutil"
	"github.com/BurntSushi/toml"
)

const configPath = "/etc/yig/billing/billing.toml"

type Config struct {
	PrometheusUrl string        `toml:"prometheus_url"`
	RegionId      string        `toml:"region_id"`
	UsageDataDir  string        `toml:"usage_data_dir"`
	LogPath       string        `toml:"log_path"`
	EnableCron    bool          `toml:"enable_cron"`
	Spec          string        `toml:"spec"`
	SparkHome     string        `toml:"spark_home"`
	TisparkShell  string        `toml:"tispark_shell"`
	Producer      DummyProducer `toml:"producer"`
}

func readConfig() {
	data, err := ioutil.ReadFile(configPath)
	if err != nil {
		if err != nil {
			panic("[ERROR] Cannot open /etc/yig/billing/billing.toml")
		}
	}
	_, err = toml.Decode(string(data), &conf)
	if err != nil {
		panic("[ERROR] Load billing.toml error: " + err.Error())
	}
}
