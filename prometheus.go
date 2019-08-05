package main

import (
	"net/http"
	"io/ioutil"
	"encoding/json"
)

type PrometheusResponse struct {
	Status string         `json:"status"`
	Data   PrometheusData `json:"data"`
}

type PrometheusData struct {
	ResultType string             `json:"resultType"`
	Result     []PrometheusResult `json:"result"`
}

type PrometheusResult struct {
	Metric interface{}   `json:"metric"`
	Value  []interface{} `json:"value"`
}

func getDataFromPrometheus(queryString string) (p *PrometheusResponse) {
	res, err := http.Get(conf.PrometheusUrl + "/api/v1/query?query=" + queryString)
	if err != nil {
		logger.Println("[ERROR] GetTrafficFromPrometheus:", err)
		return
	}
	defer res.Body.Close()
	data, err := ioutil.ReadAll(res.Body)
	if err != nil {
		logger.Println("[ERROR] Read Prometheus data err:", err)
		return
	}
	p = new(PrometheusResponse)
	err = json.Unmarshal(data, p)
	if err != nil {
		logger.Println("[ERROR] json.Unmarshal Prometheus data err:", err)
		return nil
	}
	return p

}
