package prometheus

import (
	"encoding/json"
	. "github.com/journeymidnight/yig-billing/helper"
	"io/ioutil"
	"net/http"
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

func GetDataFromPrometheus(queryString string) (p *PrometheusResponse) {
	res, err := http.Get(Conf.PrometheusUrl + "/api/v1/query?query=" + queryString)
	if err != nil {
		Logger.Println("[ERROR] GetTrafficFromPrometheus:", err)
		return
	}
	defer res.Body.Close()
	data, err := ioutil.ReadAll(res.Body)
	if err != nil {
		Logger.Println("[ERROR] Read Prometheus data err:", err)
		return
	}
	p = new(PrometheusResponse)
	err = json.Unmarshal(data, p)
	if err != nil {
		Logger.Println("[ERROR] json.Unmarshal Prometheus data err:", err)
		return nil
	}
	return p

}
