package helper

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha1"
	"encoding/base64"
	"errors"
	"hash"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
)

type QuerySorter struct {
	Keys []string
	Vals []string
}

// Additional function for function SignHeader.
func newQuerySorter(m map[string]string) *QuerySorter {
	hs := &QuerySorter{
		Keys: make([]string, 0, len(m)),
		Vals: make([]string, 0, len(m)),
	}
	for k, v := range m {
		hs.Keys = append(hs.Keys, k)
		hs.Vals = append(hs.Vals, v)
	}
	return hs
}

func GetSignature(httpMethod, requestUrl, secret string) string {
	u, err := url.Parse(requestUrl)
	if err != nil {
		panic(err)
	}
	temp := make(map[string]string)
	queryPair := strings.Split(u.RawQuery, "&")
	for _, pair := range queryPair {
		kvPair := strings.SplitN(pair, "=", 2)
		if len(kvPair) == 1 {
			temp[kvPair[0]] = percentEncode("")
		} else if len(kvPair) == 2 {
			temp[kvPair[0]] = percentEncode(kvPair[1])
		}
	}
	qs := newQuerySorter(temp)
	qs.Sort()
	canonicalizedQueryString := ""
	for i := range qs.Keys {
		canonicalizedQueryString += qs.Keys[i] + "=" + qs.Vals[i] + "&"
	}
	canonicalizedQueryString = canonicalizedQueryString[:len(canonicalizedQueryString)-1]
	Logger.Println("[TRACE] canonicalizedQueryString:", canonicalizedQueryString)
	stringToSign := httpMethod + "&" + percentEncode("/") + "&" +
		percentEncode(canonicalizedQueryString)
	Logger.Println("[TRACE] StringToSign:", stringToSign)
	Logger.Println("[TRACE] SecretKey:", secret)
	h := hmac.New(func() hash.Hash { return sha1.New() }, []byte(secret+"&"))
	io.WriteString(h, stringToSign)
	signedStr := url.QueryEscape(base64.StdEncoding.EncodeToString(h.Sum(nil)))
	newQueryString, _ := url.QueryUnescape(canonicalizedQueryString)
	if strings.Contains(newQueryString, "#") {
		newQueryString = strings.Replace(newQueryString, "#", "%23", -1)
	}
	return signedStr
}

// Additional function for function SignHeader.
func (hs *QuerySorter) Sort() {
	sort.Sort(hs)
}

// Additional function for function SignHeader.
func (hs *QuerySorter) Len() int {
	return len(hs.Vals)
}

// Additional function for function SignHeader.
func (hs *QuerySorter) Less(i, j int) bool {
	return bytes.Compare([]byte(hs.Keys[i]), []byte(hs.Keys[j])) < 0
}

// Additional function for function SignHeader.
func (hs *QuerySorter) Swap(i, j int) {
	hs.Vals[i], hs.Vals[j] = hs.Vals[j], hs.Vals[i]
	hs.Keys[i], hs.Keys[j] = hs.Keys[j], hs.Keys[i]
}

func percentEncode(value string) string {
	if strings.Contains(value, "+") || strings.Contains(value, " ") {
		value = strings.Replace(value, "+", "%20", -1)
		value = strings.Replace(value, " ", "%20", -1)
		return value
	}
	return url.QueryEscape(value)
}

type Producer interface {
	Send(data []byte) error
}

type DummyProducer struct {
	Url       string
	Path      string
	AccessKey string
	SecretKey string
}

func (d DummyProducer) Send(data []byte) error {
	receiverUrl := d.Url + d.Path + "?AccessKeyId=" + d.AccessKey
	receiverUrl += "&Signature=" + GetSignature("POST", receiverUrl, d.SecretKey)
	Logger.Println("[INFO] Post data to", receiverUrl)

	request, _ := http.NewRequest("POST", receiverUrl, bytes.NewReader(data))
	request.Header.Set("Content-Type", "application/json;charset=UTF-8")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode == http.StatusOK {
		return nil
	} else {
		return errors.New("StatusCode is not 200 :" + strconv.Itoa(response.StatusCode))
	}

}
