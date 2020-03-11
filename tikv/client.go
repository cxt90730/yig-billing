package tikv

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/journeymidnight/yig-billing/helper"
	"github.com/tikv/client-go/config"
	"github.com/tikv/client-go/key"
	"github.com/tikv/client-go/txnkv"
	"github.com/tikv/client-go/txnkv/kv"
	"gopkg.in/bufio.v1"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"sync"
	"time"
)

/*
 * Modified from yig/meta/client/tikvclient, maybe refactor to a standalone package
 */

const (
	TableClusterPrefix    = "c"
	TableBucketPrefix     = "b"
	TableUserBucketPrefix = "u"
	TableMultipartPrefix  = "m"
	TableObjectPartPrefix = "p"
	TableLifeCyclePrefix  = "l"
	TableGcPrefix         = "g"

	TableClusterChar    = 'c'
	TableBucketChar     = 'b'
	TableUserBucketChar = 'u'
	TableMultipartChar  = 'm'
	TableObjectPartChar = 'p'
	TableLifeCycleChar  = 'l'
	TableGcChar         = 'g'
	TableSeparatorChar  = '\\'
)

const (
	TableMinKeySuffix = ""
	TableMaxKeySuffix = string(0xFF)
	TableSeparator    = string(92) // "\"
)

func GenKey(args ...string) []byte {
	buf := bufio.NewBuffer([]byte{})
	for _, arg := range args {
		buf.WriteString(arg)
		buf.WriteString(TableSeparator)
	}
	key := buf.Bytes()

	return key[:len(key)-1]
}

type TiKVClient struct {
	pdAddresses []string
	httpClient  http.Client
	TxnCli      *txnkv.Client
}

// KV represents a Key-Value pair.
type KV struct {
	K, V []byte
}

func NewClient(pdAddresses []string) *TiKVClient {
	TxnCli, err := txnkv.NewClient(context.TODO(), pdAddresses, config.Default())
	if err != nil {
		panic(err)
	}
	httpClient := http.Client{
		Timeout: 30 * time.Second,
		Transport: &http.Transport{
			MaxIdleConnsPerHost: 1024,
			DialContext: (&net.Dialer{
				Timeout: 3 * time.Second,
			}).DialContext,
			TLSHandshakeTimeout: 10 * time.Second,
		},
	}
	return &TiKVClient{
		pdAddresses: pdAddresses,
		httpClient:  httpClient,
		TxnCli:      TxnCli,
	}
}

func (c *TiKVClient) TxGet(k []byte, ref interface{}) (bool, error) {
	tx, err := c.TxnCli.Begin(context.TODO())
	if err != nil {
		return false, err
	}
	v, err := tx.Get(context.TODO(), k)
	if err != nil && !kv.IsErrNotFound(err) {
		return false, err
	}
	if kv.IsErrNotFound(err) {
		return false, nil
	}
	return true, helper.MsgPackUnMarshal(v, ref)
}

func (c *TiKVClient) TxExist(k []byte) (bool, error) {
	tx, err := c.TxnCli.Begin(context.TODO())
	if err != nil {
		return false, err
	}
	_, err = tx.Get(context.TODO(), k)
	if err != nil && !kv.IsErrNotFound(err) {
		return false, err
	}
	if kv.IsErrNotFound(err) {
		return false, nil
	}
	return true, nil
}

func (c *TiKVClient) TxPut(args ...interface{}) error {
	tx, err := c.TxnCli.Begin(context.TODO())
	if err != nil {
		return err
	}
	defer func() {
		if err == nil {
			err = tx.Commit(context.Background())
		}
		if err != nil {
			tx.Rollback()
		}
	}()
	for i := 0; i < len(args); i += 2 {
		rowKey := args[i].([]byte)
		val := args[i+1]
		v, err := helper.MsgPackMarshal(val)
		if err != nil {
			return err
		}

		err = tx.Set(rowKey, v)
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *TiKVClient) TxDelete(keys ...[]byte) error {
	tx, err := c.TxnCli.Begin(context.TODO())
	if err != nil {
		return err
	}
	defer func() {
		if err == nil {
			err = tx.Commit(context.Background())
		}
		if err != nil {
			tx.Rollback()
		}
	}()

	for _, key := range keys {
		err := tx.Delete(key)
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *TiKVClient) TxScan(keyPrefix []byte, upperBound []byte, limit int) ([]KV, error) {
	tx, err := c.TxnCli.Begin(context.TODO())
	if err != nil {
		return nil, err
	}
	it, err := tx.Iter(context.TODO(), key.Key(keyPrefix), key.Key(upperBound))
	if err != nil {
		return nil, err
	}
	defer it.Close()
	var ret []KV
	for it.Valid() && limit > 0 {
		ret = append(ret, KV{K: it.Key()[:], V: it.Value()[:]})
		limit--
		it.Next(context.TODO())
	}
	return ret, nil
}

func (c *TiKVClient) TxIter(startKey, endKey []byte, timestamp uint64) (kv.Iterator, error) {
	start := key.Key(startKey)
	if len(startKey) == 0 {
		start = nil
	}
	end := key.Key(endKey)
	if len(endKey) == 0 {
		end = nil
	}
	tx := c.TxnCli.BeginWithTS(context.TODO(), timestamp)
	return tx.Iter(context.TODO(), start, end)
}

type Region struct {
	StartKey      []byte
	EndKey        []byte
	LeaderAddress string
}

type leader struct {
	ID      int `json:"id"`
	StoreID int `json:"store_id"`
}

type region struct {
	ID              int    `json:"id"`
	StartKey        string `json:"start_key"`
	EndKey          string `json:"end_key"`
	Leader          leader `json:"leader"`
	ApproximateKeys int    `json:"approximate_keys"`
}

type regionResponse struct {
	Count   int      `json:"count"`
	Regions []region `json:"regions"`
}

type store struct {
	ID        int    `json:"id"`
	Address   string `json:"address"`
	StateName string `json:"state_name"`
}

type stores struct {
	Store store `json:"store"`
}

type storeResponse struct {
	Count  int      `json:"count"`
	Stores []stores `json:"stores"`
}

// read from ReadCloser and unmarshal to out;
// `out` should be of POINTER type
func readJsonBody(body io.ReadCloser, out interface{}) (err error) {
	defer func() {
		_ = body.Close()
	}()
	jsonBytes, err := ioutil.ReadAll(body)
	if err != nil {
		return err
	}
	//	fmt.Println("json body:", string(jsonBytes))
	err = json.Unmarshal(jsonBytes, out)
	if err != nil {
		return err
	}
	return nil
}

func (c *TiKVClient) fetchRegions() (regions []region, err error) {
	for _, pdAddress := range c.pdAddresses {
		url := fmt.Sprintf("http://%s/pd/api/v1/regions", pdAddress)
		resp, err := c.httpClient.Get(url)
		if err != nil {
			helper.Logger.Error("Fetch regions from", pdAddress, err)
			continue
		}
		var result regionResponse
		err = readJsonBody(resp.Body, &result)
		if err != nil {
			helper.Logger.Error("Fetch region body from", pdAddress, err)
			continue
		}
		return result.Regions, nil
	}
	return nil, errors.New("no pd available")
}

func (c *TiKVClient) fetchStores() (stores map[int]store, err error) {
	for _, pdAddress := range c.pdAddresses {
		url := fmt.Sprintf("http://%s/pd/api/v1/stores", pdAddress)
		resp, err := c.httpClient.Get(url)
		if err != nil {
			helper.Logger.Error("Fetch stores from", pdAddress, err)
			continue
		}
		var result storeResponse
		err = readJsonBody(resp.Body, &result)
		if err != nil {
			helper.Logger.Error("Fetch store body from", pdAddress, err)
			continue
		}
		mappedStores := make(map[int]store)
		for _, s := range result.Stores {
			mappedStores[s.Store.ID] = s.Store
		}
		return mappedStores, nil
	}
	return nil, errors.New("no pd available")
}

// fetch all region info from pd
// TODO maybe cache these info, as tikv client do
func (c *TiKVClient) GetRegions() (regions []Region, err error) {
	wg := &sync.WaitGroup{}
	wg.Add(2)
	var regionResponse []region
	var storeResponse map[int]store
	var e1, e2 error
	go func() {
		regionResponse, e1 = c.fetchRegions()
		wg.Done()
	}()
	go func() {
		storeResponse, e2 = c.fetchStores()
		wg.Done()
	}()
	wg.Wait()
	if e1 != nil || e2 != nil {
		helper.Logger.Error("fetch error:", e1, e2)
		if e1 != nil {
			return nil, e1
		}
		return nil, e2
	}

	regions = make([]Region, 0, len(regionResponse))
	for _, r := range regionResponse {
		// "start_key" and "end_key" are hex encoded in response
		startKey, err := hex.DecodeString(r.StartKey)
		if err != nil {
			helper.Logger.Error("Bad region key:", r.StartKey)
			continue
		}
		endKey, err := hex.DecodeString(r.EndKey)
		if err != nil {
			helper.Logger.Error("Bad region key:", r.EndKey)
			continue
		}
		region := Region{
			StartKey: startKey,
			EndKey:   endKey,
		}
		leaderStoreID := r.Leader.StoreID
		leaderStore, ok := storeResponse[leaderStoreID]
		if !ok {
			return nil, errors.New("unmatched region info")
		}
		region.LeaderAddress = leaderStore.Address
		regions = append(regions, region)
	}
	return regions, nil
}
