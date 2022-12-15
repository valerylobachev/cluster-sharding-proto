package cluster_sharding_proto

import (
	"encoding/json"
	"fmt"
	"github.com/serialx/hashring"
	"github.com/spaolacci/murmur3"
	"github.com/valerylobachev/cluster-sharding-proto/server"
	"hash"
	"net/http"
	"strings"
	"testing"
)

const KEY_NUM = 20

var hashFunc = func() hashring.HashFunc {
	hashFunc, err := hashring.NewHash(func() hash.Hash {
		return murmur3.New128().(hash.Hash)
	}).Use(hashring.NewInt64PairHashKey)
	if err != nil {
		panic(fmt.Sprintf("failed to create hashFunc: %s", err.Error()))
	}
	return hashFunc
}()

func Test_Clushard(t *testing.T) {
	keys := make([]string, KEY_NUM)
	for i := 0; i < KEY_NUM; i++ {
		keys[i] = fmt.Sprintf("P%05d", i)
	}

	for _, key := range keys {
		res, _ := callClushard(key)
		if res != nil {
			fmt.Printf("%s %s\n", key, res.Server)
		}
	}
}

func callClushard(key string) (*server.Response, error) {
	url := "http://localhost:8100/process"
	method := "POST"

	payload := strings.NewReader(fmt.Sprintf("{\"key\": \"%s\",\"msg\": \"inc\"}", key))

	client := &http.Client{}
	req, err := http.NewRequest(method, url, payload)

	if err != nil {
		fmt.Println(err)
		return nil, nil
	}
	req.Header.Add("Content-Type", "application/json")

	res, err := client.Do(req)
	if err != nil {
		fmt.Println(err)
		return nil, nil
	}
	defer res.Body.Close()

	var response server.Response

	json.NewDecoder(res.Body).Decode(&response)

	return &response, nil

}
