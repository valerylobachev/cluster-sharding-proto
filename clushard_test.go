package cluster_sharding_proto

import (
	"encoding/json"
	"fmt"
	"github.com/valerylobachev/cluster-sharding-proto/server"
	rand2 "golang.org/x/exp/rand"
	"math/rand"
	"net/http"
	"strings"
	"testing"
	"time"
)

const KEY_NUM = 1000

func Test_Clushard(t *testing.T) {
	dist := make(map[string]int)
	for _, key := range keyGen(KEY_NUM) {
		res, _ := callClushard(key)
		dist[res.Server]++
		//if res != nil {
		//	fmt.Printf("%s %s\n", key, res.Server)
		//}
	}

	fmt.Println("Distribution:")
	for k, v := range dist {
		fmt.Printf("%s %4d \n", k, v)
	}
}

func Benchmark_Clushard(b *testing.B) {
	rand.Seed(time.Now().UnixNano())
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("Person|P%04d", rand2.Intn(1000))
		_, _ = callClushard(key)
	}
}

func keyGen(n int) []string {
	keys := make([]string, n)
	for i := 0; i < n; i++ {
		keys[i] = fmt.Sprintf("Person|P%04d", i)
	}
	return keys
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
