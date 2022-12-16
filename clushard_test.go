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

const KEY_NUM = 100
const MAX_KEY_NUM = 100000

func Test_Clushard(t *testing.T) {
	dist := make(map[string]int)
	for _, key := range keyGen(KEY_NUM) {
		res, _ := callClushard(key, "inc")
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

func Benchmark_ClushardInc(b *testing.B) {
	errors := 0
	rand.Seed(time.Now().UnixNano())
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("Person|P%04d", rand2.Intn(MAX_KEY_NUM))
		res, err := callClushard(key, "inc")
		if err != nil || res.Err != "" {
			errors++
		}
	}
	//log.Printf("Total errors: %d\n", errors)
}

func Benchmark_ClushardGet(b *testing.B) {
	errors := 0
	rand.Seed(time.Now().UnixNano())
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("Person|P%04d", rand2.Intn(MAX_KEY_NUM))
		res, err := callClushard(key, "get")
		if err != nil || res.Err != "" {
			errors++
		}
	}
	//log.Printf("Total errors: %d\n", errors)
}

func keyGen(n int) []string {
	keys := make([]string, n)
	for i := 0; i < n; i++ {
		keys[i] = fmt.Sprintf("Person|P%04d", i)
	}
	return keys
}

func callClushard(key string, msg string) (*server.Response, error) {
	url := getUrl()
	method := "POST"

	payload := strings.NewReader(fmt.Sprintf("{\"key\": \"%s\",\"msg\": \"%s\"}", key, msg))

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

func getUrl() string {
	urls := []string{
		"http://localhost:8100/process",
		"http://localhost:8101/process",
		"http://localhost:8102/process",
		"http://localhost:8103/process",
	}
	return urls[rand.Intn(len(urls))]
}
