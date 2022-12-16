package cluster_sharding_proto

import (
	"fmt"
	"github.com/serialx/hashring"
	"github.com/spaolacci/murmur3"
	"hash"
	"testing"
)

var hosts = []string{"192.168.0.101:8001", "192.168.0.101:8002", "192.168.0.101:8000", "192.168.0.101:8003"} //, "192.168.0.101:8000", "192.168.0.101:8000"}

var hashFunc = func() hashring.HashFunc {
	hashFunc, err := hashring.NewHash(func() hash.Hash {
		return murmur3.New128().(hash.Hash)
	}).Use(hashring.NewInt64PairHashKey)
	if err != nil {
		panic(fmt.Sprintf("failed to create hashFunc: %s", err.Error()))
	}
	return hashFunc
}()

func Test_HR_md5(t *testing.T) {
	//test := "HR2"
	hosts := []string{"192.168.0.101:8001", "192.168.0.101:8002", "192.168.0.101:8000", "192.168.0.101:8003"} //, "192.168.0.101:8000", "192.168.0.101:8000"}
	ring := hashring.New([]string{})
	for _, host := range hosts {
		ring = ring.AddNode(host)
	}

	dist := make(map[string]int)
	for _, key := range keyGen(KEY_NUM) {
		host, _ := ring.GetNode(key)
		dist[host]++
		//fmt.Printf("%s %s %s\n", key, host, test)
	}

	fmt.Println("Distribution:")
	for k, v := range dist {
		fmt.Printf("%s %4d \n", k, v)
	}
}

func Test_HR_mur3(t *testing.T) {
	//test := "HR2_MUR"
	hosts := []string{"192.168.0.101:8001", "192.168.0.101:8002", "192.168.0.101:8000", "192.168.0.101:8003"} //, "192.168.0.101:8000", "192.168.0.101:8000"}
	ring := hashring.NewWithHash([]string{}, hashFunc)
	for _, host := range hosts {
		ring = ring.AddWeightedNode(host, 1000)
	}

	dist := make(map[string]int)
	for _, key := range keyGen(KEY_NUM) {
		host, _ := ring.GetNode(key)
		dist[host]++
		//fmt.Printf("%s %s %s\n", key, host, test)
	}

	fmt.Println("Distribution:")
	for k, v := range dist {
		fmt.Printf("%s %4d \n", k, v)
	}
}
