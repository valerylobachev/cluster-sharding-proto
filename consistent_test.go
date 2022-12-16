package cluster_sharding_proto

import (
	"fmt"
	"github.com/samber/lo"
	"github.com/zeromicro/go-zero/core/hash"
	"testing"
)

func Test_GZ_mur(t *testing.T) {
	//test := "HR0"
	hosts := []string{"192.168.0.101:8001", "192.168.0.101:8002", "192.168.0.101:8000", "192.168.0.101:8003"} //, "192.168.0.101:8000", "192.168.0.101:8000"}
	//ring := hash.NewConsistentHash()
	ring := hash.NewCustomConsistentHash(100, hash.Hash)
	lo.ForEach(hosts, func(host string, _ int) {
		ring.Add(host)
	})

	dist := make(map[string]int)
	for _, key := range keyGen(KEY_NUM) {
		host, _ := ring.Get(key)
		dist[host.(string)]++
		//fmt.Printf("%s %s %s\n", key, host, test)
	}

	fmt.Println("Distribution:")
	for k, v := range dist {
		fmt.Printf("%s %4d \n", k, v)
	}
}
