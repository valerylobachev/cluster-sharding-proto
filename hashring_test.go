package cluster_sharding_proto

import (
	"fmt"
	"github.com/serialx/hashring"
	"testing"
)

var keys = []string{"Person|P0001", "Person|P0002", "Person|P0003", "Person|P0004", "Person|P0005", "Person|P0006", "Person|P0007", "Person|P0008", "Person|P0009"}

func Test_HR1(t *testing.T) {
	test := "HR1"
	hosts := []string{"host-1", "host-2", "host-3", "host-4", "host-5"}
	ring := hashring.New([]string{"host-0"})
	for _, host := range hosts {
		ring = ring.AddNode(host)
	}

	for _, key := range keys {
		host, _ := ring.GetNode(key)
		fmt.Printf("%s %s %s\n", key, host, test)
	}
}

func Test_HR2(t *testing.T) {
	test := "HR2"
	hosts := []string{"host-5", "host-4", "host-2", "host-1", "host-3", "host-0"}
	ring := hashring.New([]string{})
	for _, host := range hosts {
		ring = ring.AddNode(host)
	}

	for _, key := range keys {
		host, _ := ring.GetNode(key)
		fmt.Printf("%s %s %s\n", key, host, test)
	}
}

func Test_HR1_mur(t *testing.T) {
	test := "HR1_MUR"
	hosts := []string{"host-1", "host-2", "host-3", "host-4", "host-5"}
	ring := hashring.NewWithHash([]string{"host-0"}, hashFunc)
	for _, host := range hosts {
		ring = ring.AddNode(host)
	}

	for _, key := range keys {
		host, _ := ring.GetNode(key)
		fmt.Printf("%s %s %s\n", key, host, test)
	}
}

func Test_HR2_mur(t *testing.T) {
	test := "HR2_MUR"
	hosts := []string{"host-5", "host-4", "host-2", "host-1", "host-3", "host-0"}
	ring := hashring.NewWithHash([]string{}, hashFunc)
	for _, host := range hosts {
		ring = ring.AddNode(host)
	}

	for _, key := range keys {
		host, _ := ring.GetNode(key)
		fmt.Printf("%s %s %s\n", key, host, test)
	}
}
