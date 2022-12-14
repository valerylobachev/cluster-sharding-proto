package main

import (
	"fmt"
	"github.com/hashicorp/memberlist"
	"github.com/serialx/hashring"
	"log"
)

type ClusterEventDelegate struct {
	consistent *hashring.HashRing
}

func (d *ClusterEventDelegate) NotifyJoin(node *memberlist.Node) {
	hostPort := fmt.Sprintf("%s:%d", node.Addr.To4().String(), node.Port)
	log.Printf("join %s", hostPort)
	if d.consistent == nil {
		d.consistent = hashring.New([]string{hostPort})
	} else {
		d.consistent = d.consistent.AddNode(hostPort)
	}
}
func (d *ClusterEventDelegate) NotifyLeave(node *memberlist.Node) {
	hostPort := fmt.Sprintf("%s:%d", node.Addr.To4().String(), node.Port)
	log.Printf("leave %s", hostPort)
	if d.consistent != nil {
		d.consistent = d.consistent.RemoveNode(hostPort)
	}
}
func (d *ClusterEventDelegate) NotifyUpdate(node *memberlist.Node) {
	// skip
}
