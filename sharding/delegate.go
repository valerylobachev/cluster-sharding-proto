package sharding

import (
	"fmt"
	"github.com/hashicorp/memberlist"
	"log"
)

type ClusterEventDelegate struct {
	shardManager *ShardManager
}

func NewClusterEventDelegate(shardManager *ShardManager) *ClusterEventDelegate {
	return &ClusterEventDelegate{shardManager: shardManager}
}

func (d *ClusterEventDelegate) NotifyJoin(node *memberlist.Node) {
	host := fmt.Sprintf("%s:%d", node.Addr.To4().String(), node.Port)
	d.shardManager.AddNode(host)
}

func (d *ClusterEventDelegate) NotifyLeave(node *memberlist.Node) {
	host := fmt.Sprintf("%s:%d", node.Addr.To4().String(), node.Port)
	d.shardManager.RemoveNode(host)
}
func (d *ClusterEventDelegate) NotifyUpdate(node *memberlist.Node) {
	host := fmt.Sprintf("%s:%d", node.Addr.To4().String(), node.Port)
	log.Printf("Update node: %s\n", host)
	// skip
}
