package sharding

import (
	"fmt"
	"github.com/hashicorp/memberlist"
	"github.com/zeromicro/go-zero/core/hash"
	"log"
	"strconv"
	"sync"
	"time"
)

type ShardManager struct {
	List     *memberlist.Memberlist
	local    string
	ring     *hash.ConsistentHash
	m        sync.RWMutex
	entities map[string]Actor
}

func NewShardManager(port int, seed string) (*ShardManager, error) {

	sm := &ShardManager{
		List:     nil,
		ring:     hash.NewConsistentHash(),
		entities: make(map[string]Actor),
	}
	conf := memberlist.DefaultLocalConfig()
	conf.Name = "node" + strconv.Itoa(port%100)
	conf.BindPort = port
	conf.AdvertisePort = conf.BindPort
	conf.Events = NewClusterEventDelegate(sm)

	list, err := memberlist.Create(conf)
	if err != nil {
		return nil, err
	}
	sm.List = list
	local := list.LocalNode()
	sm.local = fmt.Sprintf("%s:%d", local.Addr.To4().String(), local.Port)
	//list.Join([]string{sm.local})

	if len(seed) != 0 {
		log.Printf("cluster join to %s", seed)
		if _, err := list.Join([]string{seed}); err != nil {
			return sm, err
		}
	}
	sm.RefreshEntities()
	return sm, nil
}

func (s *ShardManager) AddNode(node string) {
	log.Printf("Add node: %s\n", node)
	s.ring.Add(node)
	s.RefreshEntities()
}

func (s *ShardManager) RemoveNode(node string) {
	log.Printf("Remove node: %s\n", node)
	s.ring.Remove(node)
	s.RefreshEntities()
}

func (s *ShardManager) GetNode(key string) (node string, isLocal bool) {
	if len(s.local) != 0 {
		node, ok := s.ring.Get(key)
		if ok == true {
			return node.(string), node.(string) == s.local
		}
	}
	return "", false
}

func (s *ShardManager) Ask(key string, msg string) (ans string, err error) {
	s.m.RLock()
	entity, ok := s.entities[key]
	s.m.RUnlock()
	if ok {
		return entity.Process(msg)
	} else {
		entity = &Counter{}
		err := entity.Start(key)
		if err != nil {
			return "", err
		}
		s.m.Lock()
		s.entities[key] = entity
		s.m.Unlock()
		return entity.Process(msg)
	}
}

func (s *ShardManager) RefreshEntities() {
	log.Println("RefreshEntities")
	if len(s.local) != 0 {
		log.Printf("Local node: %v", s.local)
		s.m.Lock()
		for key, _ := range s.entities {
			n, _ := s.ring.Get(key)
			node := n.(string)
			//log.Printf("key: %s, node: %s, ok: %v\n", key, node, ok)
			if node != s.local {
				entity, ok := s.entities[key]
				if ok {
					log.Printf("Remove actor: %s\n", key)
					delete(s.entities, key)
					go entity.Stop()
				}
			}
		}
		s.m.Unlock()
		s.LogKeys()
		fmt.Println()
	} else {
		log.Println("List is nil")
	}
}

func (s *ShardManager) LogKeys() {
	s.m.RLock()
	log.Printf("Node has %v keys\n", len(s.entities))
	//for k, _ := range s.entities {
	//	log.Printf("- %s \n", k)
	//}
	s.m.RUnlock()
	//fmt.Println()
}

func (s *ShardManager) Leave(duration time.Duration) {
	s.List.Leave(duration)
}
