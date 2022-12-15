package sharding

import (
	"fmt"
	"github.com/hashicorp/memberlist"
	"github.com/serialx/hashring"
	"github.com/spaolacci/murmur3"
	"hash"
	"log"
	"strconv"
	"sync"
	"time"
)

type ShardManager struct {
	List     *memberlist.Memberlist
	local    string
	ring     *hashring.HashRing
	m        sync.RWMutex
	entities map[string]Actor
}

var hashFunc = func() hashring.HashFunc {
	hashFunc, err := hashring.NewHash(func() hash.Hash {
		return murmur3.New128().(hash.Hash)
	}).Use(hashring.NewInt64PairHashKey)
	if err != nil {
		panic(fmt.Sprintf("failed to create hashFunc: %s", err.Error()))
	}
	return hashFunc
}()

func NewShardManager(port int, seed string) (*ShardManager, error) {

	sm := &ShardManager{
		List:     nil,
		ring:     hashring.NewWithHash([]string{}, hashFunc),
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
	s.ring = s.ring.AddNode(node)
	s.RefreshEntities()
}

func (s *ShardManager) RemoveNode(node string) {
	log.Printf("Remove node: %s\n", node)
	s.ring = s.ring.RemoveNode(node)
	s.RefreshEntities()
}

func (s *ShardManager) GetNode(key string) (node string, isLocal bool) {
	if len(s.local) != 0 {
		node, ok := s.ring.GetNode(key)
		if ok == true {
			return node, node == s.local
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
			node, ok := s.ring.GetNode(key)
			log.Printf("key: %s, node: %s, ok: %v\n", key, node, ok)
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
	for k, _ := range s.entities {
		log.Printf("- %s \n", k)
	}
	s.m.RUnlock()
	fmt.Println()
}

func (s *ShardManager) Leave(duration time.Duration) {
	s.List.Leave(duration)
}
