package sharding

import (
	"crypto/md5"
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

const KEYS_NUM = 100000

type ShardManager struct {
	List     *memberlist.Memberlist
	local    string
	ring     *hashring.HashRing
	m        sync.RWMutex
	entities map[string]string
	keys     []string
}

var hashFunc = func() hashring.HashFunc {
	md5.New()
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
		entities: make(map[string]string),
		keys:     generateKeys(KEYS_NUM),
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

func generateKeys(n int) []string {
	keys := make([]string, n)
	for i := 0; i < n; i++ {
		keys[i] = fmt.Sprintf("Person|P%07d", i)
	}
	return keys
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

func (s *ShardManager) RefreshEntities() {
	log.Println("RefreshEntities")
	if len(s.local) != 0 {
		log.Printf("Local node: %v", s.local)
		for _, key := range s.keys {
			node, ok := s.ring.GetNode(key)
			if ok == true && node == s.local {
				// add entity
				s.m.RLock()
				_, ok := s.entities[key]
				s.m.RUnlock()
				if !ok {
					log.Printf("Add key: %s\n", key)
					s.m.Lock()
					s.entities[key] = key
					s.m.Unlock()
				}

			} else {
				s.m.RLock()
				_, ok := s.entities[key]
				s.m.RUnlock()
				if ok {
					log.Printf("Remove key: %s\n", key)
					s.m.Lock()
					delete(s.entities, key)
					s.m.Unlock()
				}
			}
		}
		s.LogKeys()
		fmt.Println()
	} else {
		log.Println("List is nil")
	}
}

func (s *ShardManager) LogKeys() {
	s.m.RLock()
	log.Printf("Node has %v keys", len(s.entities))
	s.m.RUnlock()
	fmt.Println()
}

func (s *ShardManager) Leave(duration time.Duration) {
	s.List.Leave(duration)
}
