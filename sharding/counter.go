package sharding

import (
	"strconv"
	"sync"
)

type Counter struct {
	key string
	m   sync.RWMutex
	cnt int
}

func (c *Counter) Start(key string) error {
	c.key = key
	c.cnt = 0
	return nil
}

func (c *Counter) Stop() error {
	return nil
}

func (c *Counter) Process(msg string) (string, error) {
	switch msg {
	case "inc":
		c.m.Lock()
		c.cnt++
		cnt := c.cnt
		c.m.Unlock()
		return strconv.Itoa(cnt), nil
	case "dec":
		c.m.Lock()
		c.cnt--
		cnt := c.cnt
		c.m.Unlock()
		return strconv.Itoa(cnt), nil
	case "get":
		c.m.RLock()
		cnt := c.cnt
		c.m.RUnlock()
		return strconv.Itoa(cnt), nil
	}
	return "", nil
}
