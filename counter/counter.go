package counter

import (
	"database/sql"
	"errors"
	"fmt"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"log"
	"strconv"
	"sync"
	"time"
)

type CounterActor struct {
	Db             *gorm.DB
	Key            string
	sequenceNumber int64
	m              sync.RWMutex
	counter        int
}

func (c *CounterActor) Start(key string) error {
	log.Printf("counter.Start key: %s", key)
	c.Key = key
	var res CounterRecord
	r := c.Db.First(&res, "persistence_id = ?", key)
	err := r.Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		c.sequenceNumber = 0
		c.counter = 0
		log.Printf("counter.Start done key: %s", key)
		return nil
	} else if err != nil {
		log.Printf("counter.Start error: %s", err.Error())
		return err
	}
	c.sequenceNumber = res.SequenceNumber
	c.counter = res.Counter
	log.Printf("counter.Start done key: %s", key)
	return nil
}

func (c *CounterActor) Stop() error {
	return nil
}

func (c *CounterActor) Process(msg string) (string, error) {
	switch msg {
	case "inc":
		return c.Update(true)
	case "dec":
		return c.Update(false)
	case "get":
		c.m.RLock()
		cnt := c.counter
		c.m.RUnlock()
		return strconv.Itoa(cnt), nil
	}
	return "", nil
}

func (c *CounterActor) Update(increment bool) (string, error) {
	c.m.Lock()
	defer c.m.Unlock()
	tx := c.Db.Begin(&sql.TxOptions{Isolation: sql.LevelReadCommitted})
	var rec CounterRecord
	r := tx.Select("sequence_number").First(&rec, "persistence_id = ?", c.Key)
	err := r.Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
	} else if err != nil {
		log.Printf("counter.Update error: %s", err.Error())
		tx.Rollback()
		return "", err
	}
	if c.sequenceNumber != rec.SequenceNumber {
		// TODO: memory state inconsistent, remove actor form memory
		err := errors.New(fmt.Sprintf("failed to persist actor key: %s, memory seqNr: %d, db seqNr: %d",
			c.Key, c.sequenceNumber, rec.SequenceNumber))
		log.Printf("counter.Update error: %s", err.Error())
		tx.Rollback()
		return "", err
	}
	rec.PersistenceId = c.Key
	rec.Timestamp = time.Now().UnixMilli()
	rec.SequenceNumber = c.sequenceNumber + 1
	if increment {
		rec.Counter = c.counter + 1
	} else {
		rec.Counter = c.counter - 1
	}
	r = tx.Clauses(clause.OnConflict{
		UpdateAll: true,
	}).Create(&rec)
	if err = r.Error; err != nil {
		log.Printf("counter.Update error: %s", err.Error())
		tx.Rollback()
		return "", err
	}
	r = tx.Commit()
	if err = r.Error; err != nil {
		log.Printf("counter.Update error: %s", err.Error())
		return "", err
	}
	c.sequenceNumber++
	if increment {
		c.counter++
	} else {
		c.counter--
	}
	return strconv.Itoa(c.counter), nil
}
