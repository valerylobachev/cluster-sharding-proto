package counter

import (
	"github.com/valerylobachev/cluster-sharding-proto/sharding"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/schema"
	"log"
)

type CounterFactory struct {
	db *gorm.DB
}

func NewCounterFactory(dsn string) (*CounterFactory, error) {
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{
		PrepareStmt:            true,
		SkipDefaultTransaction: true,
		NamingStrategy: schema.NamingStrategy{
			TablePrefix:   "public.", // schema name
			SingularTable: false,
		},
	})
	if err != nil {
		return nil, err
	}
	return &CounterFactory{db: db}, nil
}

func (cf *CounterFactory) Build(key string) (sharding.EntityActor, error) {
	log.Printf("building actor %s", key)
	actor := &CounterActor{Db: cf.db}
	err := actor.Start(key)
	return actor, err
}
