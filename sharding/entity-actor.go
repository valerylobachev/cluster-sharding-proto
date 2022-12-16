package sharding

type EntityActor interface {
	Start(key string) error
	Stop() error
	Process(msg string) (string, error)
}
