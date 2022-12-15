package sharding

type Actor interface {
	Start(key string) error
	Stop() error
	Process(msg string) (string, error)
}
