package sharding

type EntityFactory interface {
	Build(key string) (EntityActor, error)
}
