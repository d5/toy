package toy

import (
	"github.com/go-redis/redis/v8"
)

type Store struct {
	conn          *redis.Client
	baseKeyPrefix string
	scriptAdd     *redis.Script
	scriptRemove  *redis.Script
	scriptList    *redis.Script
}

func NewStore(
	conn *redis.Client,
	baseKeyPrefix string,
) *Store {
	return &Store{
		conn:          conn,
		baseKeyPrefix: baseKeyPrefix,
		scriptAdd:     redis.NewScript(scriptAdd),
		scriptRemove:  redis.NewScript(scriptRemove),
		scriptList:    redis.NewScript(scriptList),
	}
}
