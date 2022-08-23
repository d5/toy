package toy

import (
	"context"
	"fmt"

	"github.com/go-redis/redis/v8"
)

// TODO: this implementation won't work in clustered deployments because it's
//  accessing keys using programmatically-generated names inside the script.
// KEYS:
//  [1] item key
// ARGV:
//  [1] item ID
//  [2] index key prefix
var scriptRemove = `
local indexKeys = redis.call('KEYS', ARGV[2]..'*')
for idx=1,#indexKeys do
	redis.call('ZREM', indexKeys[idx], ARGV[1])  
end
return redis.call('DEL', KEYS[1])
`

func (s *Store) Remove(
	ctx context.Context,
	kind string,
	id string,
) (bool, error) {
	if kind == "" {
		return false, fmt.Errorf("kind must not be empty")
	}
	if id == "" {
		return false, fmt.Errorf("id must not be empty")
	}

	keys := []string{
		s.baseKeyPrefix + keyPrefixItem + kind + ":" + id,
	}
	args := []interface{}{
		id,
		s.baseKeyPrefix + keyPrefixIndex + kind + ":",
	}

	res, err := s.scriptRemove.Run(
		ctx,
		s.conn,
		keys,
		args,
	).Result()
	if err != nil &&
		err != redis.Nil {
		return false, fmt.Errorf("redis script run: %w", err)
	}
	return res.(int64) == 1, nil
}
