package toy

import (
	"context"
	"fmt"

	"github.com/go-redis/redis/v8"
)

func (s *Store) Get(
	ctx context.Context,
	typeName string,
	id string,
) (item Item, found bool, err error) {
	if typeName == "" {
		err = fmt.Errorf("type name must not be empty")
		return
	}
	if id == "" {
		err = fmt.Errorf("id must not be empty")
		return
	}

	key := s.baseKeyPrefix + keyPrefixItem + typeName + ":" + id
	v, err := s.conn.Get(ctx, key).Result()
	if err != nil {
		if err == redis.Nil {
			// item not found
			err = nil
			return
		}
		err = fmt.Errorf("redis get: %w", err)
		return
	}
	item, err = s.decodeItem(v)
	if err != nil {
		err = fmt.Errorf("decode item: %w", err)
		return
	}
	found = true
	return
}
