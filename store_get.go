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
	v, err := s.conn.HGetAll(ctx, key).Result()
	if err != nil {
		if err == redis.Nil {
			// item not found
			err = nil
			return
		}
		err = fmt.Errorf("redis get: %w", err)
		return
	}
	item = make(Item)
	for field, value := range v {
		var dv interface{}
		err = gobDecode(value, dv)
		if err != nil {
			err = fmt.Errorf("gob decode (field: %s): %w", field, err)
			return
		}
		item[field] = dv
	}
	found = true
	return
}
