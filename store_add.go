package toy

import (
	"context"
	"fmt"

	"github.com/go-redis/redis/v8"
)

// KEYS:
//  [1] item key
//  [2...] index keys
// ARGV:
//  [1] item ID
//  [2] item data
//  [3...] indexed field values
var scriptAdd = `
redis.call('SET', KEYS[1], ARGV[2])
for idx=2,#KEYS do
	redis.call('ZADD', KEYS[idx], ARGV[idx+1], ARGV[1])  
end
`

func (s *Store) Add(
	ctx context.Context,
	item *Item,
) error {
	if item == nil {
		return fmt.Errorf("item must not be nil")
	}
	if err := item.Validate(); err != nil {
		return fmt.Errorf("item validate: %w", err)
	}

	// encoded item
	encodedItem, err := item.Serialize()
	if err != nil {
		return fmt.Errorf("encode item: %w", err)
	}

	keys := []string{
		s.baseKeyPrefix + keyPrefixItem + item.Kind() + ":" + item.ID(),
	}
	args := []interface{}{
		item.ID(),
		encodedItem,
	}

	// check indexed fields
	for name, field := range item.fields {
		if field.Indexed {
			indexKey :=
				s.baseKeyPrefix + keyPrefixIndex + item.Kind() + ":" + name
			switch field.ValueType {
			case ItemValueTypeInt64,
				ItemValueTypeFloat64:
				keys = append(keys, indexKey)
				args = append(args, field.Value)
			default:
				return fmt.Errorf(
					"value type not indexable (field: %s): %s",
					name,
					field.ValueType.String(),
				)
			}
		}
	}

	_, err = s.scriptAdd.Run(
		ctx,
		s.conn,
		keys,
		args,
	).Result()
	if err != nil &&
		err != redis.Nil {
		return fmt.Errorf("redis script run: %w", err)
	}
	return nil
}
