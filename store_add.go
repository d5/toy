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
	typeName string,
	id string,
	item Item,
) error {
	if typeName == "" {
		return fmt.Errorf("type name must not be empty")
	}
	if id == "" {
		return fmt.Errorf("id must not be empty")
	}
	if item == nil {
		return fmt.Errorf("item must not be nil")
	}

	// get indexed field names
	typ, err := s.getType(ctx, typeName)
	if err != nil {
		return fmt.Errorf("get type: %w", err)
	}

	// check index fields
	indexValues := make(map[string]float64)
	for _, index := range typ.Indexes {
		v, ok := item[index]
		if !ok {
			return fmt.Errorf("must include indexed field: %s", index)
		}
		switch v := v.(type) {
		case int:
			indexValues[index] = float64(v)
		case int8:
			indexValues[index] = float64(v)
		case int16:
			indexValues[index] = float64(v)
		case int32:
			indexValues[index] = float64(v)
		case int64:
			indexValues[index] = float64(v)
		case uint:
			indexValues[index] = float64(v)
		case uint8:
			indexValues[index] = float64(v)
		case uint16:
			indexValues[index] = float64(v)
		case uint32:
			indexValues[index] = float64(v)
		case uint64:
			indexValues[index] = float64(v)
		default:
			return fmt.Errorf(
				"indexed field (%s) value must be a number: %v (type: %T)",
				index,
				v,
				v,
			)
		}
	}

	// encoded item data
	itemData, err := s.encodeItem(item)
	if err != nil {
		return fmt.Errorf("encode item: %w", err)
	}

	// keys and args
	keys := []string{
		s.baseKeyPrefix + keyPrefixItem + typeName + ":" + id,
	}
	args := []interface{}{
		id,
		itemData,
	}
	for _, index := range typ.Indexes {
		keys = append(
			keys,
			s.baseKeyPrefix+keyPrefixIndex+typeName+":"+index,
		)
		args = append(
			args,
			indexValues[index],
		)
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
