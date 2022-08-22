package toy

import (
	"context"
	"fmt"

	"github.com/go-redis/redis/v8"
)

// TODO: this implementation won't work in clustered deployments because it's
//  accessing keys using programmatically-generated names inside the script.
// KEYS:
//  [1] type key
//  [2] item key
// ARGV:
//  [1] item ID
//  [2] index key prefix
//  [3...] (field1, value1, field2, value2, ...)
// RETURN:
//  0: success
//  1: type not defined
var scriptAdd = `
redis.log(redis.LOG_NOTICE, ARGV[3])
local indexes = redis.call('HKEYS', KEYS[1])
if #indexes == 0 then
	return 1
end
redis.call('HSET', KEYS[2], unpack(ARGV, 3))
-- for i=1,#indexes do
-- 	redis.call('ZADD', ARGV[3]..indexes[i], ARGV[idx+1], ARGV[1])  
-- end
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

	// keys and args
	keys := []string{
		s.baseKeyPrefix + keyPrefixType + typeName,
		s.baseKeyPrefix + keyPrefixItem + typeName + ":" + id,
	}
	args := []interface{}{
		id,
		s.baseKeyPrefix + keyPrefixIndex + typeName + ":",
	}
	for field, value := range item {
		args = append(args, field)

		dv, err := gobEncode(value)
		if err != nil {
			return fmt.Errorf("gob encode (field: %s): %w", field, err)
		}
		args = append(args, dv)
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
