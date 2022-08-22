package toy

import (
	"context"
	"fmt"
)

func (s *Store) DefineType(
	ctx context.Context,
	typeName string,
	indexes []string,
) error {
	if typeName == "" {
		return fmt.Errorf("type name must not be empty")
	}

	typeKey := s.baseKeyPrefix + keyPrefixType + typeName
	var values []interface{}
	for _, index := range indexes {
		values = append(values, index)
		values = append(values, 1)
	}
	_, err := s.conn.HSet(ctx, typeKey, values...).Result()
	if err != nil {
		return fmt.Errorf("redis set: %w", err)
	}
	return nil
}
