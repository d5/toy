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

	v, err := s.encodeType(
		itemType{
			Indexes: indexes,
		},
	)
	if err != nil {
		return fmt.Errorf("encode type: %w", err)
	}

	typeKey := s.baseKeyPrefix + keyPrefixType + typeName
	_, err = s.conn.Set(ctx, typeKey, v, 0).Result()
	if err != nil {
		return fmt.Errorf("redis set: %w", err)
	}
	return nil
}
