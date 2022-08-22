package toy

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"strings"

	"github.com/go-redis/redis/v8"
)

type Store struct {
	conn          *redis.Client
	baseKeyPrefix string
	scriptAdd     *redis.Script
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
		scriptList:    redis.NewScript(scriptList),
	}
}

func (s *Store) getType(
	ctx context.Context,
	typeName string,
) (*itemType, error) {
	typeKey := s.baseKeyPrefix + keyPrefixType + typeName
	v, err := s.conn.Get(ctx, typeKey).Result()
	if err != nil {
		if err == redis.Nil {
			return nil, fmt.Errorf("type not defined: %s", typeName)
		}
		return nil, fmt.Errorf("redis get: %w", err)
	}
	typ, err := s.decodeType(v)
	if err != nil {
		return nil, fmt.Errorf("decode type: %w", err)
	}
	return &typ, nil
}

func (s *Store) encodeItem(item Item) (string, error) {
	var b bytes.Buffer
	enc := gob.NewEncoder(&b)
	err := enc.Encode(item)
	if err != nil {
		return "", fmt.Errorf("encode: %w", err)
	}
	return b.String(), nil
}

func (s *Store) decodeItem(v string) (Item, error) {
	dec := gob.NewDecoder(strings.NewReader(v))
	var item Item
	err := dec.Decode(&item)
	if err != nil {
		return nil, fmt.Errorf("decode: %w", err)
	}
	return item, nil
}

func (s *Store) encodeType(typ itemType) (string, error) {
	var b bytes.Buffer
	enc := gob.NewEncoder(&b)
	err := enc.Encode(typ)
	if err != nil {
		return "", fmt.Errorf("encode: %w", err)
	}
	return b.String(), nil
}

func (s *Store) decodeType(v string) (itemType, error) {
	dec := gob.NewDecoder(strings.NewReader(v))
	var typ itemType
	err := dec.Decode(&typ)
	if err != nil {
		return itemType{}, fmt.Errorf("decode: %w", err)
	}
	return typ, nil
}
